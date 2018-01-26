#!/usr/bin/python
import boto3
import json
import time
import datetime
import argparse
import Utils
import logging 
import re

from distutils.util import strtobool
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
from Worker import Worker, StopWorker, StartWorker
from Utils import RetryNotifier

__author__ = "Gary Silverman"

logger = logging.getLogger('Orchestrator') #The Module Name

class Orchestrator(object):

	# Class Variables

	# Mapping of Python Class Variables to DynamoDB Attribute Names in Workload Table
	WORKLOAD_SPEC_TABLE_NAME='WorkloadSpecification'
	WORKLOAD_SPEC_PARTITION_KEY='SpecName'

	WORKLOAD_SPEC_REGION_KEY='WorkloadRegion'

	WORKLOAD_ENVIRONMENT_FILTER_TAG_KEY='WorkloadFilterTagName'
	WORKLOAD_ENVIRONMENT_FILTER_TAG_VALUE='WorkloadFilterTagValue'

	WORKLOAD_VPC_ID_KEY='VPC_ID'

	WORKLOAD_SSM_S3_BUCKET_NAME='SSMS3BucketName'
	WORKLOAD_SSM_S3_KEY_PREFIX_NAME='SSMS3KeyPrefixName'

	WORKLOAD_SNS_TOPIC_NAME='SNSTopicName'

	WORKLOAD_KILL_SWITCH="DisableAllSchedulingActions"
	WORKLOAD_KILL_SWITCH_TRUE="1"

	WORKLOAD_SCALE_INSTANCE_DELAY="ScaleInstanceDelay"  #Delay in seconds after modifyAttribute() called ahead of startIntance()


	TIER_SPEC_TABLE_NAME='TierSpecification'
	TIER_SPEC_PARTITION_KEY='SpecName'

	# Mapping of Python Class Variables to DynamoDB Attribute Names in Tier Table
	TIER_FILTER_TAG_KEY='TierFilterTagName'
	TIER_FILTER_TAG_VALUE='TierTagValue'

	TIER_STOP='TierStop'
	TIER_START='TierStart'
	TIER_SCALING='TierScaling'
	TIER_NAME='TierTagValue'
	TIER_SEQ_NBR='TierSequence'
	TIER_SYCHRONIZATION='TierSynchronization'
	TIER_STOP_OVERRIDE_FILENAME='TierStopOverrideFilename'
	TIER_STOP_OS_TYPE='TierStopOverrideOperatingSystem' # Valid values are Linux and Windows

	INTER_TIER_ORCHESTRATION_DELAY='InterTierOrchestrationDelay' # The sleep time between commencing an action on this tier
	INTER_TIER_ORCHESTRATION_DELAY_DEFAULT = 5

	ACTION_STOP='Stop'
	ACTION_START='Start'
	FLEET_SUBSET='1'
	# ACTION_SCALE_UP='ScaleUp'
	# ACTION_SCALE_DOWN='ScaleDown'

	LOG_LEVEL_INFO='info'
	LOG_LEVEL_DEBUG='debug'

	def __init__(self, partitionTargetValue, dynamoDBRegion, scalingProfile, dryRun=False):


		# default to us-west-2
		self.dynamoDBRegion=dynamoDBRegion 
		self.workloadRegion='us-west-2'  #default

		# The commmand line param will be passed as a string
		self.dryRunFlag=dryRun
	
		self.partitionTargetValue=partitionTargetValue  # must be set prior to invoking initlogging()
	
		###
		# DynamoDB Table Related
		#
		try:
			self.dynDBC = boto3.client('dynamodb', region_name=self.dynamoDBRegion)
		except Exception as e:
			msg = 'Orchestrator::__init__() Exception obtaining botot3 dynamodb client in region %s -->' % self.workloadRegion
			logger.error(msg + str(e))

		

		# Directive DynamoDB Table Related
		self.workloadSpecificationTableName=Orchestrator.WORKLOAD_SPEC_TABLE_NAME
		self.workloadSpecificationPartitionKey=Orchestrator.WORKLOAD_SPEC_PARTITION_KEY


		# Create a List of valid dynamoDB attributes to address user typos in dynamoDB table
		self.workloadSpecificationValidAttributeList = [
			Orchestrator.WORKLOAD_SPEC_PARTITION_KEY,
			Orchestrator.WORKLOAD_SPEC_REGION_KEY,
			Orchestrator.WORKLOAD_ENVIRONMENT_FILTER_TAG_KEY,
			Orchestrator.WORKLOAD_ENVIRONMENT_FILTER_TAG_VALUE,
			Orchestrator.WORKLOAD_VPC_ID_KEY,
			Orchestrator.WORKLOAD_SSM_S3_BUCKET_NAME,
			Orchestrator.WORKLOAD_SSM_S3_KEY_PREFIX_NAME,
			Orchestrator.WORKLOAD_SNS_TOPIC_NAME,
			Orchestrator.WORKLOAD_KILL_SWITCH,
			Orchestrator.WORKLOAD_SCALE_INSTANCE_DELAY,
			Orchestrator.TIER_FILTER_TAG_KEY,
			Orchestrator.FLEET_SUBSET
		]

		# Tier-specific DynamoDB Table Related
		self.tierSpecTableName=Orchestrator.TIER_SPEC_TABLE_NAME
		self.tierSpecPartitionKey=Orchestrator.TIER_SPEC_PARTITION_KEY # Same as workloadSpecificationPartitionKey

		# Table requires the DynamoDB.Resource
		try:
			self.dynDBR = boto3.resource('dynamodb', region_name=self.dynamoDBRegion)
			self.tierSpecTable = self.dynDBR.Table(self.tierSpecTableName)
		except Exception as e:
			msg = 'Orchestrator::__init__() Exception obtaining botot3 dynamodb resource in region %s -->' % self.workloadRegion
			logger.error(msg + str(e))

		# Create a List of valid dynamoDB attributes to address user typos in dynamoDB table
		self.tierSpecificationValidAttributeList = [
			Orchestrator.TIER_FILTER_TAG_VALUE,
			Orchestrator.TIER_SPEC_TABLE_NAME,
			Orchestrator.TIER_SPEC_PARTITION_KEY,
			Orchestrator.TIER_STOP,
			Orchestrator.TIER_START,
			Orchestrator.TIER_SCALING,
			Orchestrator.TIER_NAME,
			Orchestrator.TIER_SEQ_NBR,
			Orchestrator.TIER_SYCHRONIZATION,
			Orchestrator.TIER_STOP_OVERRIDE_FILENAME,
			Orchestrator.TIER_STOP_OS_TYPE,
			Orchestrator.INTER_TIER_ORCHESTRATION_DELAY
		]

		self.scalingProfile = scalingProfile

		#
		###

		self.startTime=0
		self.finishTime=0

		self.workloadSpecificationDict={}

		self.tierSpecDict={}

		# Dynamically created based on tierSpecDict, based on TierSequence for Action specified
		self.sequencedTiersList=[]

		self.validActionNames = [ Orchestrator.ACTION_START, 
								  Orchestrator.ACTION_STOP
								]

		# These are the official codes per Boto3
		self.instanceStateMap = {
			0: "pending",
			16: "running",
			32: "shutting-down",
			48: "terminated",
			64: "stopping",
			80: "stopped"
		}

	def initializeState(self):

		# Maximum number of retries for API calls
		self.max_api_request=8

		# Log the duration of the processing
		self.startTime = datetime.datetime.now().replace(microsecond=0)

		# Grab general workload information from DynamoDB
		self.lookupWorkloadSpecification(self.partitionTargetValue)

		# The region where the workload is running.  Note: this may be a different region than the 
		# DynamodDB configuration
		try:
			self.workloadRegion = self.workloadSpecificationDict[Orchestrator.WORKLOAD_SPEC_REGION_KEY]
		except Exception as e:
			logger.warning('Orchestrator.py::initializeState() Error obtaining self.workloadRegion --> %s' % str(e))
		# The delay (in seconds) when scaling an instance type ahead of Starting the newly scaled instance.
		# This is needed due to an eventual consistency issue in AWS whereby Instance.modifyAttribute() is changed
		# and Instance.startInstance() experiences an Exception because the modifyAttribute() has not fully propogated.
		self.scaleInstanceDelay = float(4.0)  # default to four seconds (float)
		if( Orchestrator.WORKLOAD_SCALE_INSTANCE_DELAY in self.workloadSpecificationDict ):
			try:
				delayValueStr = self.workloadSpecificationDict[Orchestrator.WORKLOAD_SCALE_INSTANCE_DELAY]
				self.scaleInstanceDelay = float(delayValueStr)
			except Exception as e:
				logger.warning('Couldn\'t convert %s to float. Using default of %s.  Exception was %s' % (delayValueStr, str(self.scaleInstanceDelay), str(e)) )

		# We provision the boto3 resource here because we need to have determined the Workload Region as a dependency,
		# which is done just above in this method
		try:
			self.ec2R = boto3.resource('ec2', region_name=self.workloadRegion)
		except Exception as e:
			msg = 'Orchestrator::initializeState() Exception obtaining botot3 ec2 resource in region %s -->' % self.workloadRegion
			logger.error(msg + str(e))

		try:
			self.elb = boto3.client('elb', region_name=self.workloadRegion)
		except Exception as e:
			msg = 'Orchestrator::__init__() Exception obtaining botot3 elb client in region %s -->' % self.workloadRegion
			logger.error(msg + str(e))

		# Grab tier specific workload information from DynamoDB
                self.lookupTierSpecs(self.partitionTargetValue)

	def lookupELBs(self):

		success_describe_elbs_done = 0
                describe_elb_api_retry_count = 1
		while (success_describe_elbs_done == 0):
			try:
				self.all_elbs = self.elb.describe_load_balancers()
				msg = 'Orchestrator::__init() Found attached ELBs-->'
				success_describe_elbs_done = 1
			except Exception as e:
				self.all_elbs = "0"
				msg = 'Orchestrator:: Exception obtaining all ELBs in region %s --> %s' % (self.workloadRegion,e)
				subject_prefix = "Scheduler Exception in %s" % self.workloadRegion
				logger.error(msg + str(e))
				self.snsInit.exponentialBackoff(describe_elb_api_retry_count,msg,subject_prefix)
				describe_elb_api_retry_count += 1

	def lookupWorkloadSpecification(self, partitionTargetValue):
		try:
			dynamodbItem=self.dynDBC.get_item(
				TableName=self.workloadSpecificationTableName,
				Key={
					self.workloadSpecificationPartitionKey : { "S" : partitionTargetValue }
				},
				ConsistentRead=False,
				ReturnConsumedCapacity="TOTAL",
			)
		except ClientError as e:
			logger.error('lookupWorkloadSpecification()' + e.response['Error']['Message'])
		else:
			# Get the dynamoDB Item from the result
			resultItem=dynamodbItem['Item']

		
			for attributeName in resultItem:
				# Validate the attributes entered into DynamoDB are valid.  If not, spit out individual warning messages
				if( attributeName in self.workloadSpecificationValidAttributeList ):
					attributeValue=resultItem[attributeName].values()[0]
					logger.info('Workload Attribute [%s maps to %s]' % (attributeName, attributeValue))
					self.workloadSpecificationDict[attributeName]=attributeValue
				else:
					logger.warning('Invalid dynamoDB attribute specified->'+str(attributeName)+'<- will be ignored')


	def recursiveFindKeys(self, sourceDict, resList):
		for k,v in sourceDict.iteritems():
			resList.append(k)
			if( isinstance(v, dict) ):
				# Since scalingProfile key names are user dependent, we can't validate them
				if( k != Orchestrator.TIER_SCALING ):
					self.recursiveFindKeys(v, resList)

	def lookupTierSpecs(self, partitionTargetValue):
		'''
		Find all rows in table with partitionTargetValue
		Build a Dictionary (of Dictionaries).  Dictionary Keys are: TIER_START, TIER_STOP, TierScaleUp, TierScaleDown
			Values are attributeValues of the DDB Item Keys
		'''
		try:
			dynamodbItem=self.tierSpecTable.query(
				KeyConditionExpression=Key(self.tierSpecPartitionKey).eq(partitionTargetValue),
				ConsistentRead=False,
				ReturnConsumedCapacity="TOTAL",
			)
		except ClientError as e:
			logger.error('Exception encountered in lookupTierSpecs() -->' + str(e))
		else:
			# Get the items from the result
			resultItems=dynamodbItem['Items']

			# Create a Dictionary that stores the currTier and currTier associated with Tiers
			for currTier in resultItems:
				logger.info('DynamoDB Query for Tier->'+ currTier[Orchestrator.TIER_NAME])

				tierKeys=[]
				self.recursiveFindKeys(currTier, tierKeys)
				setDiff = set(tierKeys).difference(self.tierSpecificationValidAttributeList)
				if( setDiff ):
					for badAttrKey in setDiff:
						logger.warning('Invalid dynamoDB attribute specified->'+str(badAttrKey)+'<- will be ignored')

				self.tierSpecDict[currTier[Orchestrator.TIER_NAME]] = {}
				# Pull out the Dictionaries for each of the below. 
				# Result is a key, and a dictionary
				if( Orchestrator.TIER_STOP in currTier ):
					self.tierSpecDict[ currTier[Orchestrator.TIER_NAME] ].update( { Orchestrator.TIER_STOP : currTier[ Orchestrator.TIER_STOP ] } )

				if (Orchestrator.TIER_START in currTier):
					self.tierSpecDict[ currTier[Orchestrator.TIER_NAME] ].update( { Orchestrator.TIER_START : currTier[ Orchestrator.TIER_START ] } )

				if (Orchestrator.TIER_SCALING in currTier):
					self.tierSpecDict[ currTier[Orchestrator.TIER_NAME] ].update( { Orchestrator.TIER_SCALING : currTier[ Orchestrator.TIER_SCALING ] } )

				if (Orchestrator.FLEET_SUBSET in currTier):
					self.tierSpecDict[ currtier[Orchestrator.TIER_NAME] ].update( { Orchestrator.FLEET_SUBSET : currTier[ Orchestrator.FLEET_SUBSET ] } )

				#self.logSpecDict('lookupTierSpecs', currTier, Orchestrator.LOG_LEVEL_DEBUG )

			# Log the constructed Tier Spec Dictionary
			#self.logSpecDict('lookupTierSpecs', self.tierSpecDict, Orchestrator.LOG_LEVEL_INFO )

	def sequenceTiers(self, tierAction):
		# Using the Tier Spec Dictionary, construct a simple List to order the sequence of Tier Processing
		# for the given Action.  Sequence is ascending.
		#
		# Prefill list for easy insertion
		self.sequencedTiersList=range( len(self.tierSpecDict) )

		# tierAction indicates whether it is a TIER_STOP, or TIER_START, as they may have different sequences
		for currKey, currAttributes in self.tierSpecDict.iteritems():
			logger.debug('sequenceTiers() Action=%s, currKey=%s, currAttributes=%s)' % (tierAction, currKey, currAttributes) )
			
			# Grab the Tier Name first
			tierName = currKey
			#tierName = currAttributes[Orchestrator.TIER_NAME]

			tierAttributes={}	# do I need to scope this variable as such?
			logger.debug('In sequenceTiers(), tierAction is %s' % tierAction)
			if( tierAction == Orchestrator.TIER_STOP):
				# Locate the TIER_STOP Dictionary
				tierAttributes = currAttributes[Orchestrator.TIER_STOP]

			elif( tierAction == Orchestrator.TIER_START ):
				tierAttributes = currAttributes[Orchestrator.TIER_START]

			#logger.info('In sequenceTiers(): tierAttributes is ', tierAttributes )


			# Insert into the List 
			self.sequencedTiersList[ int( tierAttributes[Orchestrator.TIER_SEQ_NBR ] ) ] = tierName

		logger.debug('Sequence List for Action %s is %s' % (tierAction, self.sequencedTiersList))
			
		return( self.sequencedTiersList )
	

	def logSpecDict(self, label, dict, level):
		# for key, value in self.workloadSpecificationDict.iteritems():
		# 	logger.debug('%s (key==%s, value==%s)' % (label, key, value))
		for key, value in dict.iteritems():
			if( level == Orchestrator.LOG_LEVEL_INFO ):
				logger.info('%s (key==%s, value==%s)' % (label, key, value))
			else:
				logger.debug('%s (key==%s, value==%s)' % (label, key, value))

	def isTierSynchronized(self, tierName, tierAction):
		# Get the Tier Named tierName
		tierAttributes = self.tierSpecDict[tierName]

		# Get the dictionary for the correct Action
		tierActionAttributes={}
		if( tierAction == Orchestrator.TIER_STOP):
			# Locate the TIER_STOP Dictionary
			tierActionAttributes = tierAttributes[Orchestrator.TIER_STOP]

		elif( tierAction == Orchestrator.TIER_START ):
			# Locate the TIER_START Dictionary
			tierActionAttributes = tierAttributes[Orchestrator.TIER_START]
			#print tierActionAttributes

		# Return the value in the Dict for TierSynchronization
		if Orchestrator.TIER_SYCHRONIZATION in tierActionAttributes:
			res = tierActionAttributes[Orchestrator.TIER_SYCHRONIZATION]
		else:
			res = False

		logger.debug('isTierSynchronized for tierName==%s, tierAction==%s is syncFlag==%s' % (tierName, tierAction, res) )
		return( res )

	def getTierStopOverrideFilename(self, tierName):

		# Get the Tier Named tierName
		tierAttributes = self.tierSpecDict[tierName]

		# Get the dictionary for the correct Action
		tierActionAttribtes={}

		# Locate the TIER_STOP Dictionary method only applies to TIER_STOP
		tierActionAttributes = tierAttributes[Orchestrator.TIER_STOP]
		
		# Return the value in the Dict for TierStopOverrideFilename
		if Orchestrator.TIER_STOP_OVERRIDE_FILENAME in tierActionAttributes:
			res = tierActionAttributes[Orchestrator.TIER_STOP_OVERRIDE_FILENAME]
		else:
			res = ''

		return( res )

	def getTierOperatingSystemType(self, tierName):

		# Get the Tier Named tierName
		tierAttributes = self.tierSpecDict[tierName]

		# Get the dictionary for the correct Action
		tierActionAttribtes={}

		# Locate the TIER_STOP Dictionary
		tierActionAttributes = tierAttributes[Orchestrator.TIER_STOP]
		
		# Return the value in the Dict for TierStopOverrideOperatingSystem
		if Orchestrator.TIER_STOP_OS_TYPE in tierActionAttributes:
			res = tierActionAttributes[Orchestrator.TIER_STOP_OS_TYPE]
		else:
			res = ''

		return( res )

	def getInterTierOrchestrationDelay(self, tierName, tierAction):

		# Get the Tier Named tierName
		tierAttributes = self.tierSpecDict[tierName]

		# Get the dictionary for the correct Action
		tierActionAttribtes={}

		# Locate the TIER_STOP Dictionary
		tierActionAttributes = tierAttributes[tierAction]
		
		# Return the value in the Dict for TierStopOverrideOperatingSystem
		if Orchestrator.INTER_TIER_ORCHESTRATION_DELAY in tierActionAttributes:
			res = tierActionAttributes[Orchestrator.INTER_TIER_ORCHESTRATION_DELAY]
		else:
			res = Orchestrator.INTER_TIER_ORCHESTRATION_DELAY_DEFAULT

		return( float(res) )
	
	def lookupInstancesByFilter(self, targetInstanceStateKey, tierName):
	    # Use the filter() method of the instances collection to retrieve
	    # all running EC2 instances.
		logger.debug('lookupInstancesByFilter() seeking instances in tier %s' % tierName)
		logger.debug('lookupInstancesByFilter() instance state %s' % targetInstanceStateKey)
		logger.debug('lookupInstancesByFilter() tier tag key %s' % self.workloadSpecificationDict[Orchestrator.TIER_FILTER_TAG_KEY])
		logger.debug('lookupInstancesByFilter() tier tag value %s' % tierName)
		logger.debug('lookupInstancesByFilter() Env tag key %s' % self.workloadSpecificationDict[Orchestrator.WORKLOAD_ENVIRONMENT_FILTER_TAG_KEY])
		logger.debug('lookupInstancesByFilter() Env tag value %s' % self.workloadSpecificationDict[Orchestrator.WORKLOAD_ENVIRONMENT_FILTER_TAG_VALUE])


		targetFilter = [
			{
		        'Name': 'instance-state-name', 
		        'Values': [targetInstanceStateKey]
		    },
		    {
		        'Name': 'tag:' + self.workloadSpecificationDict[Orchestrator.WORKLOAD_ENVIRONMENT_FILTER_TAG_KEY],
		        'Values': [self.workloadSpecificationDict[Orchestrator.WORKLOAD_ENVIRONMENT_FILTER_TAG_VALUE]]
		    },
		    {
		        'Name': 'tag:' + self.workloadSpecificationDict[Orchestrator.TIER_FILTER_TAG_KEY],
		        'Values': [tierName]
		    }
		]

		# If the Optional VPC ID was provided to further tighten the filter, include it.
		if( Orchestrator.WORKLOAD_VPC_ID_KEY in self.workloadSpecificationDict ):
			vpc_filter_dict_element = { 
				'Name': 'vpc-id', 
		        'Values': [self.workloadSpecificationDict[Orchestrator.WORKLOAD_VPC_ID_KEY]]
			}
			targetFilter.append(vpc_filter_dict_element)
			logger.debug('VPC_ID provided, Filter List is %s' % str(targetFilter))

		# Filter the instances
		# NOTE: Only instances within the specified region are returned
		targetInstanceColl = {}
		instances_filter_done=0
		filter_instances_api_retry_count=1
		while (instances_filter_done==0):
				try:	
					targetInstanceColl = sorted(self.ec2R.instances.filter(Filters=targetFilter))
					logger.info('lookupInstancesByFilter(): # of instances found for tier %s in state %s is %i' % (tierName, targetInstanceStateKey, len(list(targetInstanceColl))))
					if(logger.level==Orchestrator.LOG_LEVEL_DEBUG):
						for curr in targetInstanceColl:
							logger.debug('lookupInstancesByFilter(): Found the following matching targets %s' % curr)
					instances_filter_done=1
				except Exception as e:
					msg = 'Orchestrator::lookupInstancesByFilter() Exception encountered during instance filtering %s -->' % e
					logger.error(msg + str(e))
					subject_prefix = "Scheduler Exception in %s" % self.workloadRegion
					self.snsInit.exponentialBackoff(filter_instances_api_retry_count,msg,subject_prefix)
					filter_instances_api_retry_count += 1

		return targetInstanceColl

			
	def isKillSwitch(self):

		res = False
		if( Orchestrator.WORKLOAD_KILL_SWITCH in self.workloadSpecificationDict ):

			switchValue = self.workloadSpecificationDict[Orchestrator.WORKLOAD_KILL_SWITCH]

			if( switchValue == Orchestrator.WORKLOAD_KILL_SWITCH_TRUE ):
				logger.warning('Kill Switch found.  All scheduling actions on the workload will be bypassed')
				res = True

		return( res )

	
	def orchestrate(self, action ):
		'''
		Given an Action, 
		1) Iterate through the Tiers based on the sequence and
		2) Apply the directive to each tier, applying the inter-tier delay factor 
		3) Log
		'''


		killSwitch = self.isKillSwitch()

		if( killSwitch ):
			bodyMsg = '%s: Kill Switch Enabled.   All scheduling actions on the workload will be bypassed.' % self.workloadSpecificationDict[Orchestrator.WORKLOAD_ENVIRONMENT_FILTER_TAG_VALUE]
			self.publishSNSTopic(self.snsTopicSubjectLine, bodyMsg)

		else:

			if( action == Orchestrator.ACTION_STOP ):

				# Sequence the tiers per the STOP order
				self.sequenceTiers(Orchestrator.TIER_STOP)

				for currTier in self.sequencedTiersList:
				
					logger.info('Orchestrate() Stopping Tier: ' + currTier)
				
					# Stop the next tier in the sequence
					self.stopATier(currTier)
					

			elif( action == Orchestrator.ACTION_START ): 
				orchMain.lookupELBs()
	
				# Sequence the tiers per the START order
				self.sequenceTiers(Orchestrator.TIER_START)
				
				for currTier in self.sequencedTiersList:
				
					logger.info('Orchestrate() Starting Tier: ' + currTier)
				
					# Start the next tier in the sequence
					self.startATier(currTier)

			elif( action not in self.validActionNames ):
				
				logger.warning('Action requested %s not a valid choice of ', self.validActionNames)
			
			else:
			
				logger.warning('Action requested %s is not yet implemented. No action taken', action)	

		# capture completion time
		self.finishTime = datetime.datetime.now().replace(microsecond=0)

		logger.info('++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
		logger.info('++ Completed processing ['+ action +'][' + self.partitionTargetValue + ']<- in ' + str(self.finishTime - self.startTime) + ' seconds')
		logger.info('++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
	

	def stopATier(self, tierName):
		'''
		Given a Tier,
		0) We may want to create a separate "client" per instance within the tier, if we process in parallel
		1) Determine if the override flag is set, and if so, log and bypass
		2) Determine if the tier is synchronized and if so, ensure the use Waiters is applied
		   during processing, prior to returning
		3) Stop the tier 
		4) Log 
		'''
		
		# Find the running instances of this tier to stop
		running=self.instanceStateMap[16]

		instancesToStopList = self.lookupInstancesByFilter(
			running,
			tierName
		)

		# Determine if operations on the Tier should be synchronized or not
		tierSynchronized=self.isTierSynchronized(tierName, Orchestrator.TIER_STOP)

		# Grab the EC2 region (not DynamodDB region) for the worker to make API calls
		#region=self.workloadSpecificationDict[self.WORKLOAD_SPEC_REGION_KEY]


		for currInstance in instancesToStopList:
			stopWorker = StopWorker(self.dynamoDBRegion, self.workloadRegion, currInstance, self.dryRunFlag,self.max_api_request,self.snsInit)
			stopWorker.setWaitFlag(tierSynchronized)
			stopWorker.execute(
				self.workloadSpecificationDict[Orchestrator.WORKLOAD_SSM_S3_BUCKET_NAME],
				self.workloadSpecificationDict[Orchestrator.WORKLOAD_SSM_S3_KEY_PREFIX_NAME],
				self.getTierStopOverrideFilename(tierName),
				self.getTierOperatingSystemType(tierName)
			)

		# Configured delay to be introduced prior to allowing the next tier to be Actioned.
		# It may make sense to allow some amount of time for the instances to Stop, prior to Orchestration continuing.
		time.sleep(self.getInterTierOrchestrationDelay(tierName, Orchestrator.TIER_STOP))

	def startATier(self, tierName):
		'''
		Given a Tier,
		0) We may want to create a separate "client" per instance within the tier, if we process in parallel
		1) Determine if the override flag is set, and if so, log and bypass
		2) Determine if the tier is synchronized and if so, ensure the use Waiters is applied
		   during processing, prior to returning
		3) Start the tier 
		4) Log 
		'''

		stopped=self.instanceStateMap[80]

		# Find the stopped instances of this tier to start
		instancesToStartList = self.lookupInstancesByFilter(
			stopped,
			tierName
		)

		# Find the running instances of this Tier required to analyze if we already have
		# enough of the running instances

		running=self.instanceStateMap[16]

		runningInstancesList = self.lookupInstancesByFilter(
			running,
			tierName
		)

		# Determine if operations on the Tier should be synchronized or not,
		# currently this feature is not implemented for Starting A Tier
		#syncFlag=self.isTierSynchronized(tierName, Orchestrator.TIER_START)

		# Grab the region the worker should make API calls against
		#region=self.workloadSpecificationDict[self.WORKLOAD_SPEC_REGION_KEY]

		logger.debug('In startATier() for %s', tierName)

		# number_of_instances is the output of isFleetSubset method

		number_of_instances = self.isFleetSubset(tierName)

		if (number_of_instances is None ):
			logger.debug('Orchestrator(): Fleet Subset profile not specified')
			logger.info('Orchestrator(): Fleet Subset profile not specified')

		else:
			if re.search("%",number_of_instances):
				split_number = number_of_instances.split("%")[0]
				if (int(split_number) <= 0) or (int(split_number) > 100):
					number_of_instances = "default"
				else:
					number_of_instances = int(round(int(split_number) * len(list(instancesToStartList)) / 100.0))

		# We are checking if there is "default" ScalingTier (which is the same as None actually), or if number of desired instances is higher than current number of instances
		
		if (number_of_instances is None ) or (number_of_instances == "default") or (int(number_of_instances) > len(list(instancesToStartList))):
			for currInstance in instancesToStartList:
				logger.debug('Number of desired instances in FleetSubset higher than number of existing EC2 instances or no FleetSubset Profile present; Will start all EC2 instances within Tier. Starting instance %s', currInstance)
                                logger.info('Number of desired EC2 instances in FleetSubset higher than number of existing EC2 instances or no FleetSubset Profile present; Will start all EC2 instances within Tier. Starting instance %s', currInstance)
				startWorker = StartWorker(self.dynamoDBRegion, self.workloadRegion, currInstance, self.all_elbs, self.elb, self.scaleInstanceDelay, self.dryRunFlag, self.max_api_request,self.snsInit)

				# If a ScalingProfile was specified, change the instance type now, prior to Start
				instanceTypeToLaunch = self.isScalingAction(tierName)
				if( instanceTypeToLaunch ):
					startWorker.scaleInstance(instanceTypeToLaunch)

				# Finally, have the worker Start the instance
				startWorker.start()

			# Delay to be introduced prior to allowing the next tier to be actioned.
			# It may make sense to allow some amount of time for the instances to Stop, prior to Orchestration continuing.
			time.sleep(self.getInterTierOrchestrationDelay(tierName, Orchestrator.TIER_START))

			logger.debug('startATier() completed for tier %s' % tierName)

		elif (int(number_of_instances) == 0):
			logger.debug('Number of desired instances in Tier %s has been chosen to 0, not starting any EC2 instances' % tierName)
			logger.info('Number of desired instances in Tier %s has been chosen to 0, not starting any EC2 instances' % tierName)

		elif (len(runningInstancesList) >= int(number_of_instances)):
			logger.debug('Number of desired instances already reached: %s' % len(runningInstancesList))
			logger.info('Number of desired instances already reached: %s' % len(runningInstancesList))
		else:
			logger.info('Orchestrator(): FleetSubset defined, starting %s subset of EC2 instances' % number_of_instances)
			Counter = 1
			for currInstance in instancesToStartList:
				logger.debug('Starting instance %s', currInstance)
				startWorker = StartWorker(self.dynamoDBRegion, self.workloadRegion, currInstance, self.all_elbs, self.elb, self.scaleInstanceDelay, self.dryRunFlag, self.max_api_request,self.snsInit)
				# If a ScalingProfile was specified, change the instance type now, prior to Start
				instanceTypeToLaunch = self.isScalingAction(tierName)
				if( instanceTypeToLaunch ):
					startWorker.scaleInstance(instanceTypeToLaunch)

				# Finally, have the worker Start the instance
				startWorker.start()				
				if (Counter == int(number_of_instances)):
					logger.debug('Number of instances reached desired number')
					logger.info('Number of instances reached desired number')
					break
				else:
					logger.debug('Counter: %s, Desired number: %s, Current total number: %s' % (Counter,number_of_instances,len(list(instancesToStartList))))
					logger.info('Counter: %s, Desired number: %s, Current total number: %s' % (Counter,number_of_instances,len(list(instancesToStartList))))
				Counter += 1

	def isScalingAction(self, tierName):

		# First, is the ScalingProfile flag even set ?
		if(self.scalingProfile):
			logger.debug('ScalingProfile requested')

			# Unpack the ScalingDictionary
			tierAttributes = self.tierSpecDict[tierName]
			if( Orchestrator.TIER_SCALING in tierAttributes):

				scalingDict = tierAttributes[ Orchestrator.TIER_SCALING ]
				logger.debug('ScalingProfile for Tier %s is %s ' % (tierName, str(scalingDict) ))

				# Ok, so next, does this tier have a Scaling Profile?
				if( self.scalingProfile in scalingDict ):
					# Ok, so then what is the EC2 InstanceType to launch with, for the given ScalingProfile specified ?
					instanceType = scalingDict[self.scalingProfile]['InstanceType']
					logger.info('Orchestrator().isScalingAction(): Chosen ScalingProfile: %s, Instance Type: %s' % (str(instanceType), str(self.scalingProfile)))
					return instanceType
				else:
					logger.warning('Scaling Profile of [%s] not in tier [%s] ' % (str(self.scalingProfile), tierName ) )

			else:
				logger.warning('Scaling Profile of [%s] specified but no TierScaling dictionary found in DynamoDB for tier [%s].  No scaling action taken' % (str(self.scalingProfile), tierName) )

		return( None )
	
	def isFleetSubset(self, tierName):
		if(self.scalingProfile):
			logger.debug('scalingProfile requested - FleetSubset calculator')
			
			# Unpack the ScalingDictionary
			tierAttributes = self.tierSpecDict[tierName]
			if( Orchestrator.TIER_SCALING in tierAttributes):

				scalingDict = tierAttributes[ Orchestrator.TIER_SCALING ]
				logger.debug('FleetSubset for Tier %s %s ' % (tierName, str(scalingDict) ))

				if ( self.scalingProfile in scalingDict ):
					if 'FleetSubset' in scalingDict[self.scalingProfile]:
						fleet_number = scalingDict[self.scalingProfile]['FleetSubset']
					else:
						fleet_number = "default"

					return fleet_number
				else:
					logger.warning('FleetSubset of [%s] not in tier [%s] ' % (str(self.scalingProfile), tierName ) )
			else:
				logger.warning('FleetSubset Profile of [%s] specified but no FleetSubset dictionary found in DynamoDB for tier [%s]. No action taken' % (str(self.scalingProfile), tierName) )

	def runTestCases(self):
		logger.info('Executing runTestCases()')
		
		self.initializeState()

		# print 'Role_Web override file loc ', self.getTierStopOverrideFilename('Role_Web')
		# print 'Role_AppServer override file loc ', self.getTierStopOverrideFilename('Role_AppServer')
		# print 'Role_DB override file loc ', self.getTierStopOverrideFilename('Role_DB')
		# Test Case: Stop an Environment
		logger.info('\n### Orchestrating START Action ###')
		self.orchestrate(Orchestrator.ACTION_START )

		# Test Case: Start an Environment
		sleepSecs=20
		logger.info('\n### Sleeping for ' + str(sleepSecs) + ' seconds ###')
		time.sleep(sleepSecs)

		logger.info('\n### Orchestrating STOP Action ###')
		self.orchestrate(Orchestrator.ACTION_STOP )

	def sns_init(self):
		try:
			sns_topic_name = self.workloadSpecificationDict[Orchestrator.WORKLOAD_SNS_TOPIC_NAME]
			sns_workload = self.workloadSpecificationDict[Orchestrator.WORKLOAD_SPEC_PARTITION_KEY]
			self.snsInit = RetryNotifier(self.workloadRegion,sns_workload,self.max_api_request)
			self.snsInit.makeTopic(sns_topic_name)
#		else:
		except Exception as e: 
			logger.info('Orchestrator::sns_init() sns_topic_name must be defined in DynamoDB --> ' + str(e))
			exit()

if __name__ == "__main__":
	# python Orchestrator.py -i workloadIdentier -r us-west-2

	parser = argparse.ArgumentParser(description='Command line parser')
	parser.add_argument('-w','--workloadIdentifier', help='Workload Identifier to Action Upon',required=True)
	parser.add_argument('-r','--dynamoDBRegion', help='Region where the DynamoDB configuration exists. Note: could be different from the target EC2 workload is running', required=True)
	parser.add_argument('-a','--action', choices=['Stop', 'Start'], help='Action to Orchestrate (e.g. Stop or Start)', required=False)
	parser.add_argument('-t','--testcases', action='count', help='Run the test cases', required=False)
	parser.add_argument('-d','--dryrun', action='count', help='Run but take no Action', required=False)
	parser.add_argument('-p','--scalingProfile', help='Resize instances based on Scaling Profile name', required=False)
	parser.add_argument('-l','--loglevel', choices=['critical', 'error', 'warning', 'info', 'debug', 'notset'], help='The level to record log messages to the logfile', required=False)
	
	args = parser.parse_args()

	if( args.loglevel > 0 ):
		loglevel = args.loglevel
	else:
		loglevel = 'info'

	if( args.dryrun > 0 ):
		dryRun = True
	else:
		dryRun = False

	# Launch the Orchestrator - the main component of the subsystem

	Utils.initLogging(args.loglevel,args.workloadIdentifier)

	orchMain = Orchestrator(args.workloadIdentifier, args.dynamoDBRegion, args.scalingProfile, dryRun)


	# If testcases set, run them, otherwise run the supplied Action only
	if( args.testcases > 0 ):	
		orchMain.runTestCases()
	else:

		# Default Action to Start
		if( args.action ):
			action = args.action
		else:
			action = Orchestrator.ACTION_START

		logger.info('\n### Orchestrating %s' % action +' Action ###')
		orchMain.initializeState()
		orchMain.sns_init()
		orchMain.orchestrate(action)

	

