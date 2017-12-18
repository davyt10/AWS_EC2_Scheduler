#!/usr/bin/python
import boto3
import time
from botocore.exceptions import ClientError
from distutils.util import strtobool
from SSMDelegate import SSMDelegate
import botocore
import Utils
import logging

__author__ = "Gary Silverman"


logger = logging.getLogger('Orchestrator') #The Module Name

class Worker(object):
    SNS_SUBJECT_PREFIX_WARNING="Warning:"
    SNS_SUBJECT_PREFIX_INFORMATIONAL="Info:"

    def __init__(self, workloadRegion, snsNotConfigured, instance, loglevel, partitionTargetValue,workloadSpecificationDictPass,dryRunFlag):

        self.workloadRegion=workloadRegion
        self.instance=instance
        self.dryRunFlag=dryRunFlag
	self.loglevel = loglevel
	self.partitionTargetValue = partitionTargetValue
	self.workloadSpecificationDict = workloadSpecificationDictPass

	self.sns_publisher = Utils.Snspublisher(self.workloadRegion)

#        self.snsUtils.makeSNSTopic()

	self.sns_publisher.makeTopic(self.workloadSpecificationDict)

#        self.snsUtils.snsTopicSubject()

	self.sns_publisher.makeTopicSubjectLine(self.workloadSpecificationDict)
#	self.snsTopicSubjectLine = self.Utils.makeSNSTopicSubjectLine(self.workloadSpecificationDict)

        self.snsNotConfigured = snsNotConfigured

        self.instanceStateMap = {
            "pending" : 0,
            "running" : 16,
            "shutting-down" : 32,
            "terminated" : 48,
            "stopping" : 64,
            "stopped" : 80
        }

        try:
            self.ec2Resource = boto3.resource('ec2', region_name=self.workloadRegion)
        except Exception as e:
            msg = 'Worker::__init__() Exception obtaining botot3 ec2 resource in region %s -->' % workloadRegion
            logger.error(msg + str(e))


class StartWorker(Worker):

    def __init__(self, ddbRegion, workloadRegion, snsNotConfigured, instance, all_elbs, elb, scalingInstanceDelay, dryRunFlag, exponentialBackoff, max_api_request, loglevel,partitionTargetValue,workloadSpecificationDictPass):
        super(StartWorker, self).__init__(workloadRegion, snsNotConfigured, instance, loglevel, partitionTargetValue,workloadSpecificationDictPass,dryRunFlag)

        self.ddbRegion=ddbRegion
        self.all_elbs=all_elbs
        self.elb=elb
        self.scalingInstanceDelay = scalingInstanceDelay
        self.exponentialBackoff=exponentialBackoff
        self.max_api_request=max_api_request

    def addressELBRegistration(self):
        for i in self.all_elbs['LoadBalancerDescriptions']:
            for j in i['Instances']:
                if j['InstanceId'] == self.instance.id:
                    elb_name = i['LoadBalancerName']
                    logger.info("Instance %s is attached to ELB %s, and will be deregistered and re-registered" % (self.instance.id, elb_name))

                    success_deregister_done=0
                    elb_api_retry_count=1
                    while (success_deregister_done == 0):
                        try:
                            self.elb.deregister_instances_from_load_balancer(LoadBalancerName=elb_name,Instances=[{'InstanceId': self.instance.id}])
                            logger.debug("Succesfully deregistered instance %s from load balancer %s" % (self.instance.id, elb_name))
                            success_deregister_done=1
                            # self.warningMsg = "ELB Deregistered for instance"
                            # self.publishSNSTopicMessage(self.warningMsg, self.warningMsg, self.instance)
                        except botocore.exceptions.ClientError as e:
                            logger.warning("Could not deregistered instance %s from load balancer %s" % (self.instance.id, elb_name))
                            logger.warning('Worker::addressELBRegistration()::deregister_instances_from_load_balancer() encountered an exception of -->' + str(e))
                            if (elb_api_retry_count > self.max_api_request):
                                msg = 'Maximum API Call Retries for addressELBRegistration: deregister() reached, exiting program'
                                logger.error(msg + str(elb_api_retry_count))
                                subjectPrefix = "MaxRetries Exceeded ELB"
                                try:
	                                Orchestrator.publishSNSTopicMessage(subjectPrefix, msg, self.instance)
					logger.info('Sending SNS notification for Max Retries RateLimitExceeded - ELB')
                                except Exception as e:
					logger.warning('Worker::instance.start() encountered an exception of -->' + str(e))
                                exit()
                            else:
				elb_api_retry_count+=1
                                logger.warning('Exponential Backoff in progress, retry count = %s' % str(elb_api_retry_count))
				msg = "Exponential Backoff in progress for EC2 instance %s, sleeping %s seconds" % (self.instance,elb_api_retry_count)
                                self.exponentialBackoff(elb_api_retry_count,msg)

                    success_register_done=0
                    elb_api_retry_count=1
                    while (success_register_done == 0):
                        try:
                            self.elb.register_instances_with_load_balancer(LoadBalancerName=elb_name, Instances=[{'InstanceId': self.instance.id}])
                            logger.debug('Succesfully registered instance %s to load balancer %s' % (self.instance.id, elb_name))
                            success_register_done=1
                            # self.warningMsg = "ELB Registered for instance"
                            # self.publishSNSTopicMessage(self.warningMsg, self.warningMsg, self.instance)
                        except botocore.exceptions.ClientError as e:
                            logger.warning('Could not register instance [%s] to load balancer [%s] because of [%s]' % (self.instance.id, elb_name, str(e)))
                            logger.warning('Worker::addressELBRegistration()::register_instances_with_load_balancer() encountered an exception of -->' + str(e))
                            if (elb_api_retry_count > self.max_api_request):
                                msg = 'Maximum API Call Retries for addressELBRegistration:register() reached, EC2 instance: %s. Exiting program' % self.instance
                                logger.error(msg + str(elb_api_retry_count))
                                subjectPrefix = "MaxRetries Exceeded ELB"
                                try:
                                        Orchestrator.publishSNSTopicMessage(subjectPrefix, msg)
                                        logger.info('Sending SNS notification for Max Retries RateLimitExceeded - ELB')
                                except Exception as e:
                                        logger.warning('Worker::instance.start() encountered an exception of -->' + str(e))
                                exit()
                            else:
				elb_api_retry_count+=1
                                logger.warning('Exponential Backoff in progress, retry count = %s' % str(elb_api_retry_count))
				msg = 'Exponential Backoff in progress, retry count = %s' % str(elb_api_retry_count) % self.instance
                                self.exponentialBackoff(elb_api_retry_count,msg)
                                


    def startInstance(self):

        result='Instance not started'
        if( self.dryRunFlag ):
            logger.warning('DryRun Flag is set - instance will not be started')
        else:
            try:
                if self.all_elbs != "0":
                    logger.debug('addressELBRegistration() for %s' % self.instance.id)
                    self.addressELBRegistration()
                result=self.instance.start()
                logger.info('startInstance() for ' + self.instance.id + ' result is %s' % result)
            except Exception as e:
                logger.warning('Worker::instance.start() encountered an exception of -->' + str(e))

    def scaleInstance(self, modifiedInstanceType):

        instanceState  = self.instance.state
        if (instanceState['Name'] == 'stopped'):


            result='no result'
            if( self.dryRunFlag ):
                logger.warning('DryRun Flag is set - instance will not be scaled')

            else:
                logger.info('Instance [%s] will be scaled to Instance Type [%s]' % (self.instance.id , modifiedInstanceType) )

                targetInstanceFamily = modifiedInstanceType.split('.')[0]  # Grab the first token after split()

                # EC2.Instance.modify_attribute()
                # Check and exclude non-optimized instance families. Should be enhanced to be a map.  Currently added as hotfix.
                preventEbsOptimizedList = [ 't2' ]
                if (targetInstanceFamily in preventEbsOptimizedList ):
                    ebsOptimizedAttr = False
                else:
                    ebsOptimizedAttr = self.instance.ebs_optimized    # May have been set to True or False previously

                instance_type_done=0
                scale_api_retry_count=1
                while(instance_type_done == 0):
                    try:
                        result = self.instance.modify_attribute(
                            InstanceType={
                                'Value': modifiedInstanceType
                            }
                        )
                        instance_type_done=1
                    except Exception as e:
                        logger.warning('Worker::instance.modify_attribute() encountered an exception where requested instance type ['+ modifiedInstanceType +'] resulted in -->' + str(e))
                        if (scale_api_retry_count > self.max_api_request):
                            msg = 'Maximum Retries RateLimitExceeded reached for modifiedInstanceType , stopping process at number of retries--> %s ' % self.max_api_request
                            logger.error(msg)
                            subjectPrefix = "MaxRetries Exceeded Instance Modify"
                            try:
                                        Orchestrator.publishSNSTopicMessage(subjectPrefix, msg, self.instance)
                                        logger.info('Sending SNS notification for Max Retries RateLimitExceeded - Instance Modify')
                            except Exception as e:
                                        logger.warning('Worker::instance.start() encountered an exception of -->' + str(e))

                            exit()
                        else:
			    scale_api_retry_count += 1
                            logger.warning('Exponential Backoff in progress, retry count = %s' % str(scale_api_retry_count))
				
                            self.exponentialBackoff(scale_api_retry_count,self.publishSNSTopicMessage,self.instance)
		
                ebs_optimized_done=0
                ebs_optimized_retry_count=1
                while(ebs_optimized_done == 0):
                    try:
#                        result = self.instance.modify_attribute(
#                            EbsOptimized={
#                            'Value': ebsOptimizedAttr
#                            }
#                        )
#                        ebs_optimized_done=1
#			ebs_optimized_done=1
			dfdf
                    except Exception as e:
                        logger.warning('Worker::instance.modify_attribute() encountered an exception where requested EBS optimized flag set to ['+ str(ebsOptimizedAttr) +'] resulted in -->' + str(e))
                        if (ebs_optimized_retry_count > self.max_api_request):
                            msg = 'Maximum Retries RateLimitExceeded reached for ebsOptimizedAttr, stopping process at number of retries--> %s ' % self.max_api_request
                            logger.error(msg)
                            subjectPrefix = "MaxRetries Exceeded EBS Modify"
                            try:
					publishMessage = Orchestrator()
					publishMessage().publishSNSTopicMessage(subjectPrefix, msg)
                                        logger.info('Sending SNS notification for Max Retries RateLimitExceeded - EBS Modify')
                            except Exception as e:
                                        logger.warning('Worker::instance.start() encountered an exception of -->' + str(e))
                            exit()
                        else:
			    msg = 'Exponential Backoff in progress, retry count = %s, EC2 instance %s' % (ebs_optimized_retry_count,self.instance)
                            logger.warning('Exponential Backoff in progress, retry count = %s' % str(ebs_optimized_retry_count))
                            self.exponentialBackoff(ebs_optimized_retry_count,msg)
			    ebs_optimized_retry_count += 1

                    # It appears the start instance reads 'modify_attribute' changes as eventually consistent in AWS (assume DynamoDB),
                    #    this can cause an issue on instance type change, whereby the LaunchPlan generates an exception.
                    #    To mitigate against this, we will introduce a one second sleep delay after modifying an attribute
                    time.sleep(self.scalingInstanceDelay)

                logger.info('scaleInstance() for ' + self.instance.id + ' result is %s' % result)
        else:
            logMsg = 'scaleInstance() requested to change instance type for non-stopped instance ' + self.instance.id + ' no action taken'
            logger.warning(logMsg)

    def start(self):
        self.startInstance()

class StopWorker(Worker):
    def __init__(self, ddbRegion, workloadRegion, snsNotConfigured, instance, partitionTargetValue, workloadSpecificationDictPass,loglevel,dryRunFlag):
        super(StopWorker, self).__init__(workloadRegion, snsNotConfigured, instance, loglevel, partitionTargetValue,workloadSpecificationDictPass,dryRunFlag)

        self.ddbRegion=ddbRegion

        # MUST convert string False to boolean False
        self.waitFlag=strtobool('False')
        self.overrideFlag=strtobool('False')

    def stopInstance(self):

        logger.debug('Worker::stopInstance() called')

        result='Instance not Stopped'

        if( self.dryRunFlag ):
            logger.warning('DryRun Flag is set - instance will not be stopped')
        else:
            try:
                # EC2.Instance.stop()
                result = self.instance.stop()
            except Exception as e:
                logger.warning('Worker:: instance.stop() encountered an exception of -->' + str(e))

            logger.info('stopInstance() for ' + self.instance.id + ' result is %s' % result)

        # If configured, wait for the stop to complete prior to returning
        logger.debug('The bool value of self.waitFlag %s, is %s' % (self.waitFlag, bool(self.waitFlag)))


        # self.waitFlag has been converted from str to boolean via set method
        if( self.waitFlag ):
            logger.info(self.instance.id + ' :Waiting for Stop to complete...')

            if( self.dryRunFlag ):
                logger.warning('DryRun Flag is set - waiter() will not be employed')
            else:
                try:
                    # Need the Client to get the Waiter
                    ec2Client=self.ec2Resource.meta.client
                    waiter=ec2Client.get_waiter('instance_stopped')

                    # Waits for 40 15 second increments (e.g. up to 10 minutes)
                    waiter.wait( )

                except Exception as e:
                    logger.warning('Worker:: waiter block encountered an exception of -->' + str(e))

        else:
            logger.info(self.instance.id + ' Wait for Stop to complete was not requested')

    def setWaitFlag(self, flag):

        # MUST convert string False to boolean False
        self.waitFlag = strtobool(flag)

    def getWaitFlag(self):
        return( self.waitFlag )

    def isOverrideFlagSet(self, S3BucketName, S3KeyPrefixName, overrideFileName, osType):
        ''' Use SSM to check for existence of the override file in the guest OS.  If exists, don't Stop instance but log.
        Returning 'True' means the instance will not be stopped.
            Either because the override file exists, or the instance couldn't be reached
        Returning 'False' means the instance will be stopped (e.g. not overridden)
        '''


        # If there is no overrideFilename specified, we need to return False.  This is required because the
        # remote evaluation script may evaluate to "Bypass" with a null string for the override file.  Keep in
        # mind, DynamodDB isn't going to enforce an override filename be set in the directive.
        if not overrideFileName:
            logger.info(self.instance.id + ' Override Flag not set in specification.  Therefore this instance will be actioned. ')
            return False

        if not osType:
            logger.info(self.instance.id + ' Override Flag set BUT no Operating System attribute in specification. Therefore this instance will be actioned.')
            return False


        # Create the delegate
        ssmDelegate = SSMDelegate(self.instance.id, S3BucketName, S3KeyPrefixName, overrideFileName, osType, self.ddbRegion, logger, self.workloadRegion)


        # Very first thing to check is whether SSM is going to write the output to the S3 bucket.
        #   If the bucket is not in the same region as where the instances are running, then SSM doesn't write
        #   the result to the bucket and thus the rest of this method is completely meaningless to run.

        if( ssmDelegate.isS3BucketInWorkloadRegion() == SSMDelegate.S3_BUCKET_IN_CORRECT_REGION ):

            warningMsg=''
            msg=''
            # Send request via SSM, and check if send was successful
            ssmSendResult=ssmDelegate.sendSSMCommand()
            if( ssmSendResult ):
                # Have delegate advise if override file was set on instance.  If so, the instance is not to be stopped.
                overrideRes=ssmDelegate.retrieveSSMResults(ssmSendResult)
                logger.debug('SSMDelegate retrieveSSMResults() results :' + overrideRes)

                if( overrideRes == SSMDelegate.S3_BUCKET_IN_WRONG_REGION ):
                    # Per SSM, the bucket must be in the same region as the target instance, otherwise the results will not be writte to S3 and cannot be obtained.
                    self.overrideFlag=True
                    warningMsg= Worker.SNS_SUBJECT_PREFIX_WARNING + ' ' + self.instance.id + ' Instance will be not be stopped because the S3 bucket is not in the same region as the workload'
                    logger.warning(warningMsg)

                elif( overrideRes == SSMDelegate.DECISION_STOP_INSTANCE ):
                    # There is a result and it specifies it is ok to Stop
                    self.overrideFlag=False
                    logger.info(self.instance.id + ' Instance will be stopped')

                elif( overrideRes == SSMDelegate.DECISION_NO_ACTION_UNEXPECTED_RESULT ):
                    # Unexpected SSM Result, see log file.  Will default to overrideFlag==true out of abundance for caution
                    self.overrideFlag=True
                    warningMsg = Worker.SNS_SUBJECT_PREFIX_WARNING +  ' ' + self.instance.id + ' Instance will be not be stopped as there was an unexpected SSM result.'
                    logger.warning(warningMsg)

                elif( overrideRes == SSMDelegate.DECISION_RETRIES_EXCEEDED ):
                    self.overrideFlag=True
                    warningMsg = Worker.SNS_SUBJECT_PREFIX_WARNING +  ' ' + self.instance.id + ' Instance will be not be stopped # retries to collect SSM result from S3 was exceeded'
                    logger.warning(warningMsg)

                else:
                    # Every other result means the instance will be bypassed (e.g. not stopped)
                    self.overrideFlag=True
                    msg=Worker.SNS_SUBJECT_PREFIX_INFORMATIONAL +  ' ' + self.instance.id + ' Instance will be not be stopped because override file was set'
                    logger.info(msg)

            else:
                self.overrideFlag=True
                warningMsg=Worker.SNS_SUBJECT_PREFIX_WARNING +  ' ' + self.instance.id + ' Instance will be not be stopped because SSM could not query it'
                logger.warning(warningMsg)

        else:
            self.overrideFlag=True
            warningMsg=Worker.SNS_SUBJECT_PREFIX_WARNING + ' SSM will not be executed as S3 bucket is not in the same region as the workload. [' + self.instance.id + '] Instance will be not be stopped'
            logger.warning(warningMsg)

        if( self.snsNotConfigured == False ):
            if( self.overrideFlag == True ):
                if( warningMsg ):
                    self.publishSNSTopicMessage(Worker.SNS_SUBJECT_PREFIX_WARNING, warningMsg, self.instance)
                else:
                    self.publishSNSTopicMessage(Worker.SNS_SUBJECT_PREFIX_INFORMATIONAL, msg, self.instance)

        return( self.overrideFlag )

    def setOverrideFlagSet(self, overrideFlag):
        self.overrideFlag=strtobool(overrideFlag)


    def execute(self, S3BucketName, S3KeyPrefixName, overrideFileName, osType):
        if( self.isOverrideFlagSet(S3BucketName, S3KeyPrefixName, overrideFileName, osType) == False ):
            self.stopInstance()
