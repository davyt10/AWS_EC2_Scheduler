#!/usr/bin/env python
import boto3
import logging
import logging.handlers

logger = logging.getLogger('Orchestrator') #The Module Name

class Snspublisher():
	def __init__(self,dynamoDBRegion):
		self.dynamoDBRegion=dynamoDBRegion
		self.snsTopicR = boto3.resource('sns', region_name=self.dynamoDBRegion)
		snsTopic = ''
		snsNotConfigured=False

	def makeTopic(self,workloadSpecificationDict):
			self.workloadSpecificationDict=workloadSpecificationDict
        	        if (self.workloadSpecificationDict):

                	        # Make or retrieve the SNS Topic setup.  Method is Idempotent
                        	try:
                                	snsTopic = self.snsTopicR.create_topic( Name=self.workloadSpecificationDict)
	                                snsTopicSubjectLine = self.makeTopicSubjectLine(self.workloadSpecificationDict)

        	                except Exception as e:
                	                logger.error('orchestrate() - creating SNS Topic ' + str(e) )
                        	        snsNotConfigured=True
	                else:
        	                snsNotConfigured=True

	def publishTopicMessage(subjectPrefix, theMessage):
        	        tagsMsg=''
                	try:
	                    	self.snsTopic.publish(
        	                	Subject=self.snsTopicSubjectLine + ':' + subjectPrefix,
	        	                Message=theMessage + "\n" + tagsMsg,
        	        	)

                	except Exception as e:
                        	logger.error('publishSNSTopicMessage() ' + str(e) )

	def publishTopic(self, subject, message):
                try:
                        self.snsTopic.publish(
                                Subject=subject,
                                Message=message,
                        )

                except Exception as e:
                        logger.error('publishSNSTopicMessage() ' + str(e) )

	def makeTopicSubjectLine(self,workloadSpecificationDict):
                res = 'AWS_EC2_Scheduler Notification:  Workload==>' + workloadSpecificationDict
                return( res )


def initLogging(loglevel,partitionTargetValue):
               # Setup the Logger
    	logger = logging.getLogger('Orchestrator')  #The Module Name
            # Set logging level
        loggingLevelSelected = logging.INFO

       # Set logging level
        if( loglevel == 'critical' ):
                loggingLevelSelected=logging.CRITICAL
        elif( loglevel == 'error' ):
                loggingLevelSelected=logging.ERROR
        elif( loglevel == 'warning' ):
                loggingLevelSelected=logging.WARNING
        elif( loglevel == 'info' ):
                loggingLevelSelected=logging.INFO
        elif( loglevel == 'debug' ):
                loggingLevelSelected=logging.DEBUG
        elif( loglevel == 'notset' ):
                loggingLevelSelected=logging.NOTSET

        filenameVal='Orchestrator_' + partitionTargetValue + '.log' 
        log_formatter = logging.Formatter('[%(asctime)s][%(levelname)s][%(module)s:%(funcName)s()][%(lineno)d]%(message)s')

        # Add the rotating file handler
        handler = logging.handlers.RotatingFileHandler(
                filename=filenameVal,
                mode='a',
                maxBytes=128 * 1024,
                backupCount=10)
        handler.setFormatter(log_formatter)

        logger.addHandler(handler)
        logger.setLevel(loggingLevelSelected)
