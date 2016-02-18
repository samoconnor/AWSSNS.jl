#==============================================================================#
# AWSSNS.jl
#
# SNS API. See http://aws.amazon.com/documentation/sns/
#
# Copyright Sam O'Connor 2014 - All rights reserved
#==============================================================================#


module AWSSNS

__precompile__()

export sns_list_topics, sns_delete_topic, sns_create_topic, sns_subscribe_sqs,
       sns_subscribe_email, sns_subscribe_lambda, sns_publish


using AWSCore
using SymDict



sns_arn(aws, topic_name) = arn(aws, "sns", topic_name)


function sns(aws, query)
    query["ContentType"] = "JSON"
    do_request(post_request(aws, "sns", "2010-03-31", query))
end


function sns(aws, action, topic; args...)

    sns(aws, merge(StringDict(args),
                   "Action" => action,
                   "Name" => topic,
                   "TopicArn" => sns_arn(aws, topic)))
end


function sns_list_topics(aws) 
    r = sns(aws, Dict("Action" => "ListTopics"))
    [split(t["TopicArn"],":")[6] for t in r["Topics"]]
end

function sns_create_topic(aws, topic_name) 

    sns(aws, "CreateTopic", topic_name)
end


function sns_delete_topic(aws, topic_name)

    sns(aws, "DeleteTopic", topic_name)
end


function sns_publish(aws, topic_name, message, subject="No Subject")

    if length(subject) > 100
        subject = subject[1:100]
    end
    sns(aws, "Publish", topic_name, Message = message, Subject = subject)
end

import AWSSQS: sqs_arn, sqs

function sns_subscribe_sqs(aws, topic_name, queue; raw=flase)

    r = sns(aws, Dict("Action" => "Subscribe",
                      "Name" => topic_name,
                      "TopicArn" => sns_arn(aws, topic_name),
                      "Endpoint" => sqs_arn(queue),
                      "Protocol" => "sqs"))

    if raw
        arn = r["SubscriptionArn"]
        sns(aws, "SetSubscriptionAttributes", topic_name,
                  SubscriptionArn = arn,
                  AttributeName = "RawMessageDelivery",
                  AttributeValue = "true")
    end

    sqs(queue, Dict(
        "Action" => "SetQueueAttributes",
        "Attribute.Name" => "Policy",
        "Attribute.Value" => """{
          "Version": "2008-10-17",
          "Statement": [
            {
              "Effect": "Allow",
              "Principal": {
                "AWS": "*"
              },
              "Action": "SQS:SendMessage",
              "Resource": "$(sqs_arn(queue))",
              "Condition": {
                "ArnEquals": {
                  "aws:SourceArn": "$(sns_arn(aws, topic_name))"
                }
              }
            }
          ]
        }"""
    ))
end


function sns_subscribe_email(aws, topic_name, email)

    sns(aws, "Subscribe", topic_name, Endpoint = email, Protocol = "email")
end


import AWSLambda: lambda

function sns_subscribe_lambda(aws, topic_name, lambda_name)

    lambda_arn = arn(aws, "lambda", "function:$lambda_name")
    sns(aws, "Subscribe", topic_name, Endpoint = lambda_arn, Protocol = "lambda")

    lambda(aws, "POST"; path="$lambda_name/policy", query=Dict(
           "Action" => "lambda:InvokeFunction",
           "Principal" => "sns.amazonaws.com",
           "SourceArn" => sns_arn(aws, topic_name),
           "StatementId" => "sns_$topic_name"))
end


end # module AWSSNS



#==============================================================================#
# End of file.
#==============================================================================#

