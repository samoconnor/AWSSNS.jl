#==============================================================================#
# AWSSNS.jl
#
# SNS API. See http://aws.amazon.com/documentation/sns/
#
# Copyright Sam O'Connor 2014 - All rights reserved
#==============================================================================#


__precompile__()


module AWSSNS

export sns_list_topics, sns_delete_topic, sns_create_topic, sns_subscribe_sqs,
       sns_subscribe_email, sns_subscribe_lambda, sns_publish,
       sns_list_subscriptsion, sns_unsubscribe


using AWSCore
using SymDict
using Retry



sns_arn(aws::AWSConfig, topic_name) = arn(aws, "sns", topic_name)


function sns(aws::AWSConfig, query)
    query["ContentType"] = "JSON"
    do_request(post_request(aws, "sns", "2010-03-31", query))
end


function sns(aws::AWSConfig, action, topic; args...)

    sns(aws, merge(StringDict(args),
                   "Action" => action,
                   "Name" => topic,
                   "TopicArn" => sns_arn(aws, topic)))
end


function sns_list_topics(aws::AWSConfig)
    r = sns(aws, Dict("Action" => "ListTopics"))
    [split(t["TopicArn"],":")[6] for t in r["Topics"]]
end


function sns_create_topic(aws::AWSConfig, topic_name)
    sns(aws, "CreateTopic", topic_name)
end


function sns_delete_topic(aws::AWSConfig, topic_name)
    sns(aws, "DeleteTopic", topic_name)
end


function sns_publish(aws::AWSConfig, topic_name, message, subject="No Subject")

    if length(subject) > 100
        subject = subject[1:100]
    end
    sns(aws, "Publish", topic_name, Message = message, Subject = subject)
end

function sns_subscribe_sqs(aws::AWSConfig, topic_name, queue; raw=false)

    r = sns(aws, Dict("Action" => "Subscribe",
                      "Name" => topic_name,
                      "TopicArn" => sns_arn(aws, topic_name),
                      "Endpoint" => arn(aws, "sqs", queue),
                      "Protocol" => "sqs"))

    if raw
        sns(aws, "SetSubscriptionAttributes", topic_name,
                  SubscriptionArn = r["SubscriptionArn"],
                  AttributeName = "RawMessageDelivery",
                  AttributeValue = "true")
    end

#=
    import AWSSQS: sqs, sqs_get_queue

    This is probably a bad idea.
    It might wipe out a policy already attached to the queue

    q = sqs_get_queue(aws, queue)

    sqs(q, Dict(
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
              "Resource": "$(arn(aws, "sqs", queue))",
              "Condition": {
                "ArnEquals": {
                  "aws:SourceArn": "$(sns_arn(aws, topic_name))"
                }
              }
            }
          ]
        }"""
    ))
=#
end


function sns_subscribe_email(aws::AWSConfig, topic_name, email)

    sns(aws, "Subscribe", topic_name, Endpoint = email, Protocol = "email")
end


import AWSLambda: lambda

function sns_subscribe_lambda(aws::AWSConfig, topic_name, lambda_name)


    if ismatch(r"^arn", lambda_name)
        lambda_arn = lambda_name
        lambda_name = split(lambda_arn, ":")[7]
        laws = copy(aws)
        laws[:region] = arn_region(lambda_arn)
    else
        lambda_arn = arn(aws, "lambda", "function:$lambda_name")
        laws = aws
    end

    sns(aws, "Subscribe", topic_name, Endpoint = lambda_arn, Protocol = "lambda")

# FIXME does not scale. End up with:
#   ERROR: LoadError: 400 -- The final policy size is bigger than the limit.
#=
    @protected try

        lambda(laws, "POST"; path="$lambda_name/policy", query=Dict(
               "Action" => "lambda:InvokeFunction",
               "Principal" => "sns.amazonaws.com",
               "SourceArn" => sns_arn(aws, topic_name),
               "StatementId" => "sns_$topic_name"))
    catch e
        @ignore if e.code == "409" end
    end
=#
end


function sns_list_subscriptsion(aws::AWSConfig, topic_name)
    r = sns(aws, "ListSubscriptionsByTopic", topic_name)
    r["Subscriptions"]
end


function sns_unsubscribe(aws::AWSConfig, topic_name, SubscriptionArn)

    sns(aws, "Unsubscribe", topic_name, SubscriptionArn = SubscriptionArn)
end


function sns_unsubscribe(aws::AWSConfig, topic_name, pattern::Regex)

    for s in sns_list_subscriptsion(aws, topic_name)
        if ismatch(pattern, s["Endpoint"])
            sns_unsubscribe(aws, topic_name, s["SubscriptionArn"])
        end
    end
end


end # module AWSSNS



#==============================================================================#
# End of file.
#==============================================================================#

