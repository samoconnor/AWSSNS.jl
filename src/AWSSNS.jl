#==============================================================================#
# AWSSNS.jl
#
# SNS API. See http://aws.amazon.com/documentation/sns/
#
# Copyright OC Technology Pty Ltd 2014 - All rights reserved
#==============================================================================#


__precompile__()


module AWSSNS

export sns_list_topics, sns_delete_topic, sns_create_topic, sns_subscribe_sqs,
       sns_subscribe_email, sns_subscribe_lambda, sns_publish,
       sns_list_subscriptions, sns_unsubscribe, send_sms


using AWSCore
using SymDict
using Retry
using AWSSQS



sns_arn(aws::AWSConfig, topic_name) = arn(aws, "sns", topic_name)


sns(aws::AWSConfig, action, args) = AWSCore.Services.sns(aws, action, args)


function sns(aws::AWSConfig, action::String, topic::String; args...)

    sns(aws, action, merge(stringdict(args),
                           Dict("Name" => topic,
                                "TopicArn" => sns_arn(aws, topic))))
end


"""
    sns_list_topics([::AWSConfig])

Returns a list of topic names.
"""
function sns_list_topics(aws::AWSConfig=default_aws_config())
    l = String[]
    while true
        if isempty(l)
            r = sns(aws, "ListTopics", [])
        else
            r = sns(aws, "ListTopics", ["NextToken" => r["NextToken"]])
        end
        for t in r["Topics"]
            push!(l, String(split(t["TopicArn"],":")[6]))
        end
        if !haskey(r, "NextToken") || r["NextToken"] == nothing
            break
        end
    end
    return l
end


"""
   sns_create_topic([::AWSConfig], topic_name)

Create a named topic.
"""
function sns_create_topic(aws::AWSConfig, topic_name)
    sns(aws, "CreateTopic", topic_name)
end

sns_create_topic(name) = sns_create_topic(default_aws_config(), name)


"""
   sns_delete_topic([::AWSConfig], topic_name)

Delete a named topic.
"""
function sns_delete_topic(aws::AWSConfig, topic_name)
    sns(aws, "DeleteTopic", topic_name)
end

sns_delete_topic(name) = sns_delete_topic(default_aws_config(), name)


"""
    sns_publish([::AWSConfig], topic_name, message, subject="No Subject")

Send a `message` to a named topic (with optional `subject`).
"""
function sns_publish(aws::AWSConfig, topic_name, message, subject="No Subject")

    if length(subject) > 100
        subject = subject[1:100]
    end
    sns(aws, "Publish", topic_name, Message = message, Subject = subject)
end

sns_publish(a...) = sns_publish(default_aws_config(), a...)


"""
    send_sms([::AWSConfig], number, message)

Send SMS `message` to `number`.
"""
function send_sms(aws::AWSConfig, number, message)

    sns(aws, "Publish", ["PhoneNumber" => number,
                         "Message" => message])
end

send_sms(number, message) = send_sms(default_aws_config(), number, message)


"""
    sns_subscribe_sqs([::AWSConfig], topic_name, queue; raw=false)     

Connect SQS `queue` to `topic_name`.
"""
function sns_subscribe_sqs(aws::AWSConfig, topic_name, queue; raw=false)

    r = sns(aws, "Subscribe", ["Name" => topic_name,
                               "TopicArn" => sns_arn(aws, topic_name),
                               "Endpoint" => sqs_arn(queue),
                               "Protocol" => "sqs"])

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

function sns_subscribe_sqs(a...; b...)
    sns_subscribe_sqs(default_aws_config(), a...; b...)
end


"""
    sns_subscribe_email([::AWSConfig], topic_name, email)

Connect `email` to `topic_name`.
"""
function sns_subscribe_email(aws::AWSConfig, topic_name, email)

    sns(aws, "Subscribe", topic_name, Endpoint = email, Protocol = "email")
end

sns_subscribe_email(a...) = sns_subscribe_email(default_aws_config(), a...)


import AWSLambda: lambda

"""
    sns_subscribe_lambda([::AWSConfig], topic_name, lambda_name)

Connect `lambda_name` to `topic_name`.
"""
function sns_subscribe_lambda(aws::AWSConfig, topic_name, lambda_name)

    if occursin(r"^arn", lambda_name)
        lambda_arn = lambda_name
    else
        lambda_arn = arn(aws, "lambda", "function:$lambda_name")
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

sns_subscribe_lambda(a...) = sns_subscribe_lambda(default_aws_config(), a...)


"""
    sns_list_subscriptions([::AWSConfig], topic_name)

List endpoints that are subscribed to `topic_name`.
"""
function sns_list_subscriptions(aws::AWSConfig, topic_name)

    r = sns(aws, "ListSubscriptionsByTopic", topic_name)
    r["Subscriptions"]
end

sns_list_subscriptions(a) = sns_list_subscriptions(default_aws_config(), a)


"""
    sns_unsubscribe([::AWSConfig], topic_name, SubscriptionArn)
    sns_unsubscribe([::AWSConfig], topic_name, ::Regex)

Disconnect `SubscriptionArn` from `topic_name`.
"""
function sns_unsubscribe(aws::AWSConfig, topic_name, SubscriptionArn)

    sns(aws, "Unsubscribe", topic_name, SubscriptionArn = SubscriptionArn)
end


function sns_unsubscribe(aws::AWSConfig, topic_name, pattern::Regex)

    for s in sns_list_subscriptions(aws, topic_name)
        if occursin(pattern, s["Endpoint"])
            sns_unsubscribe(aws, topic_name, s["SubscriptionArn"])
        end
    end
end

sns_unsubscribe(a...) = sns_unsubscribe(default_aws_config(), a...)


end # module AWSSNS



#==============================================================================#
# End of file.
#==============================================================================#

