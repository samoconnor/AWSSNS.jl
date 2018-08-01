#==============================================================================#
# SNS/test/runtests.jl
#
# Copyright OC Technology Pty Ltd 2014 - All rights reserved
#==============================================================================#


using AWSSQS
using AWSSNS
using AWSCore
using Retry
using Test
using Dates

AWSCore.set_debug_level(1)


#-------------------------------------------------------------------------------
# Load credentials...
#-------------------------------------------------------------------------------

aws = AWSCore.aws_config(region="ap-southeast-2")



#-------------------------------------------------------------------------------
# SNS tests
#-------------------------------------------------------------------------------


for t in sns_list_topics(aws)
    if occursin(r"^ocaws-jl-test-topic", t)
        sns_delete_topic(aws, t)
    end
end


test_queue = "ocaws-jl-test-queue-" * lowercase(Dates.format(now(Dates.UTC),
                                                             "yyyymmddTHHMMSSZ"))

qa = sqs_create_queue(aws, test_queue)

sqs_set_policy(qa, """{
      "Version": "2008-10-17",
      "Id": "test_policy",
      "Statement": [
        {
          "Effect": "Allow",
          "Principal": {
            "AWS": "*"
          },
          "Action": "sqs:SendMessage",
          "Resource": "$(sqs_arn(qa))"
        }
      ]
    }""")


test_topic = "ocaws-jl-test-topic-" * lowercase(Dates.format(now(Dates.UTC),
                                                             "yyyymmddTHHMMSSZ"))

sns_create_topic(aws, test_topic)

sns_subscribe_sqs(aws, test_topic, qa; raw = true)

sns_publish(aws, test_topic, "Hello SNS!")

@repeat 6 try

    sleep(2)
    m = sqs_receive_message(qa)
    @test m != nothing && m[:message] == "Hello SNS!"

catch e
    @retry if true end
end


sqs_delete_queue(qa)
sns_delete_topic(aws, test_topic)


#==============================================================================#
# End of file.
#==============================================================================#
