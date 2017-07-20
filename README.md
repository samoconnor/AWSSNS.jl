# AWSSNS

AWS SNS Interface for Julia

[![Build Status](https://travis-ci.org/samoconnor/AWSSNS.jl.svg)](https://travis-ci.org/samoconnor/AWSSNS.jl)

[Documentation](https://juliacloud.github.io/AWSCore.jl/build/AWSSNS.html)

```julia
using AWSSNS
using AWSSQS

aws = AWSCore.aws_config()

send_sms(aws, "+61123456789", "Hello")

sns_create_topic(aws, "my-topic")

q = sqs_get_queue(aws, "my-queue")
sns_subscribe_sqs(aws, "my-topic", q; raw = true)

sns_publish(aws, "my-topic", "Hello!")

m = sqs_receive_message(q)
println(m["message"])
sqs_delete_message(q, m)
```
