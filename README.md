# Logstash Input Plugin to Solace

A plugin for reading any data off a Solace PubSub+ event broker and injecting into Logstash.  Logstash can then be configured to push the data into Elaxtic or any other number of destination, including many monitoring databases and other message broker technologies.

This is a starter project, work-in-progress.  It is based on https://github.com/logstash-plugins/logstash-input-java_input_example and https://www.elastic.co/guide/en/logstash/current/java-input-plugin.html.

This is a JCSMP Java API alternative to https://www.elastic.co/guide/en/logstash/current/plugins-inputs-jms.html.  

Right now only supports subscribing Directly to topics, can't be used yet for Guaranteed messaging.

## Building

```
./gradlew clean gem
```

Then use the `logstash-plugin` utility in your Logstash distribution to import the generated gem file.

## Example config:

```
input {
  solace {
    host => "192.168.42.35"
    vpn => "stats"
    username => "statspump"
    password => "password"
    topic-subs => [ "#STATS/>", "solace/*" ]
  }
}
```



### Parameters

- `host`: (optional, will default to "localhost") comma-separated list of Solace brokers to connect to
- `vpn`: (optional, will default to "default") name of the Message VPN to connect to
- `username`: (optional, will default to "default") client-username to connect with
- `password`: (optional, will default to "default") password for the client-username
- `topic-subs`: (optional, will default to [">"]) array of strings of Direct subscriptions
- `queue`: (**not implemented yet**) (optional, will default to not consfigured) name of the Solace queue to connect to to read messages from; if specified, will ignore `topics` subscriptionn configuration above




### Logstash Event Metadata

The following @metadata fields will be populated by the plugin. More to come!

- `solace-topic`: the Destination the message was published to
- `solace-delivery-mode`: the message's DeliveryMode, either "DIRECT" or "PERSISTENT"
- `solace-application-message-id`: (optional) if the message's Application Message ID (aka JMS Message ID) is populated
- `solace-application-message-type`: (optional) if the message's Application Message Type (aka JMS Message Type) is configured
- `solace-reply-to`: (optional) if the message's reply-to is configured
- `solace-correlation-id`: (optional) if the message's Correlation ID is configured
- `solace-sequence-number`: (optional) if the message's Long sequence number is set

In addition, `@timestamp` the Logstash event's timestamp will be updated with `msg.getSenderTimestamp()` if populated

The payload of the received Solace message will be stored in the Logstash event field `payload`.

