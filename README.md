# Logstash Input Plugin for Solace

A plugin for reading any data off a Solace PubSub+ event broker and injecting into Logstash.  Logstash can then be configured to push the data into Elaxtic or any other number of destination, including many monitoring databases and other message broker technologies.

This is a starter project, work-in-progress.  It is based on https://github.com/logstash-plugins/logstash-input-java_input_example and https://www.elastic.co/guide/en/logstash/current/java-input-plugin.html.

This is a JCSMP Java API alternative to https://www.elastic.co/guide/en/logstash/current/plugins-inputs-jms.html.  

Right now only supports subscribing Directly to topics, can't be used yet for Guaranteed messaging.

## Building

You have to do a few steps before you can just build this.  Namely, you need to have a local copy of Logstash downloaded and built, so that this project can reference a compiled JAR file from it.  It follows the steps outlined in the links above. 👆

1. Download a copy of Logstash source.  I cloned the 7.10 branch.  You can get other versions if you want.  https://github.com/elastic/logstash/tree/7.10
2. Set the environment variable `LS_HOME` to the directory where you saved Logstash.  E.g. ``export LS_HOME=`pwd` ``
3. Build Logstash.  E.g. `./gradlew assemble` from the Logstash directory.  (or `gradlew.bat` if Windows Command Prompt)
4. In the folder for _this_ project, create a new file `gradle.properties` with a single variable pointing to Logstash's built "core" directory.  E.g. mine looks like `LOGSTASH_CORE_PATH=../logstash-7.10/logstash-core`  as I have Logstash and this plugin in sibling directories.
5. You are now ready to compile this project. From the input plugin home directory:

```
./gradlew clean gem
```

This will generate a file that looks something like `logstash-input-solace-x.y.z.gem`.  Use the `logstash-plugin` utility in your Logstash distribution to import the generated gem file. Something like:
```
bin/logstash-plugin install --no-verify --local /home/alee/logstash-input-solace-0.0.1.gem
```

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
- `topic-subs`: (optional, will default to [">"]) array of strings of Direct topic subscriptions to subscribe to
- `queue`: (**not implemented yet**) (optional, will default to not consfigured) name of the Solace queue to connect to to read messages from; if specified, will ignore `topic-subs` subscriptionn configuration above




### Logstash Event Metadata

The following @metadata fields will be populated by the plugin. More to come!

- `solace-topic`: the Destination the message was published to
- `solace-delivery-mode`: the message's DeliveryMode, either "DIRECT" or "PERSISTENT"
- `solace-application-message-id`: (optional) if the message's Application Message ID (aka JMS Message ID) is populated
- `solace-application-message-type`: (optional) if the message's Application Message Type (aka JMS Message Type) is configured
- `solace-reply-to`: (optional) if the message's reply-to is configured
- `solace-correlation-id`: (optional) if the message's Correlation ID is configured
- `solace-sequence-number`: (optional) if the message's Long sequence number is set

In addition, `@timestamp` the Logstash event's timestamp will be updated with `msg.getSenderTimestamp()` if populated.

The payload of the received Solace message will be stored in the Logstash event field `payload`.

