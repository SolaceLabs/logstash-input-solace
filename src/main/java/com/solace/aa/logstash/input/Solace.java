package com.solace.aa.logstash.input;

import co.elastic.logstash.api.Configuration;
import co.elastic.logstash.api.Context;
import co.elastic.logstash.api.Input;
import co.elastic.logstash.api.LogstashPlugin;
import co.elastic.logstash.api.Password;
import co.elastic.logstash.api.PluginConfigSpec;
import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.FlowEventArgs;
import com.solacesystems.jcsmp.FlowEventHandler;
import com.solacesystems.jcsmp.JCSMPChannelProperties;
import com.solacesystems.jcsmp.JCSMPErrorResponseException;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.MapMessage;
import com.solacesystems.jcsmp.OperationNotSupportedException;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.SessionEventArgs;
import com.solacesystems.jcsmp.SessionEventHandler;
import com.solacesystems.jcsmp.StreamMessage;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageListener;
import java.nio.charset.Charset;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import org.apache.logging.log4j.Logger;


// class name must match plugin name
@LogstashPlugin(name="solace")
public class Solace implements Input {

    public static final PluginConfigSpec<String> HOST_CONFIG = PluginConfigSpec.stringSetting("host", "localhost");
    public static final PluginConfigSpec<String> VPN_CONFIG = PluginConfigSpec.stringSetting("vpn", "default");
    public static final PluginConfigSpec<String> USERNAME_CONFIG = PluginConfigSpec.stringSetting("username", "default");
    public static final PluginConfigSpec<Password> PASSWORD_CONFIG = PluginConfigSpec.passwordSetting("password", "default", false, false);

//    public static final PluginConfigSpec<List<Object>> TOPICS_CONFIG = PluginConfigSpec.arraySetting("topic-subs", Collections.singletonList(">"), false, false);
    public static final PluginConfigSpec<List<Object>> TOPICS_CONFIG = PluginConfigSpec.arraySetting("topic-subs", Collections.emptyList(), false, false);
    public static final PluginConfigSpec<String> QUEUE_CONFIG = PluginConfigSpec.stringSetting("queue");

    public static final List<PluginConfigSpec<?>> CONFIG_OPTIONS = new ArrayList<>();
    static {
        CONFIG_OPTIONS.add(HOST_CONFIG);
        CONFIG_OPTIONS.add(VPN_CONFIG);
        CONFIG_OPTIONS.add(USERNAME_CONFIG);
        CONFIG_OPTIONS.add(PASSWORD_CONFIG);
        CONFIG_OPTIONS.add(TOPICS_CONFIG);
        CONFIG_OPTIONS.add(QUEUE_CONFIG);
    }


    private String id;
    private final Configuration config;
    private final Logger logger;
    private final CountDownLatch done = new CountDownLatch(1);
    private volatile boolean stopped;
    private final DateTimeFormatter dtf = DateTimeFormatter.ISO_INSTANT.withZone(ZoneId.systemDefault());

    private JCSMPSession session;
    // private final XMLMessageProducer producer;

    // all plugins must provide a constructor that accepts id, Configuration, and Context
    public Solace(final String id, final Configuration config, final Context context) throws JCSMPException {
        System.out.println("Solace Input starting constructor");  // will dump to /var/log/messages
        // constructors should validate configuration options
        this.id = id;
        this.config = config;
        if (context != null) {
            logger = context.getLogger(this);
        } else {
            logger = null;
            System.out.println("Solace Input Context is null, cannot initialize Logger");
        }

        final JCSMPProperties properties = new JCSMPProperties();
        properties.setProperty(JCSMPProperties.HOST, config.get(HOST_CONFIG));          // host:port
        properties.setProperty(JCSMPProperties.VPN_NAME, config.get(VPN_CONFIG));       // message-vpn
        properties.setProperty(JCSMPProperties.USERNAME, config.get(USERNAME_CONFIG));  // client-username
        properties.setProperty(JCSMPProperties.PASSWORD, config.get(PASSWORD_CONFIG).getPassword());      // password
        if (logger != null) {
            logger.info("Solace Input Plugin configured: host={}, vpn={}, username={}",config.get(HOST_CONFIG),config.get(VPN_CONFIG),config.get(USERNAME_CONFIG));
        }
        properties.setProperty(JCSMPProperties.REAPPLY_SUBSCRIPTIONS, true);  // subscribe Direct subs after reconnect
        JCSMPChannelProperties channelProps = new JCSMPChannelProperties();
        channelProps.setReconnectRetries(-1);      // unlimited tries
        channelProps.setConnectRetriesPerHost(5);  // recommended settings
        // https://docs.solace.com/Solace-PubSub-Messaging-APIs/API-Developer-Guide/Configuring-Connection-T.htm
        properties.setProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES, channelProps);
        session = JCSMPFactory.onlyInstance().createSession(properties, null, new SessionEventHandler() {
            @Override
            public void handleEvent(SessionEventArgs event) {  // could be reconnecting, connection lost, etc.
                System.out.printf("### Received a Session event: %s%n", event);
            }
        });
        session.setProperty(JCSMPProperties.CLIENT_NAME,"logstash_input_"+session.getProperty(JCSMPProperties.CLIENT_NAME));
    }


    /**
     * Takes in a Solace message and parses out various fields. Users will probably want to add
     * additional processing logic depending on the type of message, maybe the topic hierarchy, etc.
     * @param solaceMessage a BytesXMLMessage (the super interface of TextMessage, BytesMessage, etc.)
     * @return Map<String,Object> used by Logstash to push into the pipeline
     */
    private Map<String,Object> parseSolaceMessage(BytesXMLMessage solaceMessage) {
        Map<String,Object> map = new HashMap<>();
        Map<String,Object> metadata = new HashMap<>();
        if (solaceMessage.getDestination() instanceof Topic) {
            metadata.put("solace-topic",solaceMessage.getDestination().getName());
        } else {
            metadata.put("solace-queue",solaceMessage.getDestination().getName());
        }
        metadata.put("solace-delivery-mode",  solaceMessage.getDeliveryMode().toString());
        if (solaceMessage.getApplicationMessageId() != null) metadata.put("solace-application-message-id",solaceMessage.getApplicationMessageId());
        if (solaceMessage.getReplyTo() != null) metadata.put("solace-reply-to",solaceMessage.getReplyTo().getName());
        if (solaceMessage.getCorrelationId() != null) metadata.put("solace-correlation-id",solaceMessage.getCorrelationId());
        if (solaceMessage.getApplicationMessageType() != null) metadata.put("solace-application-message-type",solaceMessage.getApplicationMessageType());
        if (solaceMessage.getSequenceNumber() != null) metadata.put("solace-sequence-number",solaceMessage.getSequenceNumber().longValue());
        metadata.put("solace-class-of-service",solaceMessage.getCos().toString());
        if (solaceMessage.getSenderTimestamp() != null) {  // overwrite default timestamp
            map.put("@timestamp", dtf.format(Instant.ofEpochMilli(solaceMessage.getSenderTimestamp())));
        }
        if (solaceMessage.getExpiration() != 0) metadata.put("solace-expiration",solaceMessage.getExpiration());
        // ANY OTHER HEADERS YOU WANT PUT IN THERE??
        
        map.put("@metadata",metadata);
        // now time for the payload!  You'll probably want to do some custom handling here...
        // also, maybe you want to parse the topic string and populate some Logstash map vars with topic levels?
        // e.g. geo/bus/gps/lat/lon/routeNum -> map.put("routeNum",topic.split("/")[5]
        if (solaceMessage instanceof TextMessage) {
            map.put("payload",((TextMessage)solaceMessage).getText());
            metadata.put("solace-message","TextMessage");
        } else {  // how do you want to decode / represent this payload??
            // hope that it's a UTF-8 string?
            map.put("payload",new String(solaceMessage.getAttachmentByteBuffer().array(),Charset.forName("UTF-8")));
            // assume it's actually binary, and base64 encode it?
//            map.put("payload",new String(Base64.getEncoder().encode(((BytesMessage)solaceMessage).getData()),Charset.forName("UTF-8")));
            if (solaceMessage instanceof MapMessage) metadata.put("solace-message","MapMessage");
            else if (solaceMessage instanceof StreamMessage) metadata.put("solace-message","StreamMessage");
            else metadata.put("solace-message","BytesMessage");

        }
        return map;
    }
    
    @Override
    public void start(Consumer<Map<String, Object>> logstashConsumer) {

        // The start method should push Map<String, Object> instances to the supplied QueueWriter
        // instance. Those will be converted to Event instances later in the Logstash event
        // processing pipeline.
        //
        // Inputs that operate on unbounded streams of data or that poll indefinitely for new
        // events should loop indefinitely until they receive a stop request. Inputs that produce
        // a finite sequence of events should loop until that sequence is exhausted or until they
        // receive a stop request, whichever comes first.

        com.solacesystems.jcsmp.Consumer solaceConsumer;  // super interface for both Direct and Queue consumers
        try {
            session.connect();
            if (config.get(QUEUE_CONFIG) != null) {  // GUARANTEED consumer
                // configure the queue API object locally
                final Queue queue = JCSMPFactory.onlyInstance().createQueue(config.get(QUEUE_CONFIG));
                // Create a Flow be able to bind to and consume messages from the Queue.
                final ConsumerFlowProperties flow_prop = new ConsumerFlowProperties();
                flow_prop.setEndpoint(queue);
                flow_prop.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);  // best practice
                flow_prop.setActiveFlowIndication(true);  // Flow events will advise when
                try {
                    // blocking Queue Receiver, will need to use receive() to pull messages off
                    solaceConsumer = session.createFlow(null, flow_prop, null, new FlowEventHandler() {
                        @Override
                        public void handleEvent(Object source, FlowEventArgs event) {
                            // Flow events are usually: active, reconnecting (i.e. unbound), reconnected, active
                            if (logger != null) logger.info("### Received a Flow event: " + event);
                            // try disabling and re-enabling the queue to see in action
                        }
                    });
                    // now let's add topic subs to the queue, if applicable
                    for (Object topic : config.get(TOPICS_CONFIG)) {
                        if (!(topic instanceof String)) {
                            throw new IllegalArgumentException("Topic parameter '"+topic.toString()+"' not a valid string");
                        }
                        try {
                            session.addSubscription(queue,JCSMPFactory.onlyInstance().createTopic(topic.toString()),JCSMPSession.WAIT_FOR_CONFIRM);
                            // might also throw jcsmp.AccessDeniedException, but we won't catch that, treat as unrecoverable error
                        } catch (JCSMPErrorResponseException e) {
                            if (logger != null) logger.warn("error adding subscription "+topic+", skipping! "+e.toString());
                        }
                    }
                } catch (OperationNotSupportedException e) {  // not allowed to do this
                    if (logger != null) {
                        logger.error("Not allowed to perform this operation! "+e.toString());
                    }
                    throw e;
                } catch (JCSMPErrorResponseException e) {  // something else went wrong: queue not exist, queue shutdown, etc.
                    if (logger != null) {
                        logger.error("Could not establish a connection to queue '{}'", config.get(QUEUE_CONFIG));
                        logger.error(e);
                    }
                    throw e;
                }
            } else {  // DIRECT Consumer
                solaceConsumer = session.getMessageConsumer((XMLMessageListener)null);
                for (Object topic : config.get(TOPICS_CONFIG)) {
                    if (!(topic instanceof String)) {
                        throw new IllegalArgumentException("Topic parameter '"+topic.toString()+"' not a valid string");
                    }
                    try {
                        session.addSubscription(JCSMPFactory.onlyInstance().createTopic(topic.toString()),true);
                    } catch (JCSMPErrorResponseException e) {
                        if (logger != null) logger.warn("error adding subscription "+topic+", skipping! "+e.toString());
                    }
                }
            }
            solaceConsumer.start();
            BytesXMLMessage solaceMessage = null;
            while (!stopped) {
                solaceMessage = solaceConsumer.receive(1000);  // break out every second just in case we have stopped
                if (solaceMessage == null) continue;  // means the 1000 timeout expired, but no message, so just loop
                Map<String, Object> map = parseSolaceMessage(solaceMessage);
                logstashConsumer.accept(map);  // java.util.function.Conusmer... JavaDocs don't say much
                solaceMessage.ackMessage();  // I'm assuming if accept() above doesn't throw, then we're good to ACK
            }
        } catch (JCSMPException e) {
            if (logger != null) logger.warn("caught exception, terminating",e);
            //throw new RuntimeException(e);
        } finally {
            stopped = true;
            try {
                Thread.sleep(500);  // wait for consumer ACKs to get propagated to broker, if applicable (e.g. queue consumer)
            } catch (InterruptedException e) {
            }
            session.closeSession();
            done.countDown();
        }
    }
        

    @Override
    public void stop() {
        session.closeSession();
        stopped = true; // set flag to request cooperative stop of input
    }

    @Override
    public void awaitStop() throws InterruptedException {
        done.await(); // blocks until input has stopped
    }

    @Override
    public Collection<PluginConfigSpec<?>> configSchema() {
        // should return a list of all configuration options for this plugin
        return CONFIG_OPTIONS;
    }

    @Override
    public String getId() {
        return this.id;
    }
}
