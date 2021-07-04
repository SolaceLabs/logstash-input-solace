package com.solace.aaron.logstash.input;

import co.elastic.logstash.api.Configuration;
import co.elastic.logstash.api.Context;
import co.elastic.logstash.api.Input;
import co.elastic.logstash.api.LogstashPlugin;
import co.elastic.logstash.api.Password;
import co.elastic.logstash.api.PluginConfigSpec;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.JCSMPChannelProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.SessionEventArgs;
import com.solacesystems.jcsmp.SessionEventHandler;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;
import java.nio.charset.Charset;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;


// class name must match plugin name
@LogstashPlugin(name="solace")
public class Solace implements Input {

    public static final PluginConfigSpec<String> HOST_CONFIG = PluginConfigSpec.stringSetting("host", "localhost");
    public static final PluginConfigSpec<String> VPN_CONFIG = PluginConfigSpec.stringSetting("vpn", "default");
    public static final PluginConfigSpec<String> USERNAME_CONFIG = PluginConfigSpec.stringSetting("username", "default");
    public static final PluginConfigSpec<Password> PASSWORD_CONFIG = PluginConfigSpec.passwordSetting("password", "default", false, false);

    public static final PluginConfigSpec<List<Object>> TOPICS_CONFIG = PluginConfigSpec.arraySetting("topics", Collections.singletonList(">"), false, false);
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
    private final CountDownLatch done = new CountDownLatch(1);
    private volatile boolean stopped;

    private JCSMPSession session;
    // private final XMLMessageProducer producer;

    // all plugins must provide a constructor that accepts id, Configuration, and Context
    public Solace(final String id, final Configuration config, final Context context) throws JCSMPException {
        System.out.println("AARON starting constructor Solace input plugin");
        // constructors should validate configuration options
        this.id = id;
        this.config = config;

        final JCSMPProperties properties = new JCSMPProperties();
        properties.setProperty(JCSMPProperties.HOST, config.get(HOST_CONFIG));          // host:port
        properties.setProperty(JCSMPProperties.VPN_NAME, config.get(VPN_CONFIG));     // message-vpn
        properties.setProperty(JCSMPProperties.USERNAME, config.get(USERNAME_CONFIG));      // client-username
        properties.setProperty(JCSMPProperties.PASSWORD, config.get(PASSWORD_CONFIG).getPassword());      // password
            properties.setProperty(JCSMPProperties.REAPPLY_SUBSCRIPTIONS, true);  // subscribe Direct subs after reconnect
        JCSMPChannelProperties channelProps = new JCSMPChannelProperties();
        channelProps.setReconnectRetries(20);      // recommended settings
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

    final DateTimeFormatter dtf = DateTimeFormatter.ISO_INSTANT.withZone(ZoneId.systemDefault());

    @Override
    public void start(Consumer<Map<String, Object>> consumer) {

        // The start method should push Map<String, Object> instances to the supplied QueueWriter
        // instance. Those will be converted to Event instances later in the Logstash event
        // processing pipeline.
        //
        // Inputs that operate on unbounded streams of data or that poll indefinitely for new
        // events should loop indefinitely until they receive a stop request. Inputs that produce
        // a finite sequence of events should loop until that sequence is exhausted or until they
        // receive a stop request, whichever comes first.

        try {
            session.connect();
            if (config.get(QUEUE_CONFIG) != null) {
                // configure the queue API object locally
                final Queue queue = JCSMPFactory.onlyInstance().createQueue(config.get(QUEUE_CONFIG));
                // Create a Flow be able to bind to and consume messages from the Queue.
                final ConsumerFlowProperties flow_prop = new ConsumerFlowProperties();
                flow_prop.setEndpoint(queue);
                flow_prop.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);  // best practice
                flow_prop.setActiveFlowIndication(true);  // Flow events will advise when
            }
            
            XMLMessageConsumer solaceConsumer = session.getMessageConsumer((XMLMessageListener)null);
            for (Object topic : config.get(TOPICS_CONFIG)) {
                if (!(topic instanceof String)) {
                    throw new IllegalArgumentException("Topic parameter '"+topic.toString()+"' not a valid string");
                }
                session.addSubscription(JCSMPFactory.onlyInstance().createTopic(topic.toString()));
            }
            solaceConsumer.start();
            
            BytesXMLMessage msg;
            while (!stopped) {
                msg = solaceConsumer.receive(1000);  // break out every second just in case we have stopped
                if (msg == null) continue;
                Map<String,Object> map = new HashMap<>();
                Map<String,Object> metadata = new HashMap<>();
                metadata.put("solace-topic",msg.getDestination().getName());
                metadata.put("solace-delivery-mode",  msg.getDeliveryMode().toString());
                if (msg.getApplicationMessageId() != null) metadata.put("solace-application-message-id",msg.getApplicationMessageId());
                if (msg.getReplyTo() != null) metadata.put("solace-reply-to",msg.getReplyTo().getName());
                if (msg.getCorrelationId() != null) metadata.put("solace-correlation-id",msg.getCorrelationId());
                if (msg.getApplicationMessageType() != null) metadata.put("solace-application-message-type",msg.getApplicationMessageType());
                if (msg.getSequenceNumber() != null) metadata.put("solace-sequence-number",msg.getSequenceNumber().longValue());
                if (msg.getSenderTimestamp() != null) {  // overwrite default timestamp
                    map.put("@timestamp", dtf.format(Instant.ofEpochMilli(msg.getSenderTimestamp())));
                }
                map.put("@metadata",metadata);
                if (msg instanceof TextMessage) {
                    map.put("payload",((TextMessage)msg).getText());
                } else {
                    map.put("payload",new String(msg.getAttachmentByteBuffer().array(),Charset.forName("UTF-8")));
                }
                consumer.accept(map);
                msg.ackMessage();
            }
        } catch (JCSMPException e) {
            System.out.println("AARON caught exception");
            System.out.println(e);
            throw new RuntimeException(e);
        } finally {
            stopped = true;
            try {
                Thread.sleep(500);  // wait for ACKs to get propagated, if applicable
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
