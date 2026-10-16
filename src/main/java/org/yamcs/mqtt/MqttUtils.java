package org.yamcs.mqtt;

import java.util.Arrays;
import java.util.List;

import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.yamcs.ConfigurationException;
import org.yamcs.Spec;
import org.yamcs.YConfiguration;
import org.yamcs.events.EventProducer;
import org.yamcs.logging.Log;
import org.yamcs.Spec.OptionType;

/**
 * A set of utilities used by the MQTT packet and frame links to avoid code duplication
 */
public class MqttUtils {

    /**
     * create a new MQTT async client with the clientId and initial broker loaded from the config object
     */
    public static MqttAsyncClient newClient(YConfiguration config) throws ConfigurationException {
        try {
            List<String> brokers = config.getList("brokers");
            String clientId = config.getString("clientId", MqttClient.generateClientId());

            return new MqttAsyncClient(brokers.get(0), clientId);
        } catch (MqttException e) {
            throw new ConfigurationException(e);
        }
    }

    public static MqttConnectOptions getConnectionOptions(YConfiguration config) {
        MqttConnectOptions connOpts = new MqttConnectOptions();

        connOpts.setAutomaticReconnect(config.getBoolean("autoReconnect"));
        List<String> brokers = config.getList("brokers");
        connOpts.setServerURIs(brokers.toArray(new String[0]));
        if (config.containsKey("username")) {
            connOpts.setUserName(config.getString("username"));
            connOpts.setPassword(config.getString("password").toCharArray());
        }
        connOpts.setConnectionTimeout(config.getInt("connectionTimeoutSecs"));
        connOpts.setKeepAliveInterval(config.getInt("keepAliveSecs"));
        connOpts.setMqttVersion(getMqttVersion(config.getString("mqttVersion")));
        connOpts.setCleanSession(true);

        return connOpts;
    }

    private static int getMqttVersion(String version) {
        switch (version) {
        case "3.1":
            return MqttConnectOptions.MQTT_VERSION_3_1;
        case "3.1.1":
            return MqttConnectOptions.MQTT_VERSION_3_1_1;
        default:
            return MqttConnectOptions.MQTT_VERSION_DEFAULT;
        }
    }

    public static void addConnectionOptionsToSpec(Spec spec) {
        spec.addOption("brokers", OptionType.LIST).withElementType(OptionType.STRING).withRequired(true);
        spec.addOption("username", OptionType.STRING).withRequired(false);
        spec.addOption("password", OptionType.STRING).withRequired(false);
        spec.addOption("clientId", OptionType.STRING).withRequired(false);

        spec.addOption("connectionTimeoutSecs", OptionType.INTEGER).withDefault(5);
        spec.addOption("autoReconnect", OptionType.BOOLEAN).withDefault(true);
        spec.addOption("keepAliveSecs", OptionType.INTEGER).withDefault(60);
        spec.addOption("mqttVersion", OptionType.STRING).withChoices("default", "3.1", "3.1.1")
                .withDefault("default");
        spec.requireTogether("username", "password");
    }

    /**
     * Connect to MQTT
     */
    public static void connect(MqttConnectOptions connOpts, MqttAsyncClient client, Log log, EventProducer eventProducer)
            throws MqttException {
        log.info("Connecting to MQTT with clientId {} and options: {}", client.getClientId(), connOpts);
        client.setCallback(new ConnectionCallback(client, log, eventProducer, null));

        client.connect(connOpts, null, new IMqttActionListener() {
            @Override
            public void onSuccess(IMqttToken token) {
                log.info("Succesfully connected to MQTT");

            }

            @Override
            public void onFailure(IMqttToken t, Throwable e) {
                String msg = "Failed to connect to MQTT with clientId " + client.getClientId() + ": " + e.getMessage();
                eventProducer.sendWarning(msg);
                log.warn("{}", msg);
            }
        });
    }

    /**
     * Connect MQTT and subscribe to a given topic.
     */
    public static void connectAndSubscribe(MqttConnectOptions connOpts, MqttAsyncClient client,
            IMqttMessageListener messageListener, String topic, Log log, EventProducer eventProducer,
            SubscriptionFailureCallback subscriptionFailureCallback)
            throws MqttException {
        log.info("Connecting to MQTT with clientId {} and options: {}", client.getClientId(), connOpts);
        client.setCallback(new ConnectionCallback(client, log, eventProducer,
                () -> subscribe(client, messageListener, topic, log, eventProducer, subscriptionFailureCallback)));

        client.connect(connOpts, null, new IMqttActionListener() {
            @Override
            public void onSuccess(IMqttToken token) {
                log.info("Succesfully connected to MQTT");
                subscribe(client, messageListener, topic, log, eventProducer, subscriptionFailureCallback);
            }

            @Override
            public void onFailure(IMqttToken t, Throwable e) {
                String msg = "Failed to connect to MQTT with clientId " + client.getClientId() + ": " + e.getMessage();
                eventProducer.sendWarning(msg);
                log.warn("{}", msg);
            }
        });
    }

    private static void subscribe(MqttAsyncClient client, IMqttMessageListener messageListener, String topic,
            Log log, EventProducer eventProducer, SubscriptionFailureCallback subscriptionFailureCallback) {
        try {
            client.subscribe(topic, 2, messageListener).setActionCallback(new IMqttActionListener() {
                @Override
                public void onSuccess(IMqttToken t) {
                    int[] granted = t.getGrantedQos();
                    if (granted.length != 1 || granted[0] > 2) {
                        String msg = "Subscription to " + topic + " failed; granted QoS: "
                                + Arrays.asList(granted);
                        eventProducer.sendWarning(msg);
                        subscriptionFailureCallback.setSubscriptionFailure(new Exception(msg));
                    } else {
                        log.info("Succesfully subscribed to {}", topic);
                        subscriptionFailureCallback.setSubscriptionFailure(null);
                    }
                }

                @Override
                public void onFailure(IMqttToken t, Throwable e) {
                    String msg = "Subscription to " + topic + " failed: " + e.getMessage();
                    eventProducer.sendWarning(msg);
                    log.warn("{}", msg);
                    subscriptionFailureCallback.setSubscriptionFailure(e);
                }

            });
        } catch (MqttException e) {
            String msg = "Subscription to " + topic + " failed: " + e.getMessage();
            eventProducer.sendWarning(msg);
            log.warn("{}", msg);
            subscriptionFailureCallback.setSubscriptionFailure(e);
        }
    }

    /**
     * Reports lost connections and, if needed, subscribes again after an automatic reconnect.
     */
    static class ConnectionCallback implements MqttCallbackExtended {
        final MqttAsyncClient client;
        final Log log;
        final EventProducer eventProducer;
        final Runnable onReconnect;

        ConnectionCallback(MqttAsyncClient client, Log log, EventProducer eventProducer, Runnable onReconnect) {
            this.client = client;
            this.log = log;
            this.eventProducer = eventProducer;
            this.onReconnect = onReconnect;
        }

        @Override
        public void connectionLost(Throwable cause) {
            String msg = "Lost MQTT connection with clientId " + client.getClientId() + ": "
                    + (cause == null ? "unknown cause" : cause.getMessage());
            eventProducer.sendWarning(msg);
            log.warn("{}", msg);
        }

        @Override
        public void connectComplete(boolean reconnect, String serverURI) {
            if (!reconnect) {
                return;
            }
            String msg = "Reconnected to MQTT " + serverURI + " with clientId " + client.getClientId();
            eventProducer.sendInfo(msg);
            log.info("{}", msg);
            if (onReconnect != null) {
                onReconnect.run();
            }
        }

        @Override
        public void messageArrived(String topic, MqttMessage message) {
            // messages are delivered to the listener given at subscription
        }

        @Override
        public void deliveryComplete(IMqttDeliveryToken token) {
        }
    }

    public static void doDisable(MqttAsyncClient client) throws MqttException {
        if (client.isConnected()) {
            client.disconnect();
        }
    }

    public static void doStop(MqttAsyncClient client, NotifyStoppedCallback stopCb, NotifyFailedCallback failCb) {
        try {
            if (client.isConnected()) {
                client.disconnect(null,
                        new IMqttActionListener() {
                            @Override
                            public void onSuccess(IMqttToken t) {
                                try {
                                    client.close();
                                    stopCb.notifyStopped();
                                } catch (MqttException e) {
                                    failCb.notifyFailed(e);
                                }
                            }

                            @Override
                            public void onFailure(IMqttToken t, Throwable e) {
                                failCb.notifyFailed(e);
                            }
                        });
            } else {
                client.disconnectForcibly(0, 0, false);
                client.close();
                stopCb.notifyStopped();
            }
        } catch (MqttException e) {
            failCb.notifyFailed(e);
        }
    }

    @FunctionalInterface
    public interface NotifyStoppedCallback {
        void notifyStopped();
    }

    @FunctionalInterface
    public interface NotifyFailedCallback {
        void notifyFailed(Throwable e);
    }

    @FunctionalInterface
    public interface SubscriptionFailureCallback {
        void setSubscriptionFailure(Throwable e);
    }
}
