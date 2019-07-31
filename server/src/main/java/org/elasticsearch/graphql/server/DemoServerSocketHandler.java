/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.graphql.server;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.graphql.api.GqlApiUtils;
import static org.elasticsearch.graphql.api.GqlApiUtils.*;

import org.elasticsearch.graphql.gql.GqlResult;
import org.elasticsearch.graphql.gql.GqlServer;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class DemoServerSocketHandler {
    private static final Logger logger = LogManager.getLogger(DemoServerSocketHandler.class);

    private GqlServer gqlServer;

    private DemoServerSocket socket;

    private Map<String, Subscriber<Map<String, Object>>> subscribers = new HashMap<>();

    public DemoServerSocketHandler(GqlServer gqlServer, DemoServerSocket socket) {
        this.gqlServer = gqlServer;
        this.socket = socket;
    }

    public void handle() {
        socket.getIncomingMessages().subscribe(new Subscriber<String>() {
            @Override
            public void onSubscribe(Subscription s) {
                logger.info("Listening to new socket");
            }

            @Override
            public void onNext(String message) {
                onMessage(message);
            }

            @Override
            public void onError(Throwable t) {
                logger.error(t);
            }

            @Override
            public void onComplete() {
                logger.info("Closing socket.");
                stopAllSubscriptions();
            }
        });
    }

    private void onMessage(String message) {
        try {
            logger.info("Received message: {}", message);

            Map<String, Object> data;
            try {
                data = GqlApiUtils.parseJson(message);
            } catch (Exception e) {
                sendConnectionError("Could not parse request.");
                logger.error(e);
                return;
            }

            if (!data.containsKey("type")) {
                sendConnectionError("Message type not specified.");
                return;
            }

            if (!(data.get("type") instanceof String)) {
                sendConnectionError("Message type must be a string.");
                return;
            }

            String type = (String) data.get("type");

            if (type.equals(SocketClientMsgType.INIT.value())) {
                logger.info("GQL_INIT {}", data);
                sendAck();
            } else if (type.equals(SocketClientMsgType.START.value())) {
                logger.info("GQL_START {}", data);
                onStartMessage(data);
            } else if (type.equals(SocketClientMsgType.STOP.value())) {
                logger.info("GQL_STOP {}", data);
                onStopMessage(data);
            } else if (type.equals(SocketClientMsgType.TERMINATE.value())) {
                logger.info("CONNECTION_TERMINATE {}", data);
                stopAllSubscriptions();
            } else {
                logger.info("UNKNOWN {}", data);
                sendConnectionError("Unknown message type.");
            }
        } catch (Exception e) {
            logger.error(e);
            sendConnectionError("Could not process message.");
        }
    }

    @SuppressWarnings("unchecked")
    private void onStartMessage(Map<String, Object> data) {
        if (!data.containsKey("id")) {
            sendConnectionError("id not provided.");
            return;
        }

        if (!(data.get("id") instanceof String)) {
            sendConnectionError("id must be a string.");
            return;
        }

        String id = (String) data.get("id");

        if (id == null) {
            sendConnectionError("Invalid id.");
            return;
        }

        if (id.length() < 1) {
            sendConnectionError("Invalid id.");
            return;
        }

        if (subscribers.containsKey(id)) {
            sendError(id, "Subscription with this id already exists.");
            return;
        }

        if (!data.containsKey("payload")) {
            sendError(id, "No payload found.");
            return;
        }

        if (!(data.get("payload") instanceof Map)) {
            sendError(id, "Invalid payload type.");
            return;
        }

        Map<String, Object> payload = (Map<String, Object>) data.get("payload");

        if (payload == null) {
            sendError(id, "Payload is null.");
            return;
        }

        if (!(payload.get("query") instanceof String) || (((String) payload.get("query")).length() < 3)) {
            sendError(id, "GraphQL request must have a query.");
            return;
        }

        String query = (String) payload.get("query");
        String operationName = payload.get("operationName") instanceof String
            ? (String) payload.get("operationName") : "";
        Map<String, Object> variables = payload.get("variables") instanceof Map
            ? (Map<String, Object>) payload.get("variables") : new HashMap();

        GqlResult result = gqlServer.execute(query, operationName, variables);
        if (!result.isSubscription()) {
          sendDataOnce(id, result);
          return;
        }

        Publisher<Map<String, Object>> publisher = gqlServer.execute(query, operationName, variables).getSubscription();
        Subscriber<Map<String, Object>> subscriber = new Subscriber<Map<String, Object>>() {
            AtomicReference<Subscription> subscriptionRef = new AtomicReference<>();
            boolean completed = false;

            @Override
            public void onSubscribe(Subscription s) {
                subscriptionRef.set(s);
                s.request(1);
            }

            @Override
            public void onNext(Map<String, Object> payload) {
                if (completed) {
                    return;
                }

                try {
                    sendData(id, payload);
                    subscriptionRef.get().request(1);
                } catch (Exception e) {
                    logger.error(e);
                    sendError(id, "Error while emitting data.");
                }
            }

            @Override
            public void onError(Throwable t) {
                logger.error(t);
                sendError(id, "Subscription error.");
            }

            @Override
            public void onComplete() {
                completed = true;
                sendComplete(id);
            }
        };
        subscribers.put(id, subscriber);
        publisher.subscribe(subscriber);
    }

    private void sendDataOnce(String id, GqlResult result) {
        sendData(id, result.getSpecification());
        sendComplete(id);
    }


    private void onStopMessage(Map<String, Object> data) {
        if (!data.containsKey("id")) {
            sendConnectionError("id not provided.");
            return;
        }

        if (!(data.get("id") instanceof String)) {
            sendConnectionError("id must be a string.");
            return;
        }

        String id = (String) data.get("id");

        if (id == null) {
            sendConnectionError("Invalid id.");
            return;
        }

        if (id.length() < 1) {
            sendConnectionError("Invalid id.");
            return;
        }

        if (!subscribers.containsKey(id)) {
            sendError(id, "Subscription does not exist.");
            return;
        }

        stopSubscription(id);
    }

    private void stopSubscription(String id) {
        Subscriber<Map<String, Object>> subscriber = subscribers.get(id);
        subscribers.remove(id);
        subscriber.onComplete();
    }

    private void stopAllSubscriptions() {
        for (Subscriber<Map<String, Object>> subscriber : subscribers.values()) {
            subscriber.onComplete();
        }
        subscribers.clear();
    }

    private void sendConnectionError(String message) {
        try {
            send(createJavaUtilBuilder()
                .startObject()
                    .field("type", SocketServerMsgType.CONNECTION_ERROR.value())
                    .startObject("payload")
                        .field("error", message)
                    .endObject()
                .endObject());
        } catch (Exception e) {
            logger.error(e);
        }
    }

    private void sendError(String id, String message) {
        try {
            send(createJavaUtilBuilder()
                .startObject()
                    .field("type", SocketServerMsgType.ERROR.value())
                    .field("id", id)
                    .startObject("payload")
                        .field("error", message)
                    .endObject()
                .endObject());
        } catch (Exception e) {
            logger.error(e);
        }
    }

    private void sendAck() {
        try {
            send(createJavaUtilBuilder()
                .startObject()
                .field("type", SocketServerMsgType.CONNECTION_ACK.value())
                .endObject());
        } catch (Exception e) {
            logger.error(e);
        }
    }

    private void sendComplete(String id) {
        try {
            send(createJavaUtilBuilder()
                .startObject()
                    .field("type", SocketServerMsgType.COMPLETE.value())
                    .field("id", id)
                .endObject());
        } catch (Exception e) {
            logger.error(e);
        }
    }

    private void sendData(String id, Map<String, Object> payload) {
        logger.info("GQL_DATA {} ~> {}", id, payload);

        try {
            send(createJavaUtilBuilder()
                .startObject()
                    .field("type", SocketServerMsgType.DATA.value())
                    .field("id", id)
                    .field("payload").map(payload, false)
                .endObject());
        } catch (Exception e) {
            logger.error(e);
        }
    }

    private void send(XContentBuilder builder) throws Exception {
        String json = serializeJson(getJavaUtilBuilderResult(builder));
        socket.send(json);
    }
}
