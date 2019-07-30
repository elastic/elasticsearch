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
import org.elasticsearch.graphql.gql.GqlServer;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.Map;

public class DemoServerSocketHandler {
    private static final Logger logger = LogManager.getLogger(DemoServerSocketHandler.class);

    private GqlServer gqlServer;

    private DemoServerSocket socket;

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

                } else {
                    sendConnectionError("Unknown message type.");
                }
            }

            @Override
            public void onError(Throwable t) {
                logger.error(t);
            }

            @Override
            public void onComplete() {
                logger.info("Socket closed.");
            }
        });
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

    private void send(XContentBuilder builder) throws Exception {
        String json = serializeJson(getJavaUtilBuilderResult(builder));
        socket.send(json);
    }
}
