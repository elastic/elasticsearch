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

import graphql.ExecutionResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.graphql.api.GqlApiUtils;
import org.elasticsearch.graphql.gql.GqlResult;
import org.elasticsearch.graphql.gql.GqlServer;
import org.elasticsearch.plugins.NetworkPlugin;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import static org.elasticsearch.rest.RestRequest.Method.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StartDemoServer {
    private static final Logger logger = LogManager.getLogger(StartDemoServer.class);

    private GqlServer gqlServer;

    private DemoServerRouter router = new DemoServerRouter();

    @SuppressWarnings("unchecked")
    public StartDemoServer(List<NetworkPlugin> networkPlugins, GqlServer gqlServer) {
        logger.info("Starting demo server.");

        this.gqlServer = gqlServer;

        router.addRoute(GET, "/ping", (req, res) -> {
            res.send("pong");
        });

        router.addRoute(GET, "/stream-test", (req, res) -> {
            res.sendHeadersChunk();
            res.sendChunk("abc");
            res.sendChunk("123");
            res.end();
        });

        router.addRoute(POST, "/graphql", (req, res) -> {
            Map<String, Object> body;

            try {
                body = GqlApiUtils.parseJson(req.body());
            } catch (Exception e) {
                res.sendJsonError("Could not parse JSON body.");
                logger.error(e);
                return;
            }

            if (!(body.get("query") instanceof String) || (((String) body.get("query")).length() < 3)) {
                res.sendJsonError("GraphQL request must have a query.");
                return;
            }

            String query = (String) body.get("query");
            String operationName = body.get("operationName") instanceof String
                ? (String) body.get("operationName") : "";
            Map<String, Object> variables = body.get("variables") instanceof Map
                ? (Map<String, Object>) body.get("variables") : new HashMap();

            GqlResult result = gqlServer.execute(query, operationName, variables);

            if (result.hasDeferredResults()) {
                res.sendHeadersChunk();
                res.sendChunk(GqlApiUtils.serializeJson(result.getSpecification()));

                result.getDeferredResults().subscribe(new Subscriber<ExecutionResult>() {

                    Subscription subscription;

                    @Override
                    public void onSubscribe(Subscription s) {
                        subscription = s;
                        subscription.request(1);
                    }

                    @Override
                    public void onNext(ExecutionResult executionResult) {
                        res.sendChunk(GqlApiUtils.serializeJson(executionResult.toSpecification()));
                        subscription.request(1);
                    }

                    @Override
                    public void onError(Throwable t) {
                        res.sendChunk("{\"error\": \"Something went wrong while streaming deferred results.\"}");
                    }

                    @Override
                    public void onComplete() {
                        res.end();
                    }
                });
            } else {
                logger.info("GraphQL result: {}", result);
                logger.info("JSON: {}", GqlApiUtils.serializeJson(result));

                res.setHeader("Content-Type", "application/json");
                res.send(GqlApiUtils.serializeJson(result.getSpecification()));
            }
        });

        for (NetworkPlugin plugin: networkPlugins) {
            System.out.println(plugin.getClass());
            try {
                plugin.createDemoServer(router);
            } catch (Exception e) {
                logger.info("Could not start demo server.");
                logger.error(e);
                e.printStackTrace(new java.io.PrintStream(System.out));
            }
        }
    }
}
