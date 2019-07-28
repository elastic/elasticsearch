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
import org.elasticsearch.graphql.api.GqlApiUtils;
import org.elasticsearch.plugins.NetworkPlugin;
import static org.elasticsearch.rest.RestRequest.Method.*;

import java.util.List;
import java.util.Map;

public class StartDemoServer {
    private static final Logger logger = LogManager.getLogger(StartDemoServer.class);
    DemoServerRouter router = new DemoServerRouter();

    public StartDemoServer(List<NetworkPlugin> networkPlugins) {
        logger.info("Starting demo server.");

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
                res.setStatus(400);
                res.setHeader("Content-Type", "application/json");
                res.send("{\"error\": \"Could not parse JSON body.\"}\n");
                logger.error(e);
                return;
            }

            System.out.println("PARSED: " + body);
            res.send(req.body());
        });

        for (NetworkPlugin plugin: networkPlugins) {
            System.out.println(plugin.getClass());
            try {
                plugin.createDemoServer(router);
            } catch (Exception e) {
                logger.error("Could not start demo server: {}", e);
                e.printStackTrace(new java.io.PrintStream(System.out));
            }
        }
    }
}
