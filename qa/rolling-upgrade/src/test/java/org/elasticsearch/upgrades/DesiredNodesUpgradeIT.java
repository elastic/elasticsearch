/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.upgrades;

import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.test.rest.ObjectPath;

import java.io.IOException;
import java.util.Map;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class DesiredNodesUpgradeIT extends AbstractRollingTestCase {
    public void testUpgradeDesiredNodes() throws Exception {
        // Desired nodes was introduced in 8.2
        if (UPGRADE_FROM_VERSION.before(Version.V_8_2_0)) {
            return;
        }

        switch (CLUSTER_TYPE) {
            case OLD -> {
                final var updateDesiredNodesRequest = new Request("PUT", "/_internal/desired_nodes/history/1");
                updateDesiredNodesRequest.setJsonEntity("""
                    {
                        "nodes" : [
                            {
                                "settings" : {
                                     "node.name" : "instance-000187"
                                },
                                "processors" : 8,
                                "memory" : "58gb",
                                "storage" : "1700gb",
                                "node_version" : "8.3.0"
                            }
                        ]
                    }""");
                var statusCode = client().performRequest(updateDesiredNodesRequest).getStatusLine().getStatusCode();
                assertThat(statusCode, equalTo(200));
            }
            case MIXED -> {
                final var getDesiredNodesRequest = new Request("GET", "/_internal/desired_nodes/_latest");
                Response response = client().performRequest(getDesiredNodesRequest);
                assertThat(response.getStatusLine().getStatusCode(), is(equalTo(200)));

                final var updateDesiredNodesRequest = new Request("PUT", "/_internal/desired_nodes/history/2");
                if (getMasterVersion().onOrAfter(Version.V_8_3_0)) {
                    updateDesiredNodesRequest.setJsonEntity("""
                    {
                        "nodes" : [
                            {
                                "settings" : {
                                     "node.name" : "instance-000187"
                                },
                                "processors_range" : {"min": 9.0, "max": 10.0},
                                "memory" : "58gb",
                                "storage" : "1700gb",
                                "node_version" : "8.3.0"
                            }
                        ]
                    }""");
                } else {
                    updateDesiredNodesRequest.setJsonEntity("""
                    {
                        "nodes" : [
                            {
                                "settings" : {
                                     "node.name" : "instance-000187"
                                },
                                "processors" : 9,
                                "memory" : "58gb",
                                "storage" : "1700gb",
                                "node_version" : "8.3.0"
                            }
                        ]
                    }""");
                }
                var statusCode = client().performRequest(updateDesiredNodesRequest).getStatusLine().getStatusCode();
                assertThat(statusCode, equalTo(200));
            }
            case UPGRADED -> {
                final var updateDesiredNodesRequest = new Request("PUT", "/_internal/desired_nodes/history/3");
                updateDesiredNodesRequest.setJsonEntity("""
                    {
                        "nodes" : [
                            {
                                "settings" : {
                                     "node.name" : "instance-000187"
                                },
                                "processors_range" : {"min": 9.0, "max": 10.0},
                                "memory" : "58gb",
                                "storage" : "1700gb",
                                "node_version" : "8.3.0"
                            }
                        ]
                    }""");
                var statusCode = client().performRequest(updateDesiredNodesRequest).getStatusLine().getStatusCode();
                assertThat(statusCode, equalTo(200));
            }
        }
    }

    private Version getMasterVersion() {
        return Version.V_8_2_0;
    }

    private String getNodeId(Predicate<Version> versionPredicate) throws IOException {
        Response response = client().performRequest(new Request("GET", "/_cat/master?v=true"));
        logger.info("----> {}", Streams.readFully(response.getEntity().getContent()).utf8ToString());
        return null;
    }
}
