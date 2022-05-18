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
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.cluster.metadata.DesiredNode;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class DesiredNodesUpgradeIT extends AbstractRollingTestCase {
    public void testUpgradeDesiredNodes() throws Exception {
        // Desired nodes was introduced in 8.1
        if (UPGRADE_FROM_VERSION.before(Version.V_8_1_0)) {
            return;
        }

        switch (CLUSTER_TYPE) {
            case OLD -> {
                var response = updateDesiredNodes(1, desiredNodesWithIntegerProcessor());
                var statusCode = response.getStatusLine().getStatusCode();
                assertThat(statusCode, equalTo(200));
            }
            case MIXED -> {
                final var historyVersion = FIRST_MIXED_ROUND ? 2 : 3;
                if (UPGRADE_FROM_VERSION.onOrAfter(DesiredNode.RANGE_FLOAT_PROCESSORS_SUPPORT_VERSION)) {
                    var response = updateDesiredNodes(historyVersion, desiredNodesWithRangeOrFloatProcessors());
                    var statusCode = response.getStatusLine().getStatusCode();
                    assertThat(statusCode, equalTo(200));
                } else {
                    // Processor ranges or float processors are forbidden during upgrades: 8.2 -> 8.3 clusters
                    final var responseException = expectThrows(
                        ResponseException.class,
                        () -> updateDesiredNodes(historyVersion, desiredNodesWithRangeOrFloatProcessors())
                    );
                    var statusCode = responseException.getResponse().getStatusLine().getStatusCode();
                    assertThat(statusCode, is(equalTo(400)));
                }
            }
            case UPGRADED -> {
                var response = updateDesiredNodes(4, desiredNodesWithRangeOrFloatProcessors());
                var statusCode = response.getStatusLine().getStatusCode();
                assertThat(statusCode, equalTo(200));
            }
        }

        final var getDesiredNodesRequest = new Request("GET", "/_internal/desired_nodes/_latest");
        Response response = client().performRequest(getDesiredNodesRequest);
        assertThat(response.getStatusLine().getStatusCode(), is(equalTo(200)));
    }

    private Response updateDesiredNodes(int version, String body) throws Exception {
        final var updateDesiredNodesRequest = new Request("PUT", "/_internal/desired_nodes/history/" + version);
        updateDesiredNodesRequest.setJsonEntity(body);
        return client().performRequest(updateDesiredNodesRequest);
    }

    private String desiredNodesWithRangeOrFloatProcessors() {
        if (randomBoolean()) {
            return """
                {
                    "nodes" : [
                        {
                            "settings" : {
                                 "node.name" : "instance-000187"
                            },
                            "processors_range" : {"min": 9.0, "max": 10.0},
                            "memory" : "58gb",
                            "storage" : "1tb",
                            "node_version" : "99.1.0"
                        }
                    ]
                }""";
        } else {
            return """
                {
                    "nodes" : [
                        {
                            "settings" : {
                                 "node.name" : "instance-000187"
                            },
                            "processors" : 9.5,
                            "memory" : "58gb",
                            "storage" : "1tb",
                            "node_version" : "99.1.0"
                        }
                    ]
                }""";
        }
    }

    private String desiredNodesWithIntegerProcessor() {
        return """
            {
                "nodes" : [
                    {
                        "settings" : {
                             "node.name" : "instance-000187"
                        },
                        "processors" : 9,
                        "memory" : "58gb",
                        "storage" : "1tb",
                        "node_version" : "99.1.0"
                    }
                ]
            }""";
    }
}
