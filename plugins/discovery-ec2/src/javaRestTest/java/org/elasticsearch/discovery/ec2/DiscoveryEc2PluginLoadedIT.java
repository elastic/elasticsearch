/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.discovery.ec2;

import org.elasticsearch.client.Request;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

import static org.hamcrest.Matchers.hasItem;

public class DiscoveryEc2PluginLoadedIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local().plugin("discovery-ec2").build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testPluginLoaded() throws IOException {
        final var nodesInfoResponse = assertOKAndCreateObjectPath(client().performRequest(new Request("GET", "/_nodes/plugins")));
        for (final var nodeId : nodesInfoResponse.evaluateMapKeys("nodes")) {
            final var pluginCount = asInstanceOf(List.class, nodesInfoResponse.evaluateExact("nodes", nodeId, "plugins")).size();
            final var pluginNames = new HashSet<String>();
            for (int i = 0; i < pluginCount; i++) {
                pluginNames.add(
                    Objects.requireNonNull(nodesInfoResponse.evaluateExact("nodes", nodeId, "plugins", Integer.toString(i), "name"))
                );
            }
            assertThat(pluginNames, hasItem("discovery-ec2"));
        }
    }

}
