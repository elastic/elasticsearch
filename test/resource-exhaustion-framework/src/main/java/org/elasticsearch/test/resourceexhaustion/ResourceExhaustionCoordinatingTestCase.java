/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.resourceexhaustion;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;

/**
 * Base class for resource exhaustion tests that require a dedicated coordinating node.
 * Node 0 has no data/master roles; node 1 holds data and master. Tests that want to verify
 * resource limits on the coordinating path should send requests to {@link #coordinatingClient()},
 * which connects only to node 0.
 */
public abstract class ResourceExhaustionCoordinatingTestCase extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .withNode(node -> node.setting("node.roles", "[]"))
        .withNode(node -> node.setting("node.roles", "[data,master,ingest]"))
        .setting("xpack.security.enabled", "false")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    /**
     * Returns a REST client that targets only the coordinating node (node 0).
     * The caller is responsible for closing this client.
     */
    protected RestClient coordinatingClient() throws IOException {
        String address = cluster.getHttpAddress(0);
        int portSeparator = address.lastIndexOf(':');
        String host = address.substring(0, portSeparator);
        int port = Integer.parseInt(address.substring(portSeparator + 1));
        return buildClient(restClientSettings(), new HttpHost[] { new HttpHost(host, port, getProtocol()) });
    }
}
