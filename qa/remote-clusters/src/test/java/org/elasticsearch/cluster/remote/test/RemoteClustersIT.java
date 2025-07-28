/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster.remote.test;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assume.assumeThat;

public class RemoteClustersIT extends AbstractMultiClusterRemoteTestCase {

    @Before
    public void setupIndices() throws IOException {
        assertTrue(createIndex(cluster1Client(), "test1", Settings.builder().put("index.number_of_replicas", 0).build()).isAcknowledged());
        {
            Request createDoc = new Request("POST", "/test1/_doc/id1?refresh=true");
            createDoc.setJsonEntity("""
                { "foo": "bar" }
                """);
            assertOK(cluster1Client().performRequest(createDoc));
        }
        {
            Request searchRequest = new Request("POST", "/test1/_search");
            ObjectPath doc = ObjectPath.createFromResponse(cluster1Client().performRequest(searchRequest));
            assertEquals(1, (int) doc.evaluate("hits.total.value"));
        }

        assertTrue(createIndex(cluster2Client(), "test2", Settings.builder().put("index.number_of_replicas", 0).build()).isAcknowledged());
        {
            Request createDoc = new Request("POST", "/test2/_doc/id1?refresh=true");
            createDoc.setJsonEntity("""
                { "foo": "bar" }
                """);
            assertOK(cluster2Client().performRequest(createDoc));
        }
        {
            Request createDoc = new Request("POST", "/test2/_doc/id2?refresh=true");
            createDoc.setJsonEntity("""
                { "foo": "bar" }
                """);
            assertOK(cluster2Client().performRequest(createDoc));
        }
        {
            Request searchRequest = new Request("POST", "/test2/_search");
            ObjectPath doc = ObjectPath.createFromResponse(cluster2Client().performRequest(searchRequest));
            assertEquals(2, (int) doc.evaluate("hits.total.value"));
        }
    }

    @After
    public void clearIndices() throws IOException {
        assertTrue(deleteIndex(cluster1Client(), "*").isAcknowledged());
        assertTrue(deleteIndex(cluster2Client(), "*").isAcknowledged());
    }

    @After
    public void clearRemoteClusterSettings() throws IOException {
        Settings setting = Settings.builder().putNull("cluster.remote.*").build();
        updateClusterSettings(cluster1Client(), setting);
        updateClusterSettings(cluster2Client(), setting);
    }

    public void testProxyModeConnectionWorks() throws IOException {
        String cluster2RemoteClusterSeed = "elasticsearch-" + getDistribution() + "-2:9300";
        logger.info("Configuring remote cluster [{}]", cluster2RemoteClusterSeed);
        Settings settings = Settings.builder()
            .put("cluster.remote.cluster2.mode", "proxy")
            .put("cluster.remote.cluster2.proxy_address", cluster2RemoteClusterSeed)
            .build();

        updateClusterSettings(cluster1Client(), settings);

        assertTrue(isConnected(cluster1Client()));

        {
            Request searchRequest = new Request("POST", "/cluster2:test2/_search");
            ObjectPath doc = ObjectPath.createFromResponse(cluster1Client().performRequest(searchRequest));
            assertEquals(2, (int) doc.evaluate("hits.total.value"));
        }
    }

    public void testSniffModeConnectionFails() throws IOException {
        String cluster2RemoteClusterSeed = "elasticsearch-" + getDistribution() + "-2:9300";
        logger.info("Configuring remote cluster [{}]", cluster2RemoteClusterSeed);
        Settings settings = Settings.builder()
            .put("cluster.remote.cluster2alt.mode", "sniff")
            .put("cluster.remote.cluster2alt.seeds", cluster2RemoteClusterSeed)
            .build();
        updateClusterSettings(cluster1Client(), settings);

        assertFalse(isConnected(cluster1Client()));
    }

    public void testHAProxyModeConnectionWorks() throws IOException {
        String proxyAddress = "haproxy:9600";
        logger.info("Configuring remote cluster [{}]", proxyAddress);
        Settings settings = Settings.builder()
            .put("cluster.remote.haproxynosn.mode", "proxy")
            .put("cluster.remote.haproxynosn.proxy_address", proxyAddress)
            .build();
        updateClusterSettings(cluster1Client(), settings);

        assertTrue(isConnected(cluster1Client()));

        {
            Request searchRequest = new Request("POST", "/haproxynosn:test2/_search");
            ObjectPath doc = ObjectPath.createFromResponse(cluster1Client().performRequest(searchRequest));
            assertEquals(2, (int) doc.evaluate("hits.total.value"));
        }
    }

    public void testHAProxyModeConnectionWithSNIToCluster1Works() throws IOException {
        assumeThat("test is only supported if the distribution contains xpack", getDistribution(), equalTo("default"));

        Settings settings = Settings.builder()
            .put("cluster.remote.haproxysni1.mode", "proxy")
            .put("cluster.remote.haproxysni1.proxy_address", "haproxy:9600")
            .put("cluster.remote.haproxysni1.server_name", "application1.example.com")
            .build();
        updateClusterSettings(cluster2Client(), settings);

        assertTrue(isConnected(cluster2Client()));

        {
            Request searchRequest = new Request("POST", "/haproxysni1:test1/_search");
            ObjectPath doc = ObjectPath.createFromResponse(cluster2Client().performRequest(searchRequest));
            assertEquals(1, (int) doc.evaluate("hits.total.value"));
        }
    }

    public void testHAProxyModeConnectionWithSNIToCluster2Works() throws IOException {
        assumeThat("test is only supported if the distribution contains xpack", getDistribution(), equalTo("default"));

        Settings settings = Settings.builder()
            .put("cluster.remote.haproxysni2.mode", "proxy")
            .put("cluster.remote.haproxysni2.proxy_address", "haproxy:9600")
            .put("cluster.remote.haproxysni2.server_name", "application2.example.com")
            .build();
        updateClusterSettings(cluster1Client(), settings);

        assertTrue(isConnected(cluster1Client()));

        {
            Request searchRequest = new Request("POST", "/haproxysni2:test2/_search");
            ObjectPath doc = ObjectPath.createFromResponse(cluster1Client().performRequest(searchRequest));
            assertEquals(2, (int) doc.evaluate("hits.total.value"));
        }
    }

    @SuppressWarnings("unchecked")
    private boolean isConnected(RestClient restClient) throws IOException {
        Optional<Object> remoteConnectionInfo = getAsMap(restClient, "/_remote/info").values().stream().findFirst();
        if (remoteConnectionInfo.isPresent()) {
            logger.info("Connection info: {}", remoteConnectionInfo);
            if (((Map<String, Object>) remoteConnectionInfo.get()).get("connected") instanceof Boolean connected) {
                return connected;
            }
        }
        return false;
    }
}
