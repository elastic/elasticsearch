/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.remote.test;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentFactory;
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
        assertTrue(
            cluster1Client().indices()
                .create(
                    new CreateIndexRequest("test1").settings(Settings.builder().put("index.number_of_replicas", 0).build()),
                    RequestOptions.DEFAULT
                )
                .isAcknowledged()
        );
        cluster1Client().index(
            new IndexRequest("test1").id("id1")
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .source(XContentFactory.jsonBuilder().startObject().field("foo", "bar").endObject()),
            RequestOptions.DEFAULT
        );
        assertTrue(
            cluster2Client().indices()
                .create(
                    new CreateIndexRequest("test2").settings(Settings.builder().put("index.number_of_replicas", 0).build()),
                    RequestOptions.DEFAULT
                )
                .isAcknowledged()
        );
        cluster2Client().index(
            new IndexRequest("test2").id("id1").source(XContentFactory.jsonBuilder().startObject().field("foo", "bar").endObject()),
            RequestOptions.DEFAULT
        );
        cluster2Client().index(
            new IndexRequest("test2").id("id2")
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .source(XContentFactory.jsonBuilder().startObject().field("foo", "bar").endObject()),
            RequestOptions.DEFAULT
        );
        assertEquals(1L, cluster1Client().search(new SearchRequest("test1"), RequestOptions.DEFAULT).getHits().getTotalHits().value);
        assertEquals(2L, cluster2Client().search(new SearchRequest("test2"), RequestOptions.DEFAULT).getHits().getTotalHits().value);
    }

    @After
    public void clearIndices() throws IOException {
        assertTrue(cluster1Client().indices().delete(new DeleteIndexRequest("*"), RequestOptions.DEFAULT).isAcknowledged());
        assertTrue(cluster2Client().indices().delete(new DeleteIndexRequest("*"), RequestOptions.DEFAULT).isAcknowledged());
    }

    @After
    public void clearRemoteClusterSettings() throws IOException {
        Settings setting = Settings.builder().putNull("cluster.remote.*").build();
        updateClusterSettings(cluster1Client().getLowLevelClient(), setting);
        updateClusterSettings(cluster2Client().getLowLevelClient(), setting);
    }

    public void testProxyModeConnectionWorks() throws IOException {
        String cluster2RemoteClusterSeed = "elasticsearch-" + getDistribution() + "-2:9300";
        logger.info("Configuring remote cluster [{}]", cluster2RemoteClusterSeed);
        Settings settings = Settings.builder()
            .put("cluster.remote.cluster2.mode", "proxy")
            .put("cluster.remote.cluster2.proxy_address", cluster2RemoteClusterSeed)
            .build();

        updateClusterSettings(cluster1Client().getLowLevelClient(), settings);

        assertTrue(isConnected(cluster1Client().getLowLevelClient()));

        assertEquals(
            2L,
            cluster1Client().search(new SearchRequest("cluster2:test2"), RequestOptions.DEFAULT).getHits().getTotalHits().value
        );
    }

    public void testSniffModeConnectionFails() throws IOException {
        String cluster2RemoteClusterSeed = "elasticsearch-" + getDistribution() + "-2:9300";
        logger.info("Configuring remote cluster [{}]", cluster2RemoteClusterSeed);
        Settings settings = Settings.builder()
            .put("cluster.remote.cluster2alt.mode", "sniff")
            .put("cluster.remote.cluster2alt.seeds", cluster2RemoteClusterSeed)
            .build();
        updateClusterSettings(cluster1Client().getLowLevelClient(), settings);

        assertFalse(isConnected(cluster1Client().getLowLevelClient()));
    }

    public void testHAProxyModeConnectionWorks() throws IOException {
        String proxyAddress = "haproxy:9600";
        logger.info("Configuring remote cluster [{}]", proxyAddress);
        Settings settings = Settings.builder()
            .put("cluster.remote.haproxynosn.mode", "proxy")
            .put("cluster.remote.haproxynosn.proxy_address", proxyAddress)
            .build();
        updateClusterSettings(cluster1Client().getLowLevelClient(), settings);

        assertTrue(isConnected(cluster1Client().getLowLevelClient()));

        assertEquals(
            2L,
            cluster1Client().search(new SearchRequest("haproxynosn:test2"), RequestOptions.DEFAULT).getHits().getTotalHits().value
        );
    }

    public void testHAProxyModeConnectionWithSNIToCluster1Works() throws IOException {
        assumeThat("test is only supported if the distribution contains xpack", getDistribution(), equalTo("default"));

        Settings settings = Settings.builder()
            .put("cluster.remote.haproxysni1.mode", "proxy")
            .put("cluster.remote.haproxysni1.proxy_address", "haproxy:9600")
            .put("cluster.remote.haproxysni1.server_name", "application1.example.com")
            .build();
        updateClusterSettings(cluster2Client().getLowLevelClient(), settings);

        assertTrue(isConnected(cluster2Client().getLowLevelClient()));

        assertEquals(
            1L,
            cluster2Client().search(new SearchRequest("haproxysni1:test1"), RequestOptions.DEFAULT).getHits().getTotalHits().value
        );
    }

    public void testHAProxyModeConnectionWithSNIToCluster2Works() throws IOException {
        assumeThat("test is only supported if the distribution contains xpack", getDistribution(), equalTo("default"));

        Settings settings = Settings.builder()
            .put("cluster.remote.haproxysni2.mode", "proxy")
            .put("cluster.remote.haproxysni2.proxy_address", "haproxy:9600")
            .put("cluster.remote.haproxysni2.server_name", "application2.example.com")
            .build();
        updateClusterSettings(cluster1Client().getLowLevelClient(), settings);

        assertTrue(isConnected(cluster1Client().getLowLevelClient()));

        assertEquals(
            2L,
            cluster1Client().search(new SearchRequest("haproxysni2:test2"), RequestOptions.DEFAULT).getHits().getTotalHits().value
        );
    }

    @SuppressWarnings("unchecked")
    private boolean isConnected(RestClient restClient) throws IOException {
        Optional<Object> remoteConnectionInfo = getAsMap(restClient, "/_remote/info").values().stream().findFirst();
        if (remoteConnectionInfo.isPresent()) {
            logger.info("Connection info: {}", remoteConnectionInfo);
            if (((Map<String, Object>) remoteConnectionInfo.get()).get("connected")instanceof Boolean connected) {
                return connected;
            }
        }
        return false;
    }
}
