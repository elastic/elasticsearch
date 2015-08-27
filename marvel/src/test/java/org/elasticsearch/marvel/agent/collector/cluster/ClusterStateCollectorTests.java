/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.cluster;

import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.marvel.agent.collector.AbstractCollectorTestCase;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.marvel.license.LicenseService;
import org.junit.Test;

import java.util.Collection;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.*;

public class ClusterStateCollectorTests extends AbstractCollectorTestCase {

    @Test
    public void testClusterStateCollectorNoIndices() throws Exception {
        Collection<MarvelDoc> results = newClusterStateCollector().doCollect();
        assertThat(results, hasSize(1));

        MarvelDoc marvelDoc = results.iterator().next();
        assertNotNull(marvelDoc);
        assertThat(marvelDoc, instanceOf(ClusterStateMarvelDoc.class));

        ClusterStateMarvelDoc clusterStateMarvelDoc = (ClusterStateMarvelDoc) marvelDoc;
        assertThat(clusterStateMarvelDoc.clusterUUID(), equalTo(client().admin().cluster().prepareState().setMetaData(true).get().getState().metaData().clusterUUID()));
        assertThat(clusterStateMarvelDoc.timestamp(), greaterThan(0L));
        assertThat(clusterStateMarvelDoc.type(), equalTo(ClusterStateCollector.TYPE));
        assertNotNull(clusterStateMarvelDoc.getClusterState());

        ClusterState clusterState = clusterStateMarvelDoc.getClusterState();
        assertThat(clusterState.getRoutingTable().allShards(), hasSize(0));
    }

    @Test
    public void testClusterStateCollectorOneIndex() throws Exception {
        int nbShards = randomIntBetween(1, 5);
        assertAcked(prepareCreate("test").setSettings(Settings.settingsBuilder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, nbShards)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()));

        int nbDocs = randomIntBetween(1, 20);
        for (int i = 0; i < nbDocs; i++) {
            client().prepareIndex("test", "test").setSource("num", i).get();
        }
        client().admin().indices().prepareRefresh().get();
        assertHitCount(client().prepareCount().get(), nbDocs);

        Collection<MarvelDoc> results = newClusterStateCollector().doCollect();
        assertThat(results, hasSize(1));

        MarvelDoc marvelDoc = results.iterator().next();
        assertNotNull(marvelDoc);
        assertThat(marvelDoc, instanceOf(ClusterStateMarvelDoc.class));

        ClusterStateMarvelDoc clusterStateMarvelDoc = (ClusterStateMarvelDoc) marvelDoc;
        assertThat(clusterStateMarvelDoc.clusterUUID(), equalTo(client().admin().cluster().prepareState().setMetaData(true).get().getState().metaData().clusterUUID()));
        assertThat(clusterStateMarvelDoc.timestamp(), greaterThan(0L));
        assertThat(clusterStateMarvelDoc.type(), equalTo(ClusterStateCollector.TYPE));

        assertNotNull(clusterStateMarvelDoc.getClusterState());

        ClusterState clusterState = clusterStateMarvelDoc.getClusterState();
        assertThat(clusterState.getRoutingTable().allShards("test"), hasSize(nbShards));
    }

    @Test
    public void testClusterStateCollectorMultipleIndices() throws Exception {
        int nbIndices = randomIntBetween(1, 5);
        int[] docsPerIndex = new int[nbIndices];
        int[] shardsPerIndex = new int[nbIndices];

        for (int i = 0; i < nbIndices; i++) {
            shardsPerIndex[i] = randomIntBetween(1, 5);
            assertAcked(prepareCreate("test-" + i).setSettings(Settings.settingsBuilder()
                    .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, shardsPerIndex[i])
                    .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                    .build()));

            docsPerIndex[i] = randomIntBetween(1, 20);
            for (int j = 0; j < docsPerIndex[i]; j++) {
                client().prepareIndex("test-" + i, "test").setSource("num", i).get();
            }
        }

        client().admin().indices().prepareRefresh().get();
        for (int i = 0; i < nbIndices; i++) {
            assertHitCount(client().prepareCount("test-" + i).get(), docsPerIndex[i]);
        }

        Collection<MarvelDoc> results = newClusterStateCollector().doCollect();
        MarvelDoc marvelDoc = results.iterator().next();
        assertNotNull(marvelDoc);
        assertThat(marvelDoc, instanceOf(ClusterStateMarvelDoc.class));

        ClusterStateMarvelDoc clusterStateMarvelDoc = (ClusterStateMarvelDoc) marvelDoc;
        assertThat(clusterStateMarvelDoc.clusterUUID(), equalTo(client().admin().cluster().prepareState().setMetaData(true).get().getState().metaData().clusterUUID()));
        assertThat(clusterStateMarvelDoc.timestamp(), greaterThan(0L));
        assertThat(clusterStateMarvelDoc.type(), equalTo(ClusterStateCollector.TYPE));
        assertNotNull(clusterStateMarvelDoc.getClusterState());

        ClusterState clusterState = clusterStateMarvelDoc.getClusterState();
        for (int i = 0; i < nbIndices; i++) {
            assertThat(clusterState.getRoutingTable().allShards("test-" + i), hasSize(shardsPerIndex[i]));
        }
    }

    @Test
    public void tesClusterStateCollectorWithLicensing() {
        String[] nodes = internalCluster().getNodeNames();
        for (String node : nodes) {
            logger.debug("--> creating a new instance of the collector");
            ClusterStateCollector collector = newClusterStateCollector(node);
            assertNotNull(collector);

            logger.debug("--> enabling license and checks that the collector can collect data if node is master");
            enableLicense();
            if (node.equals(internalCluster().getMasterName())) {
                assertCanCollect(collector);
            } else {
                assertCannotCollect(collector);
            }

            logger.debug("--> starting graceful period and checks that the collector can still collect data if node is master");
            beginGracefulPeriod();
            if (node.equals(internalCluster().getMasterName())) {
                assertCanCollect(collector);
            } else {
                assertCannotCollect(collector);
            }

            logger.debug("--> ending graceful period and checks that the collector cannot collect data");
            endGracefulPeriod();
            assertCannotCollect(collector);

            logger.debug("--> disabling license and checks that the collector cannot collect data");
            disableLicense();
            assertCannotCollect(collector);
        }
    }

    private ClusterStateCollector newClusterStateCollector() {
        return newClusterStateCollector(null);
    }

    private ClusterStateCollector newClusterStateCollector(String nodeId) {
        if (!Strings.hasText(nodeId)) {
            nodeId = randomFrom(internalCluster().getNodeNames());
        }
        return new ClusterStateCollector(internalCluster().getInstance(Settings.class, nodeId),
                internalCluster().getInstance(ClusterService.class, nodeId),
                internalCluster().getInstance(MarvelSettings.class, nodeId),
                internalCluster().getInstance(LicenseService.class, nodeId),
                client(nodeId));
    }
}
