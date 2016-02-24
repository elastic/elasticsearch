/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.shards;

import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.marvel.MarvelSettings;
import org.elasticsearch.marvel.agent.collector.AbstractCollectorTestCase;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;
import org.elasticsearch.marvel.license.MarvelLicensee;

import java.util.Collection;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class ShardsCollectorTests extends AbstractCollectorTestCase {

    public void testShardsCollectorNoIndices() throws Exception {
        Collection<MarvelDoc> results = newShardsCollector().doCollect();
        assertThat(results, hasSize(0));
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(MarvelSettings.INDICES.getKey(), "test-shards*")
                .build();
    }

    public void testShardsCollectorOneIndex() throws Exception {
        int nbDocs = randomIntBetween(1, 20);
        for (int i = 0; i < nbDocs; i++) {
            client().prepareIndex("test-shards", "test").setSource("num", i).get();
        }

        waitForRelocation();
        securedEnsureGreen();
        securedRefresh();

        assertHitCount(client().prepareSearch().setSize(0).get(), nbDocs);

        Collection<MarvelDoc> results = newShardsCollector().doCollect();
        assertThat(results, hasSize(getNumShards("test-shards").totalNumShards));

        final ClusterState clusterState = client().admin().cluster().prepareState().setMetaData(true).get().getState();

        int primaries = 0;
        int replicas = 0;

        for (MarvelDoc marvelDoc : results) {
            assertNotNull(marvelDoc);
            assertThat(marvelDoc, instanceOf(ShardMarvelDoc.class));

            ShardMarvelDoc shardMarvelDoc = (ShardMarvelDoc) marvelDoc;
            assertThat(shardMarvelDoc.getClusterUUID(), equalTo(clusterState.metaData().clusterUUID()));
            assertThat(shardMarvelDoc.getTimestamp(), greaterThan(0L));
            assertThat(shardMarvelDoc.getType(), equalTo(ShardsCollector.TYPE));
            assertThat(shardMarvelDoc.getId(),
                    equalTo(ShardsCollector.id(clusterState.stateUUID(), ((ShardMarvelDoc) marvelDoc).getShardRouting())));
            assertThat(shardMarvelDoc.getSourceNode(), notNullValue());
            assertThat(shardMarvelDoc.getClusterStateUUID(), equalTo(clusterState.stateUUID()));

            ShardRouting shardRouting = shardMarvelDoc.getShardRouting();
            assertNotNull(shardRouting);
            assertThat(shardMarvelDoc.getShardRouting().assignedToNode(), is(true));

            if (shardRouting.primary()) {
                primaries++;
            } else {
                replicas++;
            }
        }

        int expectedPrimaries = getNumShards("test-shards").numPrimaries;
        int expectedReplicas = expectedPrimaries * getNumShards("test-shards").numReplicas;
        assertThat(primaries, equalTo(expectedPrimaries));
        assertThat(replicas, equalTo(expectedReplicas));
    }

    public void testShardsCollectorMultipleIndices() throws Exception {
        final String indexPrefix = "test-shards-";
        final int nbIndices = randomIntBetween(1, 3);
        final int[] nbDocsPerIndex = new int[nbIndices];

        for (int i = 0; i < nbIndices; i++) {
            String index = indexPrefix + String.valueOf(i);
            assertAcked(prepareCreate(index));

            nbDocsPerIndex[i] = randomIntBetween(1, 20);
            for (int j = 0; j < nbDocsPerIndex[i]; j++) {
                client().prepareIndex(index, "test").setSource("num", i).get();
            }
        }

        waitForRelocation();
        securedRefresh();

        int totalShards = 0;
        for (int i = 0; i < nbIndices; i++) {
            String index = indexPrefix + String.valueOf(i);

            assertHitCount(client().prepareSearch(index).setSize(0).get(), nbDocsPerIndex[i]);
            disableAllocation(index);
            totalShards += getNumShards(index).totalNumShards;
        }

        Collection<MarvelDoc> results = newShardsCollector().doCollect();
        assertThat(results, hasSize(totalShards));

        final ClusterState clusterState = client().admin().cluster().prepareState().setMetaData(true).get().getState();

        for (MarvelDoc marvelDoc : results) {
            assertNotNull(marvelDoc);
            assertThat(marvelDoc, instanceOf(ShardMarvelDoc.class));

            ShardMarvelDoc shardMarvelDoc = (ShardMarvelDoc) marvelDoc;
            assertThat(shardMarvelDoc.getClusterUUID(), equalTo(clusterState.metaData().clusterUUID()));
            assertThat(shardMarvelDoc.getTimestamp(), greaterThan(0L));
            assertThat(shardMarvelDoc.getType(), equalTo(ShardsCollector.TYPE));
            assertThat(shardMarvelDoc.getId(),
                    equalTo(ShardsCollector.id(clusterState.stateUUID(), ((ShardMarvelDoc) marvelDoc).getShardRouting())));
            assertThat(shardMarvelDoc.getClusterStateUUID(), equalTo(clusterState.stateUUID()));

            ShardRouting shardRouting = shardMarvelDoc.getShardRouting();
            assertNotNull(shardRouting);
        }

        // Checks that a correct number of ShardMarvelDoc documents has been created for each index
        int[] shards = new int[nbIndices];
        for (MarvelDoc marvelDoc : results) {
            ShardRouting routing = ((ShardMarvelDoc) marvelDoc).getShardRouting();
            int index = Integer.parseInt(routing.getIndexName().substring(indexPrefix.length()));
            shards[index]++;
        }

        for (int i = 0; i < nbIndices; i++) {
            String index = indexPrefix + String.valueOf(i);
            int total = getNumShards(index).totalNumShards;
            assertThat("expecting " + total + " shards monitoring documents for index [" + index + "]", shards[i], equalTo(total));
        }
    }

    public void testShardsCollectorWithLicensing() {
        try {
            String[] nodes = internalCluster().getNodeNames();
            for (String node : nodes) {
                logger.debug("--> creating a new instance of the collector");
                ShardsCollector collector = newShardsCollector(node);
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
        } finally {
            // Ensure license is enabled before finishing the test
            enableLicense();
        }
    }

    private ShardsCollector newShardsCollector() {
        // This collector runs on master node only
        return newShardsCollector(internalCluster().getMasterName());
    }

    private ShardsCollector newShardsCollector(String nodeId) {
        assertNotNull(nodeId);
        return new ShardsCollector(internalCluster().getInstance(Settings.class, nodeId),
                internalCluster().getInstance(ClusterService.class, nodeId),
                internalCluster().getInstance(MarvelSettings.class, nodeId),
                internalCluster().getInstance(MarvelLicensee.class, nodeId));
    }
}
