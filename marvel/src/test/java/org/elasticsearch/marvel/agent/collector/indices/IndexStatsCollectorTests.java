/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.indices;

import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.marvel.agent.collector.AbstractCollectorTestCase;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.marvel.license.LicenseService;
import org.junit.Test;

import java.util.Collection;
import java.util.Iterator;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.*;

public class IndexStatsCollectorTests extends AbstractCollectorTestCase {

    @Test
    public void testIndexStatsCollectorNoIndices() throws Exception {
        waitForNoBlocksOnNodes();

        Collection<MarvelDoc> results = newIndexStatsCollector().doCollect();
        assertThat(results, is(empty()));
    }

    @Test
    public void testIndexStatsCollectorOneIndex() throws Exception {
        waitForNoBlocksOnNodes();

        final String indexName = "test_" + randomInt();

        final int nbDocs = randomIntBetween(1, 20);
        for (int i = 0; i < nbDocs; i++) {
            client().prepareIndex(indexName, "test").setSource("num", i).get();
        }
        client().admin().indices().prepareRefresh().get();
        assertHitCount(client().prepareCount().get(), nbDocs);

        Collection<MarvelDoc> results = newIndexStatsCollector().doCollect();
        assertThat(results, hasSize(1));

        MarvelDoc marvelDoc = results.iterator().next();
        assertNotNull(marvelDoc);
        assertThat(marvelDoc, instanceOf(IndexStatsMarvelDoc.class));

        IndexStatsMarvelDoc indexStatsMarvelDoc = (IndexStatsMarvelDoc) marvelDoc;
        assertThat(indexStatsMarvelDoc.clusterUUID(), equalTo(client().admin().cluster().prepareState().setMetaData(true).get().getState().metaData().clusterUUID()));
        assertThat(indexStatsMarvelDoc.timestamp(), greaterThan(0L));
        assertThat(indexStatsMarvelDoc.type(), equalTo(IndexStatsCollector.TYPE));

        IndexStats indexStats = indexStatsMarvelDoc.getIndexStats();
        assertNotNull(indexStats);

        assertThat(indexStats.getIndex(), equalTo(indexName));
        assertThat(indexStats.getPrimaries().getDocs().getCount(), equalTo((long) nbDocs));
        assertNotNull(indexStats.getTotal().getStore());
        assertThat(indexStats.getTotal().getStore().getSizeInBytes(), greaterThan(0L));
        assertThat(indexStats.getTotal().getStore().getThrottleTime().millis(), equalTo(0L));
        assertNotNull(indexStats.getTotal().getIndexing());
        assertThat(indexStats.getTotal().getIndexing().getTotal().getThrottleTimeInMillis(), equalTo(0L));
    }

    @Test
    public void testIndexStatsCollectorMultipleIndices() throws Exception {
        waitForNoBlocksOnNodes();

        final String indexPrefix = "test_" + randomInt() + "_";
        int nbIndices = randomIntBetween(1, 5);
        int[] docsPerIndex = new int[nbIndices];

        for (int i = 0; i < nbIndices; i++) {
            docsPerIndex[i] = randomIntBetween(1, 20);
            for (int j = 0; j < docsPerIndex[i]; j++) {
                client().prepareIndex(indexPrefix + i, "test").setSource("num", i).get();
            }
        }

        String clusterUUID = client().admin().cluster().prepareState().setMetaData(true).get().getState().metaData().clusterUUID();
        client().admin().indices().prepareRefresh().get();
        for (int i = 0; i < nbIndices; i++) {
            assertHitCount(client().prepareCount(indexPrefix + i).get(), docsPerIndex[i]);
        }

        Collection<MarvelDoc> results = newIndexStatsCollector().doCollect();
        assertThat(results, hasSize(nbIndices));

        for (int i = 0; i < nbIndices; i++) {
            String indexName = indexPrefix + i;
            boolean found = false;

            Iterator<MarvelDoc> it = results.iterator();
            while (!found && it.hasNext()) {
                MarvelDoc marvelDoc = it.next();
                assertThat(marvelDoc, instanceOf(IndexStatsMarvelDoc.class));

                IndexStatsMarvelDoc indexStatsMarvelDoc = (IndexStatsMarvelDoc) marvelDoc;
                IndexStats indexStats = indexStatsMarvelDoc.getIndexStats();
                assertNotNull(indexStats);

                if (indexStats.getIndex().equals(indexPrefix + i)) {
                    assertThat(indexStatsMarvelDoc.clusterUUID(), equalTo(clusterUUID));
                    assertThat(indexStatsMarvelDoc.timestamp(), greaterThan(0L));
                    assertThat(indexStatsMarvelDoc.type(), equalTo(IndexStatsCollector.TYPE));

                    assertThat(indexStats.getIndex(), equalTo(indexName));
                    assertNotNull(indexStats.getTotal().getDocs());
                    assertThat(indexStats.getPrimaries().getDocs().getCount(), equalTo((long) docsPerIndex[i]));
                    assertNotNull(indexStats.getTotal().getStore());
                    assertThat(indexStats.getTotal().getStore().getSizeInBytes(), greaterThan(0L));
                    assertThat(indexStats.getTotal().getStore().getThrottleTime().millis(), equalTo(0L));
                    assertNotNull(indexStats.getTotal().getIndexing());
                    assertThat(indexStats.getTotal().getIndexing().getTotal().getThrottleTimeInMillis(), equalTo(0L));
                    found = true;
                }
            }
            assertThat("could not find collected stats for index [" + indexPrefix + i + "]", found, is(true));
        }
    }

    private IndexStatsCollector newIndexStatsCollector() {
        String nodeId = randomFrom(internalCluster().getNodeNames());
        return new IndexStatsCollector(internalCluster().getInstance(Settings.class, nodeId),
                internalCluster().getInstance(ClusterService.class, nodeId),
                internalCluster().getInstance(MarvelSettings.class, nodeId),
                internalCluster().getInstance(LicenseService.class, nodeId),
                client(nodeId));
    }
}
