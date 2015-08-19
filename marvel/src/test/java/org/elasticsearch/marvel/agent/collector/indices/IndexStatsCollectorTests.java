/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.indices;

import com.google.common.collect.ImmutableSet;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Test;

import java.util.Collection;
import java.util.Iterator;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.*;

@LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/elastic/x-plugins/issues/470")
public class IndexStatsCollectorTests extends ESSingleNodeTestCase {

    @Test
    public void testIndexStatsCollectorNoIndices() throws Exception {
        waitForNoBlocksOnNode();

        Collection<MarvelDoc> results = newIndexStatsCollector().doCollect();
        assertThat(results, is(empty()));
    }

    @Test
    public void testIndexStatsCollectorOneIndex() throws Exception {
        waitForNoBlocksOnNode();

        int nbDocs = randomIntBetween(1, 20);
        for (int i = 0; i < nbDocs; i++) {
            client().prepareIndex("test", "test").setSource("num", i).get();
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

        IndexStatsMarvelDoc.Payload payload = indexStatsMarvelDoc.payload();
        assertNotNull(payload);
        assertNotNull(payload.getIndexStats());

        assertThat(payload.getIndexStats().getIndex(), equalTo("test"));
        assertThat(payload.getIndexStats().getTotal().getDocs().getCount(), equalTo((long) nbDocs));
        assertNotNull(payload.getIndexStats().getTotal().getStore());
        assertThat(payload.getIndexStats().getTotal().getStore().getSizeInBytes(), greaterThan(0L));
        assertThat(payload.getIndexStats().getTotal().getStore().getThrottleTime().millis(), equalTo(0L));
        assertNotNull(payload.getIndexStats().getTotal().getIndexing());
        assertThat(payload.getIndexStats().getTotal().getIndexing().getTotal().getThrottleTimeInMillis(), equalTo(0L));
    }

    @Test
    public void testIndexStatsCollectorMultipleIndices() throws Exception {
        waitForNoBlocksOnNode();

        int nbIndices = randomIntBetween(1, 5);
        int[] docsPerIndex = new int[nbIndices];

        for (int i = 0; i < nbIndices; i++) {
            docsPerIndex[i] = randomIntBetween(1, 20);
            for (int j = 0; j < docsPerIndex[i]; j++) {
                client().prepareIndex("test-" + i, "test").setSource("num", i).get();
            }
        }

        String clusterUUID = client().admin().cluster().prepareState().setMetaData(true).get().getState().metaData().clusterUUID();
        client().admin().indices().prepareRefresh().get();
        for (int i = 0; i < nbIndices; i++) {
            assertHitCount(client().prepareCount("test-" + i).get(), docsPerIndex[i]);
        }

        Collection<MarvelDoc> results = newIndexStatsCollector().doCollect();
        assertThat(results, hasSize(nbIndices));

        for (int i = 0; i < nbIndices; i++) {
            boolean found = false;

            Iterator<MarvelDoc> it = results.iterator();
            while (!found && it.hasNext()) {
                MarvelDoc marvelDoc = it.next();
                assertThat(marvelDoc, instanceOf(IndexStatsMarvelDoc.class));

                IndexStatsMarvelDoc indexStatsMarvelDoc = (IndexStatsMarvelDoc) marvelDoc;

                IndexStatsMarvelDoc.Payload payload = indexStatsMarvelDoc.payload();
                assertNotNull(payload);
                assertNotNull(payload.getIndexStats());

                if (payload.getIndexStats().getIndex().equals("test-" + i)) {
                    assertThat(indexStatsMarvelDoc.clusterUUID(), equalTo(clusterUUID));
                    assertThat(indexStatsMarvelDoc.timestamp(), greaterThan(0L));
                    assertThat(indexStatsMarvelDoc.type(), equalTo(IndexStatsCollector.TYPE));

                    assertNotNull(payload.getIndexStats().getTotal().getDocs());
                    assertThat(payload.getIndexStats().getTotal().getDocs().getCount(), equalTo((long) docsPerIndex[i]));
                    assertNotNull(payload.getIndexStats().getTotal().getStore());
                    assertThat(payload.getIndexStats().getTotal().getStore().getSizeInBytes(), greaterThan(0L));
                    assertThat(payload.getIndexStats().getTotal().getStore().getThrottleTime().millis(), equalTo(0L));
                    assertNotNull(payload.getIndexStats().getTotal().getIndexing());
                    assertThat(payload.getIndexStats().getTotal().getIndexing().getTotal().getThrottleTimeInMillis(), equalTo(0L));
                    found = true;
                }
            }
            assertThat("could not find collected stats for index [test-" + i + "]", found, is(true));
        }
    }

    private IndexStatsCollector newIndexStatsCollector() {
        return new IndexStatsCollector(getInstanceFromNode(Settings.class),
                getInstanceFromNode(ClusterService.class),
                getInstanceFromNode(MarvelSettings.class),
                client());
    }

    public void waitForNoBlocksOnNode() throws InterruptedException {
        final long start = System.currentTimeMillis();
        final TimeValue timeout = TimeValue.timeValueSeconds(30);
        ImmutableSet<ClusterBlock> blocks;
        do {
            blocks = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState().blocks().global();
        }
        while (!blocks.isEmpty() && (System.currentTimeMillis() - start) < timeout.millis());
        assertTrue(blocks.isEmpty());
    }
}
