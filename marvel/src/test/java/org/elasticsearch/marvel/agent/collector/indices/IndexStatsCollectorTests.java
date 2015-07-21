/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.indices;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.junit.Test;

import java.util.Collection;
import java.util.Iterator;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.*;

public class IndexStatsCollectorTests extends ElasticsearchSingleNodeTest {

    @Test
    public void testIndexStatsCollectorNoIndices() throws Exception {
        Collection<MarvelDoc> results = newIndexStatsCollector().doCollect();
        assertThat(results, is(empty()));
    }

    @Test
    public void testIndexStatsCollectorOneIndex() throws Exception {
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
        assertThat(indexStatsMarvelDoc.clusterName(), equalTo(client().admin().cluster().prepareHealth().get().getClusterName()));
        assertThat(indexStatsMarvelDoc.timestamp(), greaterThan(0L));
        assertThat(indexStatsMarvelDoc.type(), equalTo(IndexStatsCollector.TYPE));

        assertThat(indexStatsMarvelDoc.getIndex(), equalTo("test"));
        assertNotNull(indexStatsMarvelDoc.getDocs());
        assertThat(indexStatsMarvelDoc.getDocs().getCount(), equalTo((long) nbDocs));
        assertNotNull(indexStatsMarvelDoc.getStore());
        assertThat(indexStatsMarvelDoc.getStore().getSizeInBytes(), greaterThan(0L));
        assertThat(indexStatsMarvelDoc.getStore().getThrottleTimeInMillis(), equalTo(0L));
        assertNotNull(indexStatsMarvelDoc.getIndexing());
        assertThat(indexStatsMarvelDoc.getIndexing().getThrottleTimeInMillis(), equalTo(0L));
    }

    @Test
    public void testIndexStatsCollectorMultipleIndices() throws Exception {
        int nbIndices = randomIntBetween(1, 5);
        int[] docsPerIndex = new int[nbIndices];

        for (int i = 0; i < nbIndices; i++) {
            docsPerIndex[i] = randomIntBetween(1, 20);
            for (int j = 0; j < docsPerIndex[i]; j++) {
                client().prepareIndex("test-" + i, "test").setSource("num", i).get();
            }
        }

        String clusterName = client().admin().cluster().prepareHealth().get().getClusterName();
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
                if (indexStatsMarvelDoc.getIndex().equals("test-" + i)) {
                    assertThat(indexStatsMarvelDoc.clusterName(), equalTo(clusterName));
                    assertThat(indexStatsMarvelDoc.timestamp(), greaterThan(0L));
                    assertThat(indexStatsMarvelDoc.type(), equalTo(IndexStatsCollector.TYPE));

                    assertNotNull(indexStatsMarvelDoc.getDocs());
                    assertThat(indexStatsMarvelDoc.getDocs().getCount(), equalTo((long) docsPerIndex[i]));
                    assertNotNull(indexStatsMarvelDoc.getStore());
                    assertThat(indexStatsMarvelDoc.getStore().getSizeInBytes(), greaterThan(0L));
                    assertThat(indexStatsMarvelDoc.getStore().getThrottleTimeInMillis(), equalTo(0L));
                    assertNotNull(indexStatsMarvelDoc.getIndexing());
                    assertThat(indexStatsMarvelDoc.getIndexing().getThrottleTimeInMillis(), equalTo(0L));
                    found = true;
                }
            }
            assertThat("could not find collected stats for index [test-" + i + "]", found, is(true));
        }
    }

    private IndexStatsCollector newIndexStatsCollector() {
        return new IndexStatsCollector(getInstanceFromNode(Settings.class), getInstanceFromNode(ClusterService.class), getInstanceFromNode(ClusterName.class), client());
    }
}
