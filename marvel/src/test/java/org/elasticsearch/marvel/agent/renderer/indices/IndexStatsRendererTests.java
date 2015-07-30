/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.renderer.indices;

import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.index.indexing.IndexingStats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.marvel.agent.collector.indices.IndexStatsMarvelDoc;
import org.elasticsearch.marvel.agent.renderer.Renderer;
import org.elasticsearch.marvel.agent.renderer.RendererTestUtils;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

public class IndexStatsRendererTests extends ElasticsearchTestCase {

    private static final String SAMPLE_FILE = "/samples/marvel_index_stats.json";

    @Test
    public void testIndexStatsRenderer() throws Exception {
        logger.debug("--> creating the cluster stats marvel document");
        IndexStatsMarvelDoc marvelDoc = IndexStatsMarvelDoc.createMarvelDoc("test", "marvel_index_stats", 1437580442979L,
                new IndexStats("index-0", new ShardStats[0]) {
                    @Override
                    public CommonStats getTotal() {
                        CommonStats stats = new CommonStats();
                        stats.docs = new DocsStats(345678L, 123L);
                        stats.store = new StoreStats(5761573L, 0L);
                        stats.indexing = new IndexingStats(new IndexingStats.Stats(0L, 0L, 0L, 0L, 0L, 0L, 0L, true, 302L), null);
                        return stats;
                    }

                    @Override
                    public CommonStats getPrimaries() {
                        // Primaries will be filtered out by the renderer
                        CommonStats stats = new CommonStats();
                        stats.docs = new DocsStats(randomLong(), randomLong());
                        stats.store = new StoreStats(randomLong(), randomLong());
                        stats.indexing = new IndexingStats(new IndexingStats.Stats(0L, 0L, 0L, 0L, 0L, 0L, 0L, true, randomLong()), null);
                        return stats;
                    }
                });

        logger.debug("--> rendering the document");
        Renderer renderer = new IndexStatsRenderer();
        String result = RendererTestUtils.renderAsJSON(marvelDoc, renderer);

        logger.debug("--> loading sample document from file {}", SAMPLE_FILE);
        String expected = Streams.copyToStringFromClasspath(SAMPLE_FILE);

        logger.debug("--> comparing both documents, they must be identical");
        RendererTestUtils.assertJSONStructureAndValues(result, expected);
    }
}
