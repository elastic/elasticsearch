/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.renderer.indices;

import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.fielddata.FieldDataStats;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.IndexingStats;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.marvel.agent.collector.indices.IndexStatsMarvelDoc;
import org.elasticsearch.marvel.agent.renderer.Renderer;
import org.elasticsearch.marvel.agent.renderer.RendererTestUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.StreamsUtils;

public class IndexStatsRendererTests extends ESTestCase {
    private static final String SAMPLE_FILE = "/samples/index_stats.json";

    public void testIndexStatsRenderer() throws Exception {
        logger.debug("--> creating the index stats monitoring document");
        IndexStatsMarvelDoc marvelDoc = new IndexStatsMarvelDoc();
        marvelDoc.setClusterUUID("test");
        marvelDoc.setType("index_stats");
        marvelDoc.setTimestamp(1437580442979L);
        marvelDoc.setIndexStats(
                new IndexStats("index-0", new ShardStats[0]) {
                    @Override
                    public CommonStats getTotal() {
                        CommonStats stats = new CommonStats();
                        stats.docs = new DocsStats(345678L, 123L);
                        stats.store = new StoreStats(5761573L, 0L);
                        stats.indexing = new IndexingStats(new IndexingStats.Stats(3L, 71L, 0L, 0L, 0L, 0L, 0L, 0L, true, 302L), null);
                        stats.search = new SearchStats(new SearchStats.Stats(1L, 7L, 0L, 0L, 0L, 0L, 0L, 0L, 0L), 0L, null);
                        stats.merge = new MergeStats();
                        stats.merge.add(0L, 0L, 0L, 42L, 0L, 0L, 0L, 0L, 0L, 0L);
                        stats.refresh = new RefreshStats(0L, 978L);
                        stats.fieldData = new FieldDataStats(123456L, 0L, null);
                        stats.segments = new SegmentsStats();
                        stats.segments.add(0, 87965412L);
                        return stats;
                    }

                    @Override
                    public CommonStats getPrimaries() {
                        // Primaries will be filtered out by the renderer
                        CommonStats stats = new CommonStats();
                        stats.docs = new DocsStats(345678L, randomLong());
                        stats.store = new StoreStats(randomLong(), randomLong());
                        stats.indexing = new IndexingStats(new IndexingStats.Stats(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, true,
                                randomLong()), null);
                        stats.search = new SearchStats(new SearchStats.Stats(1L, 7L, 0L, 0L, 0L, 0L, 0L, 0L, 0L), 0L, null);
                        stats.merge = new MergeStats();
                        stats.merge.add(0L, 0L, 0L, 42L, 0L, 0L, 0L, 0L, 0L, 0L);
                        stats.refresh = new RefreshStats(0L, 978L);
                        stats.fieldData = new FieldDataStats(123456L, 0L, null);
                        stats.segments = new SegmentsStats();
                        stats.segments.add(0, 87965412L);
                        return stats;
                    }
                });

        logger.debug("--> rendering the document");
        Renderer renderer = new IndexStatsRenderer();
        String result = RendererTestUtils.renderAsJSON(marvelDoc, renderer);

        logger.debug("--> loading sample document from file {}", SAMPLE_FILE);
        String expected = StreamsUtils.copyToStringFromClasspath(SAMPLE_FILE);

        logger.debug("--> comparing both documents, they must be identical");
        RendererTestUtils.assertJSONStructureAndValues(result, expected);
    }
}
