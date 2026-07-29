/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.engine;

import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.shard.ShardFieldStats;
import org.elasticsearch.xpack.stateless.StatelessPlugin;
import org.junit.After;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Verifies that {@link IndexEngine#shardFieldStats()} reports live-docs bytes from commit metadata
 * ({@link DirectoryReaderHeapEstimator#softDeleteBitsetBytes}) rather than unwrapping {@code getLiveDocs()}.
 */
public class LiveDocsEstimationTests extends AbstractEngineTestCase {

    @After
    public void assertWarningHeaders() {
        assertWarnings(
            "[indices.merge.scheduler.use_thread_pool] setting was deprecated in Elasticsearch and will be removed in a future release. "
                + "See the breaking changes documentation for the next major version."
        );
    }

    public void testShardFieldStatsLiveDocsFromSoftDeleteMetadata() throws IOException {
        Settings nodeSettings = Settings.builder().put(StatelessPlugin.STATELESS_ENABLED.getKey(), true).build();
        try (var engine = newIndexEngine(indexConfig(Settings.EMPTY, nodeSettings, () -> 1L, NoMergePolicy.INSTANCE))) {
            // Enough docs that the FixedBitSet backing array needs multiple words (>64 bits).
            int numDocs = randomIntBetween(100, 1000);
            for (int i = 0; i < numDocs; i++) {
                engine.index(randomDoc("doc_" + i));
            }
            engine.refresh("test");

            ShardFieldStats stats = engine.shardFieldStats();
            assertThat(stats.numSegments(), equalTo(1));
            assertThat(stats.liveDocsBytes(), equalTo(0L));

            engine.delete(new Engine.Delete("doc_0", Uid.encodeId("doc_0"), 1L));
            engine.refresh("test");

            stats = engine.shardFieldStats();
            long expectedLiveDocsBytes = expectedSoftDeleteBitsetBytes(engine);
            assertThat(expectedLiveDocsBytes, greaterThan(0L));
            assertThat(stats.liveDocsBytes(), equalTo(expectedLiveDocsBytes));
            // Closed-form FixedBitSet.bits2words(maxDoc)*8 matches the estimator (no object header).
            assertThat(stats.liveDocsBytes(), equalTo(sumBits2WordsBytes(engine)));

            engine.delete(new Engine.Delete("doc_1", Uid.encodeId("doc_1"), 1L));
            engine.refresh("test");

            stats = engine.shardFieldStats();
            expectedLiveDocsBytes = expectedSoftDeleteBitsetBytes(engine);
            assertThat(expectedLiveDocsBytes, greaterThan(0L));
            assertThat(stats.liveDocsBytes(), equalTo(expectedLiveDocsBytes));
            assertThat(stats.liveDocsBytes(), equalTo(sumBits2WordsBytes(engine)));
        }
    }

    private static long expectedSoftDeleteBitsetBytes(IndexEngine engine) throws IOException {
        try (var searcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL)) {
            SegmentInfos infos = IndexEngine.getSegmentInfos(searcher.getDirectoryReader());
            long total = 0L;
            for (SegmentCommitInfo sci : infos) {
                if (sci.getSoftDelCount() > 0) {
                    total += DirectoryReaderHeapEstimator.softDeleteBitsetBytes(sci);
                }
            }
            return total;
        }
    }

    private static long sumBits2WordsBytes(IndexEngine engine) throws IOException {
        try (var searcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL)) {
            SegmentInfos infos = IndexEngine.getSegmentInfos(searcher.getDirectoryReader());
            long total = 0L;
            for (SegmentCommitInfo sci : infos) {
                if (sci.getSoftDelCount() > 0) {
                    total += (long) FixedBitSet.bits2words(sci.info.maxDoc()) * Long.BYTES;
                }
            }
            return total;
        }
    }
}
