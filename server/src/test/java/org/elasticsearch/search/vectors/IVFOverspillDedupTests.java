/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidIndexFormat;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfMergeConfigResolver;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfQueryConfigResolver;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfSegmentConfig;
import org.elasticsearch.index.codec.vectors.diskbbq.QuantEncoding;
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextRescoreOversampleTestFixture;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A vector that SOAR assigns to two posting lists is collected once per copy, each scored relative to its own centroid. The leaf must
 * keep the better of the two approximate scores; keeping an arbitrary copy lets a good candidate rank too low or fall out of the merge
 * window.
 */
public class IVFOverspillDedupTests extends ESTestCase {

    public void testPoolKeepsBestOverspillCopy() throws IOException {
        IvfSegmentConfig config = IvfSegmentConfig.of(
            CentroidIndexFormat.FLAT,
            new IvfSegmentConfig.OsqConfig(QuantEncoding.ONE_BIT_4BIT_QUERY),
            false,
            3f
        );
        try (
            Directory dir = newDirectory();
            DirectoryReader reader = ESNextRescoreOversampleTestFixture.buildTwoCommitsTwoSegments(
                dir,
                64,
                2000,
                config,
                config,
                IvfMergeConfigResolver.useCodecDefault()
            )
        ) {
            float[] queryVector = new float[64];
            for (int i = 0; i < queryVector.length; i++) {
                queryVector[i] = randomFloat();
            }
            VectorUtil.l2normalize(queryVector);

            // Every collected copy's score, keyed by global doc id, keeping the best one.
            Map<Integer, Float> bestCopy = new ConcurrentHashMap<>();
            // Post-filter delegate: rewrite returns the merged approximate pool without exact rescoring, which is what we want to inspect.
            var query = new IVFKnnFloatVectorQuery(
                ESNextRescoreOversampleTestFixture.FIELD_NAME,
                queryVector,
                10,
                500,
                null,
                1f,
                IvfQueryConfigResolver.from(true, false, 4, 3f, null),
                true
            ) {
                @Override
                protected IVFCollectorManager getKnnCollectorManager(
                    int k,
                    SharedMinCompetitiveScore sharedScore,
                    QueryTransformCache queryTransformCache
                ) {
                    // No cross-segment score sharing, so the result does not depend on how the leaves interleave.
                    return new IVFCollectorManager(k, null, queryTransformCache) {
                        @Override
                        public AbstractMaxScoreKnnCollector newCollector(
                            int visitedLimit,
                            KnnSearchStrategy searchStrategy,
                            LeafReaderContext context
                        ) {
                            return new MaxScoreTopKnnCollector(k, visitedLimit, searchStrategy) {
                                @Override
                                public TopDocs unsortedTopK() {
                                    TopDocs topDocs = super.unsortedTopK();
                                    for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                                        bestCopy.merge(scoreDoc.doc + context.docBase, scoreDoc.score, Math::max);
                                    }
                                    return topDocs;
                                }
                            };
                        }
                    };
                }
            };

            IndexSearcher searcher = new IndexSearcher(reader);
            ScoreDoc[] pool = searcher.search(query.rewrite(searcher), IvfSegmentConfig.shardMergeBudget(10, 3f)).scoreDocs;
            assertThat("the pool must not be empty", pool.length > 0, org.hamcrest.Matchers.is(true));

            List<String> worseCopyKept = new ArrayList<>();
            for (ScoreDoc scoreDoc : pool) {
                float best = bestCopy.get(scoreDoc.doc);
                if (scoreDoc.score < best) {
                    worseCopyKept.add("doc=" + scoreDoc.doc + " kept=" + scoreDoc.score + " best=" + best);
                }
            }
            assertTrue("pool docs scored by a worse overspill copy: " + worseCopyKept, worseCopyKept.isEmpty());
        }
    }
}
