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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidIndexFormat;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfMergeConfigResolver;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfQueryConfigResolver;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfSegmentConfig;
import org.elasticsearch.index.codec.vectors.diskbbq.QuantEncoding;
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextRescoreOversampleTestFixture;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Query-time tests for IVF shard merge when leaves disagree on persisted rescore oversample.
 */
public class IVFKnnVectorQueryCalibratedMergeTests extends ESTestCase {

    public void testShardMergeKeepsCandidatesPerMaxPersistedRescoreOversample() throws IOException {
        Random rnd = random();
        float oversampleA = 2f;
        float oversampleB = 5f;
        final int k = 10;
        final int expectedMergeK = (int) Math.ceil(k * oversampleB);

        try (Directory dir = newDirectory()) {
            try (
                DirectoryReader reader = ESNextRescoreOversampleTestFixture.buildTwoCommitsTwoSegments(
                    dir,
                    4,
                    64,
                    oversampleA,
                    oversampleB,
                    IvfMergeConfigResolver.useCodecDefault()
                )
            ) {
                ESNextRescoreOversampleTestFixture.assertLeafOversamples(reader, oversampleA, oversampleB);

                IndexSearcher searcher = newUnwrappedSearcher(reader);

                float[] queryVector = new float[4];
                for (int i = 0; i < queryVector.length; i++) {
                    queryVector[i] = rnd.nextFloat();
                }
                VectorUtil.l2normalize(queryVector);

                var resolver = IvfQueryConfigResolver.from(false, false, 4, oversampleB, null);

                IVFKnnFloatVectorQuery ivfQuery = new IVFKnnFloatVectorQuery(
                    ESNextRescoreOversampleTestFixture.FIELD_NAME,
                    queryVector,
                    k,
                    500,
                    null,
                    1f,
                    resolver
                );

                Query rewritten = ivfQuery.rewrite(searcher);
                TopDocs hits = searcher.search(rewritten, Integer.MAX_VALUE);

                assertThat(hits.scoreDocs.length, lessThanOrEqualTo(Math.min(expectedMergeK, reader.numDocs())));
                assertThat(hits.scoreDocs.length, greaterThan(k));
            }
        }
    }

    public void testAutoCalibrateRewriteUsesInternalRescorePath() throws IOException {
        Random rnd = random();
        float oversampleA = 1.25f;
        float oversampleB = 2.5f;
        final int k = 10;

        try (Directory dir = newDirectory()) {
            try (
                DirectoryReader reader = ESNextRescoreOversampleTestFixture.buildTwoCommitsTwoSegments(
                    dir,
                    4,
                    64,
                    oversampleA,
                    oversampleB,
                    IvfMergeConfigResolver.useCodecDefault()
                )
            ) {
                IndexSearcher searcher = newUnwrappedSearcher(reader);

                float[] queryVector = new float[4];
                for (int i = 0; i < queryVector.length; i++) {
                    queryVector[i] = rnd.nextFloat();
                }
                VectorUtil.l2normalize(queryVector);

                var resolver = IvfQueryConfigResolver.from(true, false, 4, DenseVectorFieldMapper.DEFAULT_OVERSAMPLE, null);
                IVFKnnFloatVectorQuery ivfQuery = new IVFKnnFloatVectorQuery(
                    ESNextRescoreOversampleTestFixture.FIELD_NAME,
                    queryVector,
                    k,
                    500,
                    null,
                    1f,
                    resolver
                );

                Query rewritten = ivfQuery.rewrite(searcher);
                assertThat(rewritten, instanceOf(RescoreKnnVectorQuery.class));

                TopDocs hits = searcher.search(rewritten, k);
                assertThat(hits.scoreDocs.length, equalTo(Math.min(k, reader.numDocs())));
            }
        }
    }

    public void testQueryOversampleOverridesCalibratedRescorePath() throws IOException {
        Random rnd = random();
        float oversampleA = 1.25f;
        float oversampleB = 2.5f;
        float queryOversample = 6f;
        final int k = 10;

        try (Directory dir = newDirectory()) {
            try (
                DirectoryReader reader = ESNextRescoreOversampleTestFixture.buildTwoCommitsTwoSegments(
                    dir,
                    4,
                    64,
                    oversampleA,
                    oversampleB,
                    IvfMergeConfigResolver.useCodecDefault()
                )
            ) {
                IndexSearcher searcher = newUnwrappedSearcher(reader);

                float[] queryVector = new float[4];
                for (int i = 0; i < queryVector.length; i++) {
                    queryVector[i] = rnd.nextFloat();
                }
                VectorUtil.l2normalize(queryVector);

                var resolver = IvfQueryConfigResolver.from(true, false, 4, DenseVectorFieldMapper.DEFAULT_OVERSAMPLE, queryOversample);
                IVFKnnFloatVectorQuery ivfQuery = new IVFKnnFloatVectorQuery(
                    ESNextRescoreOversampleTestFixture.FIELD_NAME,
                    queryVector,
                    k,
                    500,
                    null,
                    1f,
                    resolver
                );

                Query rewritten = ivfQuery.rewrite(searcher);
                assertThat(rewritten, instanceOf(RescoreKnnVectorQuery.class));

                TopDocs hits = searcher.search(rewritten, k);
                assertThat(hits.scoreDocs.length, equalTo(Math.min(k, reader.numDocs())));
            }
        }
    }

    /**
     * When two segments disagree on preconditioning, each must be searched with the correct query: the
     * preconditioned segment with its own transformed copy, the other with the original. Regression for a bug
     * where the first preconditioned segment mutated the shared query field, so every other segment (and the
     * exact-rescore query) then searched with the transformed vector.
     */
    public void testPreconditioningDoesNotLeakAcrossSegments() throws IOException {
        Random rnd = random();
        final int k = 10;
        final int dims = 4;

        try (Directory dir = newDirectory()) {
            try (
                DirectoryReader reader = ESNextRescoreOversampleTestFixture.buildTwoCommitsTwoSegmentsPreconditioning(
                    dir,
                    dims,
                    64,
                    true,
                    false,
                    IvfMergeConfigResolver.useCodecDefault()
                )
            ) {
                assertThat(reader.leaves(), hasSize(2));
                IvfSegmentConfig cfg0 = ESNextRescoreOversampleTestFixture.readPersistedSegmentConfig(reader.leaves().get(0).reader());
                IvfSegmentConfig cfg1 = ESNextRescoreOversampleTestFixture.readPersistedSegmentConfig(reader.leaves().get(1).reader());
                boolean p0 = cfg0 != null && cfg0.usePrecondition();
                boolean p1 = cfg1 != null && cfg1.usePrecondition();
                // the two segments must genuinely disagree, otherwise the regression path is not exercised
                assertNotEquals(p0, p1);

                IndexSearcher searcher = newUnwrappedSearcher(reader);

                float[] queryVector = new float[dims];
                for (int i = 0; i < queryVector.length; i++) {
                    queryVector[i] = rnd.nextFloat();
                }
                VectorUtil.l2normalize(queryVector);
                float[] originalQuery = queryVector.clone();

                // auto-calibrate resolver reads the persisted per-segment precondition flag
                var resolver = IvfQueryConfigResolver.from(true, false, 4, DenseVectorFieldMapper.DEFAULT_OVERSAMPLE, null);
                IVFKnnFloatVectorQuery ivfQuery = new IVFKnnFloatVectorQuery(
                    ESNextRescoreOversampleTestFixture.FIELD_NAME,
                    queryVector,
                    k,
                    500,
                    null,
                    1f,
                    resolver
                );

                Query rewritten = ivfQuery.rewrite(searcher);
                TopDocs hits = searcher.search(rewritten, Integer.MAX_VALUE);

                // the shared query vector must never be mutated by preconditioning
                assertArrayEquals(originalQuery, ivfQuery.getQuery(), 0f);
                assertThat(hits.scoreDocs.length, greaterThan(0));
            }
        }
    }

    public void testIvfRescoreQueryUsesCalibratedMergeCandidates() throws IOException {
        Random rnd = random();
        float oversampleA = 2f;
        float oversampleB = 5f;
        final int k = 10;

        try (Directory dir = newDirectory()) {
            try (
                DirectoryReader reader = ESNextRescoreOversampleTestFixture.buildTwoCommitsTwoSegments(
                    dir,
                    4,
                    64,
                    oversampleA,
                    oversampleB,
                    IvfMergeConfigResolver.useCodecDefault()
                )
            ) {
                IndexSearcher searcher = newUnwrappedSearcher(reader);

                float[] queryVector = new float[4];
                for (int i = 0; i < queryVector.length; i++) {
                    queryVector[i] = rnd.nextFloat();
                }
                VectorUtil.l2normalize(queryVector);

                var resolver = IvfQueryConfigResolver.from(true, false, 4, DenseVectorFieldMapper.DEFAULT_OVERSAMPLE, null);
                IVFKnnFloatVectorQuery ivfQuery = new IVFKnnFloatVectorQuery(
                    ESNextRescoreOversampleTestFixture.FIELD_NAME,
                    queryVector,
                    k,
                    500,
                    null,
                    1f,
                    resolver
                );
                RescoreKnnVectorQuery rescoreQuery = RescoreKnnVectorQuery.fromInnerQuery(
                    ESNextRescoreOversampleTestFixture.FIELD_NAME,
                    queryVector,
                    k,
                    k,
                    ivfQuery
                );
                assertThat(rescoreQuery.k(), equalTo(k));

                Query rewritten = rescoreQuery.rewrite(searcher);
                TopDocs hits = searcher.search(rewritten, k);
                assertThat(hits.scoreDocs.length, lessThanOrEqualTo(k));
            }
        }
    }

    /**
     * Only a leaf at the largest collector budget may publish the shared cutoff. Overspill duplicates can leave a smaller
     * queue with fewer than mergeK unique docs, so its cutoff would let other leaves prune candidates that still belong in
     * the merge window. A calibrated segment shares the shard with an uncalibrated one that falls back to the mapping
     * oversample (10, budget 200), so the merge budget is 100. The calibrated segment is checked below the merge budget
     * (oversample 1.25, budget 25) and above it (oversample 6, budget 120, possibly only 60 unique docs).
     */
    public void testLeafBelowMaxLeafBudgetDoesNotPublishSharedCutoff() throws IOException {
        final int k = 10;
        final float mappingOversample = 10f;
        IvfSegmentConfig uncalibrated = IvfSegmentConfig.fromCodecDefaults(
            CentroidIndexFormat.FLAT,
            new IvfSegmentConfig.OsqConfig(QuantEncoding.ONE_BIT_4BIT_QUERY),
            false
        );
        assertThat(IvfSegmentConfig.shardMergeBudget(k, mappingOversample), equalTo(100));

        CheckedConsumer<Float, IOException> assertOnlyMaxBudgetLeafPublishes = calibratedOversample -> {
            IvfSegmentConfig calibrated = IvfSegmentConfig.of(
                CentroidIndexFormat.FLAT,
                new IvfSegmentConfig.OsqConfig(QuantEncoding.ONE_BIT_4BIT_QUERY),
                false,
                calibratedOversample
            );
            try (Directory dir = newDirectory()) {
                try (
                    DirectoryReader reader = ESNextRescoreOversampleTestFixture.buildTwoCommitsTwoSegments(
                        dir,
                        4,
                        64,
                        calibrated,
                        uncalibrated,
                        IvfMergeConfigResolver.useCodecDefault()
                    )
                ) {
                    assertThat(reader.leaves(), hasSize(2));
                    float[] queryVector = new float[4];
                    for (int i = 0; i < queryVector.length; i++) {
                        queryVector[i] = randomFloat();
                    }
                    VectorUtil.l2normalize(queryVector);

                    var resolver = IvfQueryConfigResolver.from(true, false, 4, mappingOversample, null);
                    List<CollectorManagerArgs> captured = Collections.synchronizedList(new ArrayList<>());
                    new CollectorManagerCapturingQuery(queryVector, k, resolver, captured).rewrite(newUnwrappedSearcher(reader));

                    assertThat(
                        Set.copyOf(captured.stream().map(args -> Map.entry(args.k(), args.sharedScore().canPublish())).toList()),
                        equalTo(
                            Set.of(
                                Map.entry(IvfSegmentConfig.leafCollectorBudget(k, calibratedOversample), false),
                                Map.entry(IvfSegmentConfig.leafCollectorBudget(k, mappingOversample), true)
                            )
                        )
                    );
                }
            }
        };

        assertOnlyMaxBudgetLeafPublishes.accept(1.25f);
        assertOnlyMaxBudgetLeafPublishes.accept(6f);
    }

    /**
     * {@link IvfQueryConfigResolver} reads persisted calibration from the raw {@code SegmentReader}, and the random reader wrapping
     * done by {@code newSearcher(reader)} (e.g. {@code ParallelCompositeReader}) hides it, so wrapping is disabled here.
     */
    private static IndexSearcher newUnwrappedSearcher(DirectoryReader reader) {
        return newSearcher(reader, false);
    }

    private record CollectorManagerArgs(int k, SharedMinCompetitiveScore sharedScore) {}

    /** Records the arguments {@code rewrite()} passes to each leaf's collector manager. */
    private static class CollectorManagerCapturingQuery extends IVFKnnFloatVectorQuery {
        private final List<CollectorManagerArgs> captured;

        CollectorManagerCapturingQuery(float[] query, int k, IvfQueryConfigResolver resolver, List<CollectorManagerArgs> captured) {
            super(ESNextRescoreOversampleTestFixture.FIELD_NAME, query, k, 500, null, 1f, resolver);
            this.captured = captured;
        }

        @Override
        protected IVFCollectorManager getKnnCollectorManager(int k, SharedMinCompetitiveScore sharedScore) {
            captured.add(new CollectorManagerArgs(k, sharedScore));
            return super.getKnnCollectorManager(k, sharedScore);
        }
    }
}
