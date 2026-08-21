/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.BBQIVFIndexOptions;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.DenseVectorFieldType;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.RescoreVector;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.VectorSimilarity;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;

import static org.hamcrest.Matchers.instanceOf;

/**
 * Pins how {@code DenseVectorFieldType#createKnnQuery} wires {@code k} and {@code numCands} into the query
 * tree, which differs by engine and is easy to get wrong in a way no search-result assertion would catch.
 * <p>
 * HNSW is oversampled <em>by the caller</em>: its query is built with {@code ceil(k * oversample)} and an
 * outer {@link RescoreKnnVectorQuery} reduces that pool to {@code k}. IVF oversamples <em>itself</em>, from
 * the per-segment rescore factor - and does so whether or not {@code auto_calibrate} is on, because
 * {@code IvfQueryConfigResolver#resolve} falls through a non-finite persisted value to the mapping default.
 * So IVF must be built with the user's {@code k}; handing it the oversampled value applies the factor twice -
 * tripling the per-leaf collector and, because IVF derives its visit ratio from {@code numCands/k}, shrinking
 * how much of each segment it scans. Neither effect is visible in a correctness assertion, only in recall and
 * latency, which is why it is pinned here.
 */
public class DenseVectorKnnQueryWiringTests extends ESTestCase {

    private static final int DIMS = 64;
    private static final int K = 10;
    private static final int NUM_CANDS = 100;
    private static final float OVERSAMPLE = 3.0f;

    private static DenseVectorFieldType bbqIvfField(boolean autoCalibrate, float postFilterThreshold) {
        return new DenseVectorFieldType(
            "f",
            IndexVersion.current(),
            ElementType.FLOAT,
            DIMS,
            true,
            VectorSimilarity.L2_NORM,
            new BBQIVFIndexOptions(
                384,
                -1,
                0.0d,
                false,
                new RescoreVector(OVERSAMPLE),
                IndexVersion.current(),
                false,
                1,
                false,
                autoCalibrate,
                BBQIVFIndexOptions.QuantizationType.OSQ
            ),
            Collections.emptyMap(),
            false,
            postFilterThreshold
        );
    }

    private static Query knnQuery(DenseVectorFieldType field, Query filter, Float queryOversample) {
        return field.createKnnQuery(
            VectorData.fromFloats(new float[DIMS]),
            K,
            NUM_CANDS,
            null,
            queryOversample,
            filter,
            null,
            null,
            DenseVectorFieldMapper.FilterHeuristic.ACORN,
            false
        );
    }

    /**
     * Unfiltered default {@code bbq_disk}: an outer rescore over an IVF query that still carries the user's
     * {@code k}. {@code numCands} is widened to at least the rescore pool so the pool is reachable.
     */
    public void testBbqIvfKeepsFinalKUnderMappingOversample() {
        Query query = knnQuery(bbqIvfField(false, 1.0f), null, null);

        assertThat(query, instanceOf(RescoreKnnVectorQuery.class));
        RescoreKnnVectorQuery rescore = (RescoreKnnVectorQuery) query;
        assertEquals("the outer rescore returns the user's k", K, rescore.k());

        assertThat(rescore.innerQuery(), instanceOf(IVFKnnFloatVectorQuery.class));
        IVFKnnFloatVectorQuery ivf = (IVFKnnFloatVectorQuery) rescore.innerQuery();
        assertEquals("IVF must receive the final k, not k*oversample", K, ivf.k());
        assertEquals(Math.max((int) Math.ceil(K * OVERSAMPLE), NUM_CANDS), ivf.numCands());
        assertEquals("the pool IVF expands to is what the rescore consumes", (int) Math.ceil(K * OVERSAMPLE), ivf.candidatePoolSize());
    }

    /** A query-time oversample overrides the mapping's, and still must not reach IVF's {@code k}. */
    public void testBbqIvfKeepsFinalKUnderQueryOversample() {
        Query query = knnQuery(bbqIvfField(false, 1.0f), null, 5.0f);

        RescoreKnnVectorQuery rescore = (RescoreKnnVectorQuery) query;
        IVFKnnFloatVectorQuery ivf = (IVFKnnFloatVectorQuery) rescore.innerQuery();
        assertEquals(K, ivf.k());
        assertEquals(Math.max((int) Math.ceil(K * 5.0f), NUM_CANDS), ivf.numCands());
        assertEquals(50, ivf.candidatePoolSize());
    }

    /**
     * With {@code auto_calibrate} the exact rescore lives inside the IVF query, so there is no outer
     * rescore - and {@code numCands} keeps the oversample-widened value rather than being reset, because
     * that value is what {@code numCands/k} is calibrated against.
     */
    public void testBbqIvfAutoCalibrateHasNoOuterRescore() {
        Query query = knnQuery(bbqIvfField(true, 1.0f), null, null);

        assertThat(query, instanceOf(IVFKnnFloatVectorQuery.class));
        IVFKnnFloatVectorQuery ivf = (IVFKnnFloatVectorQuery) query;
        assertEquals(K, ivf.k());
        assertEquals(Math.max((int) Math.ceil(K * OVERSAMPLE), NUM_CANDS), ivf.numCands());
    }

    /**
     * The post-filter wrapper targets the FINAL {@code k}; the larger pool it retries towards comes from
     * {@link PostFilterableKnnQuery#candidatePoolSize()}. Giving the wrapper the oversampled count instead
     * would make it demand that many filter survivors before accepting the post-filtered result, sending
     * most queries down the fallback path.
     */
    public void testPostFilterWrapperTargetsFinalK() {
        Query query = knnQuery(bbqIvfField(false, 0.5f), new TermQuery(new Term("tag", "a")), null);

        assertThat(query, instanceOf(RescoreKnnVectorQuery.class));
        Query inner = ((RescoreKnnVectorQuery) query).innerQuery();
        assertThat(inner, instanceOf(PostFilterKnnQuery.class));
        PostFilterKnnQuery postFilter = (PostFilterKnnQuery) inner;
        assertEquals("the wrapper targets the final k", K, postFilter.k());
        assertThat(postFilter.innerQuery(), instanceOf(IVFKnnFloatVectorQuery.class));
        assertEquals(K, ((IVFKnnFloatVectorQuery) postFilter.innerQuery()).k());
    }

    /**
     * Nested vector fields stay on the pre-filter path: candidates are child vectors while results are counted
     * per parent, which breaks the round-1 sizing model and makes the filter-versus-collapse order matter. No
     * post-filter wrapper is built for them.
     */
    public void testNestedFieldsAreNotPostFiltered() {
        Query query = bbqIvfField(false, 0.5f).createKnnQuery(
            VectorData.fromFloats(new float[DIMS]),
            K,
            NUM_CANDS,
            null,
            null,
            new TermQuery(new Term("tag", "a")),
            null,
            context -> null,
            DenseVectorFieldMapper.FilterHeuristic.ACORN,
            false
        );

        assertThat(query, instanceOf(RescoreKnnVectorQuery.class));
        Query inner = ((RescoreKnnVectorQuery) query).innerQuery();
        assertThat(inner, instanceOf(DiversifyingChildrenIVFKnnFloatVectorQuery.class));
        assertEquals("the nested query still receives the user's k", K, ((DiversifyingChildrenIVFKnnFloatVectorQuery) inner).k());
    }

    /** A dormant threshold (the default 1.0) must not build a wrapper even with a filter present. */
    public void testDormantThresholdBuildsNoWrapper() {
        Query query = knnQuery(bbqIvfField(false, 1.0f), new TermQuery(new Term("tag", "a")), null);

        Query inner = ((RescoreKnnVectorQuery) query).innerQuery();
        assertThat(inner, instanceOf(IVFKnnFloatVectorQuery.class));
    }
}
