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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.instanceOf;

/**
 * Pins how {@code DenseVectorFieldType#createKnnQuery} wires {@code k} and {@code numCands} into the query
 * tree, which differs by engine and is easy to get wrong in a way no search-result assertion would catch.
 * <p>
 * Every case is asserted for both {@code float} and {@code byte}, because the two are built by separate
 * hand-maintained copies of the same logic - {@code createKnnFloatQuery} and {@code createKnnByteQuery}.
 * They agree today, and nothing other than these tests keeps them in step.
 */
public class DenseVectorKnnQueryWiringTests extends ESTestCase {

    private static final int DIMS = 64;
    private static final int K = 10;
    private static final int NUM_CANDS = 100;
    private static final float OVERSAMPLE = 3.0f;

    // The threshold post-filtering is gated on: it engages only once the estimated filter selectivity reaches
    // this, so 0.7 is the shipped operating point and 1.0 leaves post-filtering off entirely.
    private static final float POST_FILTER_ENABLED = 0.7f;
    private static final float POST_FILTER_OFF = 1.0f;

    /**
     * The element-type-dependent pieces of the wiring: the query vector to hand {@code createKnnQuery}, and
     * the IVF query classes it is expected to build. Everything else these tests assert - {@code k},
     * {@code numCands}, the rescore pool - is element-type agnostic, because {@code IvfQueryConfigResolver}
     * and {@code IvfSegmentConfig} never see the element type. The vector is a supplier so each query gets
     * its own array rather than sharing one across tests.
     */
    private record ElementSpec(
        ElementType elementType,
        Supplier<VectorData> queryVector,
        Class<? extends AbstractIVFKnnVectorQuery> ivfClass,
        Class<? extends AbstractIVFKnnVectorQuery> diversifyingIvfClass
    ) {}

    private static final ElementSpec FLOAT_SPEC = new ElementSpec(
        ElementType.FLOAT,
        () -> VectorData.fromFloats(new float[DIMS]),
        IVFKnnFloatVectorQuery.class,
        DiversifyingChildrenIVFKnnFloatVectorQuery.class
    );

    // byte + bbq_disk is a snapshot-only mapping: VectorIndexType.BBQ_DISK#supportsElementType admits BYTE
    // only when Build.current().isSnapshot(). These tests construct the field type directly and so never reach
    // that validation - the wiring below is exercised on every build, and needs no assumeTrue.
    private static final ElementSpec BYTE_SPEC = new ElementSpec(
        ElementType.BYTE,
        () -> VectorData.fromBytes(new byte[DIMS]),
        IVFKnnByteVectorQuery.class,
        DiversifyingChildrenIVFKnnByteVectorQuery.class
    );

    private static DenseVectorFieldType bbqIvfField(ElementSpec spec, boolean autoCalibrate, float postFilterThreshold) {
        return new DenseVectorFieldType(
            "f",
            IndexVersion.current(),
            spec.elementType(),
            DIMS,
            true,
            // L2_NORM keeps the all-zero query vector legal: COSINE and DOT_PRODUCT reject a zero magnitude.
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

    private static Query knnQuery(ElementSpec spec, DenseVectorFieldType field, Query filter, Float queryOversample) {
        return field.createKnnQuery(
            spec.queryVector().get(),
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
    private void assertBbqIvfKeepsFinalKUnderMappingOversample(ElementSpec spec) throws IOException {
        Query query = knnQuery(spec, bbqIvfField(spec, false, POST_FILTER_OFF), null, null);

        assertThat(query, instanceOf(RescoreKnnVectorQuery.class));
        RescoreKnnVectorQuery rescore = (RescoreKnnVectorQuery) query;
        assertEquals("the outer rescore returns the user's k", K, rescore.k());

        assertThat(rescore.innerQuery(), instanceOf(spec.ivfClass()));
        AbstractIVFKnnVectorQuery ivf = (AbstractIVFKnnVectorQuery) rescore.innerQuery();
        assertEquals("IVF must receive the final k, not k*oversample", K, ivf.k());
        assertEquals(Math.max((int) Math.ceil(K * OVERSAMPLE), NUM_CANDS), ivf.numCands());
        assertEquals(
            "the pool IVF expands to is what the rescore consumes",
            (int) Math.ceil(K * OVERSAMPLE),
            ivf.postFilterExpectedBaseQueryDocMatches(List.of())
        );
    }

    public void testBbqIvfKeepsFinalKUnderMappingOversample() throws IOException {
        assertBbqIvfKeepsFinalKUnderMappingOversample(FLOAT_SPEC);
    }

    public void testByteBbqIvfKeepsFinalKUnderMappingOversample() throws IOException {
        assertBbqIvfKeepsFinalKUnderMappingOversample(BYTE_SPEC);
    }

    /** A query-time oversample overrides the mapping's, and still must not reach IVF's {@code k}. */
    private void assertBbqIvfKeepsFinalKUnderQueryOversample(ElementSpec spec) throws IOException {
        Query query = knnQuery(spec, bbqIvfField(spec, false, POST_FILTER_OFF), null, 5.0f);

        RescoreKnnVectorQuery rescore = (RescoreKnnVectorQuery) query;
        AbstractIVFKnnVectorQuery ivf = (AbstractIVFKnnVectorQuery) rescore.innerQuery();
        assertThat(ivf, instanceOf(spec.ivfClass()));
        assertEquals(K, ivf.k());
        assertEquals(Math.max((int) Math.ceil(K * 5.0f), NUM_CANDS), ivf.numCands());
        assertEquals(50, ivf.postFilterExpectedBaseQueryDocMatches(List.of()));
    }

    public void testBbqIvfKeepsFinalKUnderQueryOversample() throws IOException {
        assertBbqIvfKeepsFinalKUnderQueryOversample(FLOAT_SPEC);
    }

    public void testByteBbqIvfKeepsFinalKUnderQueryOversample() throws IOException {
        assertBbqIvfKeepsFinalKUnderQueryOversample(BYTE_SPEC);
    }

    /**
     * With {@code auto_calibrate} the exact rescore lives inside the IVF query, so there is no outer
     * rescore - and {@code numCands} keeps the oversample-widened value rather than being reset, because
     * that value is what {@code numCands/k} is calibrated against.
     */
    private void assertBbqIvfAutoCalibrateHasNoOuterRescore(ElementSpec spec) {
        Query query = knnQuery(spec, bbqIvfField(spec, true, POST_FILTER_OFF), null, null);

        assertThat(query, instanceOf(spec.ivfClass()));
        AbstractIVFKnnVectorQuery ivf = (AbstractIVFKnnVectorQuery) query;
        assertEquals(K, ivf.k());
        assertEquals(Math.max((int) Math.ceil(K * OVERSAMPLE), NUM_CANDS), ivf.numCands());
    }

    public void testBbqIvfAutoCalibrateHasNoOuterRescore() {
        assertBbqIvfAutoCalibrateHasNoOuterRescore(FLOAT_SPEC);
    }

    public void testByteBbqIvfAutoCalibrateHasNoOuterRescore() {
        assertBbqIvfAutoCalibrateHasNoOuterRescore(BYTE_SPEC);
    }

    /**
     * The post-filter wrapper targets the FINAL {@code k}; the larger pool it retries towards comes from
     * {@link PostFilterableKnnQuery#postFilterExpectedBaseQueryDocMatches(List)}. Giving the wrapper the oversampled count instead
     * would make it demand that many filter survivors before accepting the post-filtered result, sending
     * most queries down the fallback path.
     */
    private void assertPostFilterWrapperTargetsFinalK(ElementSpec spec) {
        Query query = knnQuery(spec, bbqIvfField(spec, false, POST_FILTER_ENABLED), new TermQuery(new Term("tag", "a")), null);

        assertThat(query, instanceOf(RescoreKnnVectorQuery.class));
        Query inner = ((RescoreKnnVectorQuery) query).innerQuery();
        assertThat(inner, instanceOf(PostFilterKnnQuery.class));
        PostFilterKnnQuery postFilter = (PostFilterKnnQuery) inner;
        assertEquals("the wrapper targets the final k", K, postFilter.k());
        assertThat(postFilter.innerQuery(), instanceOf(spec.ivfClass()));
        assertEquals(K, ((AbstractIVFKnnVectorQuery) postFilter.innerQuery()).k());
    }

    public void testPostFilterWrapperTargetsFinalK() {
        assertPostFilterWrapperTargetsFinalK(FLOAT_SPEC);
    }

    public void testBytePostFilterWrapperTargetsFinalK() {
        assertPostFilterWrapperTargetsFinalK(BYTE_SPEC);
    }

    /**
     * Nested vector fields stay on the pre-filter path: candidates are child vectors while results are counted
     * per parent, which breaks the round-1 sizing model and makes the filter-versus-collapse order matter. No
     * post-filter wrapper is built for them.
     */
    private void assertNestedFieldsAreNotPostFiltered(ElementSpec spec) {
        Query query = bbqIvfField(spec, false, POST_FILTER_ENABLED).createKnnQuery(
            spec.queryVector().get(),
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
        assertThat(inner, instanceOf(spec.diversifyingIvfClass()));
        assertEquals("the nested query still receives the user's k", K, ((AbstractIVFKnnVectorQuery) inner).k());
    }

    public void testNestedFieldsAreNotPostFiltered() {
        assertNestedFieldsAreNotPostFiltered(FLOAT_SPEC);
    }

    public void testByteNestedFieldsAreNotPostFiltered() {
        assertNestedFieldsAreNotPostFiltered(BYTE_SPEC);
    }

    /** A dormant threshold (the default 1.0) must not build a wrapper even with a filter present. */
    private void assertDormantThresholdBuildsNoWrapper(ElementSpec spec) {
        Query query = knnQuery(spec, bbqIvfField(spec, false, POST_FILTER_OFF), new TermQuery(new Term("tag", "a")), null);

        Query inner = ((RescoreKnnVectorQuery) query).innerQuery();
        assertThat(inner, instanceOf(spec.ivfClass()));
    }

    public void testDormantThresholdBuildsNoWrapper() {
        assertDormantThresholdBuildsNoWrapper(FLOAT_SPEC);
    }

    public void testByteDormantThresholdBuildsNoWrapper() {
        assertDormantThresholdBuildsNoWrapper(BYTE_SPEC);
    }
}
