/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidIndexFormat;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfQueryConfigResolver;
import org.elasticsearch.index.codec.vectors.diskbbq.QuantEncoding;
import org.elasticsearch.index.codec.vectors.diskbbq.TestIvfQueryConfigResolver;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Contract tests for the {@link PostFilterableKnnQuery} factory methods on the IVF query tree
 * ({@code createPostFilterDelegate}, {@code createRetryQuery}, {@code postFilterCandidatePoolSize}), exercising the
 * {@code withParams} respawn wiring directly without a diskbbq index so they pin the per-subtype
 * reconstruction (type, filter, scaled k/numCands, slice range, parents filter) independently of codec
 * behavior. End-to-end search behavior is covered by the diskbbq integration suite.
 */
public class IVFPostFilterFactoryTests extends ESTestCase {

    private static final String FIELD = "vector";
    private static final float[] QUERY = new float[] { 0.1f, 0.2f, 0.3f, 0.4f };
    private static final byte[] BYTE_QUERY = new byte[] { 1, 2, 3, 4 };
    private static final int K = 10;
    private static final int NUM_CANDS = 20;
    private static final float VISIT_RATIO = 0.5f;
    private static final String SLICE_FIELD = "_routing";
    private static final BytesRef SLICE_ID = new BytesRef("s1");
    // parentsFilter is only stored (never invoked) by these factory paths.
    private static final BitSetProducer PARENTS = context -> null;
    private static final IvfQueryConfigResolver RESOLVER = IvfQueryConfigResolver.from(false, false, 4, 1.0f, null);

    // Derivation for selectivity=0.5, k=10, numCands=20. The rescore oversample plays no part - the delegate's
    // k is the filter oversample applied to the query's own k, and testDelegateKIgnoresTheRescoreOversample
    // pins exactly that.
    // zMargin = 2.5 * sqrt(10 * (1-0.5)/0.5) = 7.905
    // delegateK = clamp(ceil((10 + 7.905)/0.5)=36, ceil(10*1.2)=12, NUM_CANDS_LIMIT) = 36
    // scaledNumCands = clamp(ceil(20 * 36/10)=72, 36, NUM_CANDS_LIMIT) = 72
    private static final float SELECTIVITY = 0.5f;
    private static final int EXPECTED_SCALED_K = 36;
    private static final int EXPECTED_SCALED_NUM_CANDS = 72;

    private IVFKnnFloatVectorQuery plain() {
        return new IVFKnnFloatVectorQuery(FIELD, QUERY.clone(), K, NUM_CANDS, filter(), VISIT_RATIO, RESOLVER);
    }

    private IVFKnnFloatSlicedVectorQuery sliced() {
        return new IVFKnnFloatSlicedVectorQuery(FIELD, QUERY.clone(), K, NUM_CANDS, filter(), VISIT_RATIO, RESOLVER, SLICE_FIELD, SLICE_ID);
    }

    private DiversifyingChildrenIVFKnnFloatVectorQuery diversifying() {
        return new DiversifyingChildrenIVFKnnFloatVectorQuery(FIELD, QUERY.clone(), K, NUM_CANDS, filter(), PARENTS, VISIT_RATIO, RESOLVER);
    }

    private DiversifyingChildrenIVFKnnFloatSlicedVectorQuery diversifyingSliced() {
        return new DiversifyingChildrenIVFKnnFloatSlicedVectorQuery(
            FIELD,
            QUERY.clone(),
            K,
            NUM_CANDS,
            filter(),
            PARENTS,
            VISIT_RATIO,
            RESOLVER,
            SLICE_FIELD,
            SLICE_ID
        );
    }

    private List<IVFKnnFloatVectorQuery> allFloatSubtypes() {
        return Arrays.asList(plain(), sliced(), diversifying(), diversifyingSliced());
    }

    private List<IVFKnnByteVectorQuery> allByteSubtypes() {
        return Arrays.asList(
            new IVFKnnByteVectorQuery(FIELD, BYTE_QUERY.clone(), K, NUM_CANDS, filter(), VISIT_RATIO, RESOLVER),
            new IVFKnnByteSlicedVectorQuery(
                FIELD,
                BYTE_QUERY.clone(),
                K,
                NUM_CANDS,
                filter(),
                VISIT_RATIO,
                RESOLVER,
                SLICE_FIELD,
                SLICE_ID
            ),
            new DiversifyingChildrenIVFKnnByteVectorQuery(
                FIELD,
                BYTE_QUERY.clone(),
                K,
                NUM_CANDS,
                filter(),
                PARENTS,
                VISIT_RATIO,
                RESOLVER
            ),
            new DiversifyingChildrenIVFKnnByteSlicedVectorQuery(
                FIELD,
                BYTE_QUERY.clone(),
                K,
                NUM_CANDS,
                filter(),
                PARENTS,
                VISIT_RATIO,
                RESOLVER,
                SLICE_FIELD,
                SLICE_ID
            )
        );
    }

    private static Query filter() {
        return new TermQuery(new Term("tag", "pass"));
    }

    private static Query delegateOf(AbstractIVFKnnVectorQuery original, float selectivity) {
        return original.createPostFilterDelegate(selectivity);
    }

    public void testCreatePostFilterDelegateIsFilterlessAndScaled() {
        for (IVFKnnFloatVectorQuery original : allFloatSubtypes()) {
            AbstractIVFKnnVectorQuery delegate = (AbstractIVFKnnVectorQuery) delegateOf(original, SELECTIVITY);

            assertSame("delegate must be the same concrete type", original.getClass(), delegate.getClass());
            assertNull("post-filter delegate must be filterless", delegate.filter);
            assertEquals(EXPECTED_SCALED_K, delegate.k());
            assertEquals(EXPECTED_SCALED_NUM_CANDS, delegate.numCands());
            assertTrue("post-filter delegates skip the in-rewrite auto-calibrate rescore", delegate.postFilterDelegate);
        }
    }

    /** The byte subtree must respawn as byte, with the same sizing as float. */
    public void testCreatePostFilterDelegateForByteSubtypes() {
        for (IVFKnnByteVectorQuery original : allByteSubtypes()) {
            AbstractIVFKnnVectorQuery delegate = (AbstractIVFKnnVectorQuery) delegateOf(original, SELECTIVITY);

            assertSame("delegate must be the same concrete type", original.getClass(), delegate.getClass());
            assertNull(delegate.filter);
            assertEquals(EXPECTED_SCALED_K, delegate.k());
            assertEquals(EXPECTED_SCALED_NUM_CANDS, delegate.numCands());
            assertTrue(delegate.postFilterDelegate);
            assertThat(delegate, instanceOf(IVFKnnByteVectorQuery.class));
            assertArrayEquals(BYTE_QUERY, ((IVFKnnByteVectorQuery) delegate).getQuery());
        }
    }

    /**
     * With no leaf to resolve, {@code postFilterCandidatePoolSize} falls back to what configuration declares - the
     * query-time override if there is one, otherwise the mapping default. Unreachable in production (a query
     * with no segments carrying the field never gets this far), but it is the value the whole precedence chain
     * rests on, so it is pinned here rather than behind an index.
     */
    public void testCandidatePoolSizeFallsBackToDeclaredOversample() throws IOException {
        assertEquals("oversample 1.0 -> pool is k", K, plain().postFilterCandidatePoolSize(List.of()));

        IvfQueryConfigResolver oversampling = IvfQueryConfigResolver.from(false, false, 1, 3.0f, null);
        IVFKnnFloatVectorQuery q = new IVFKnnFloatVectorQuery(FIELD, QUERY.clone(), K, NUM_CANDS, filter(), VISIT_RATIO, oversampling);
        assertEquals("oversample 3.0 -> pool is ceil(k*3)", 30, q.postFilterCandidatePoolSize(List.of()));

        IvfQueryConfigResolver queryOverride = IvfQueryConfigResolver.from(false, false, 1, 3.0f, 2.0f);
        IVFKnnFloatVectorQuery overridden = new IVFKnnFloatVectorQuery(
            FIELD,
            QUERY.clone(),
            K,
            NUM_CANDS,
            filter(),
            VISIT_RATIO,
            queryOverride
        );
        assertEquals("query-time oversample wins", 20, overridden.postFilterCandidatePoolSize(List.of()));
    }

    /**
     * A calibrated segment persists its own rescore oversample, and {@code postFilterCandidatePoolSize} must report
     * <em>that</em> rather than the mapping default: the pool it returns becomes the orchestrator's cut, and
     * the cut is what {@code finalizeTopK} exact-rescores. Sizing it from the declared value makes a filtered
     * query rescore {@code k * declared} deep while the same query without a filter rescores
     * {@code k * calibrated} - and calibration deliberately picks the cheapest depth that meets target recall,
     * so declared is usually the larger of the two.
     * <p>
     * declared 3.0, segment 1.5, k=10: the pool is ceil(10*1.5)=15, not ceil(10*3.0)=30.
     */
    public void testCandidatePoolSizePrefersTheSegmentOversample() throws IOException {
        IvfQueryConfigResolver diverging = new TestIvfQueryConfigResolver(
            CentroidIndexFormat.FLAT,
            QuantEncoding.fromBits((byte) 1),
            false,
            3.0f,
            1.5f,
            true
        );
        IVFKnnFloatVectorQuery query = new IVFKnnFloatVectorQuery(FIELD, QUERY.clone(), K, NUM_CANDS, filter(), VISIT_RATIO, diverging);

        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            Document doc = new Document();
            doc.add(new KnnFloatVectorField(FIELD, QUERY.clone()));
            writer.addDocument(doc);
            writer.commit();
            try (IndexReader reader = DirectoryReader.open(dir)) {
                assertEquals("the segment's own oversample wins", 15, query.postFilterCandidatePoolSize(reader.leaves()));
            }
        }
        assertEquals("with nothing to resolve, configuration is all there is", 30, query.postFilterCandidatePoolSize(List.of()));
    }

    /**
     * The delegate's {@code k} is the filter oversample applied to the query's own {@code k}, and nothing
     * else: the rescore oversample must not enter into it. The query already expands a {@code k} into per-leaf
     * collector budgets and a shard merge by itself ({@code IvfSegmentConfig}), and it does that off whatever
     * {@code k} it is handed - so the post-filter layer neither multiplies by the oversample nor divides it
     * back out. Two resolvers that differ only in oversample must therefore produce identical delegates.
     * <p>
     * k=10, numCands=20, selectivity=0.9: zMargin=2.5*sqrt(10*0.1/0.9)=2.635,
     * delegateK=clamp(ceil(12.635/0.9)=15, 12, LIMIT)=15, numCands=ceil(20*15/10)=30.
     */
    public void testDelegateKIgnoresTheRescoreOversample() {
        int[] kPerOversample = new int[2];
        int[] numCandsPerOversample = new int[2];
        float[] oversamples = { 1.0f, 3.0f };
        for (int i = 0; i < oversamples.length; i++) {
            IvfQueryConfigResolver resolver = IvfQueryConfigResolver.from(false, false, 1, oversamples[i], null);
            IVFKnnFloatVectorQuery original = new IVFKnnFloatVectorQuery(
                FIELD,
                QUERY.clone(),
                K,
                NUM_CANDS,
                filter(),
                VISIT_RATIO,
                resolver
            );
            AbstractIVFKnnVectorQuery delegate = (AbstractIVFKnnVectorQuery) delegateOf(original, 0.9f);
            kPerOversample[i] = delegate.k();
            numCandsPerOversample[i] = delegate.numCands();
        }
        assertEquals("delegate k must not depend on the rescore oversample", kPerOversample[0], kPerOversample[1]);
        assertEquals("delegate numCands must not depend on the rescore oversample", numCandsPerOversample[0], numCandsPerOversample[1]);
        assertEquals(15, kPerOversample[0]);
        assertEquals(30, numCandsPerOversample[0]);
    }

    public void testCreatePostFilterDelegatePreservesSliceRange() {
        for (IVFKnnFloatSlicedVectorQuery original : Arrays.asList(sliced(), diversifyingSliced())) {
            IVFKnnFloatSlicedVectorQuery delegate = (IVFKnnFloatSlicedVectorQuery) delegateOf(original, SELECTIVITY);
            assertEquals(SLICE_FIELD, delegate.sliceField);
            assertArrayEquals(new BytesRef[] { SLICE_ID }, delegate.sliceIds);
        }
    }

    /**
     * A respawn differs from the original only in filter, k, numCands and the delegate flag - every other
     * field {@code equals} compares (vector, visit ratio, resolver, slice ids, parents filter) must survive.
     * Comparing whole queries catches a {@code withParams} that silently drops one of them.
     */
    public void testWithParamsRoundTripsToAnEqualQuery() {
        for (IVFKnnFloatVectorQuery original : allFloatSubtypes()) {
            IVFKnnFloatVectorQuery respawn = original.withParams(original.filter, original.k(), original.numCands(), false);
            assertEquals("respawn with identical params must equal the original", original, respawn);
            assertEquals(original.hashCode(), respawn.hashCode());
        }
        for (IVFKnnByteVectorQuery original : allByteSubtypes()) {
            IVFKnnByteVectorQuery respawn = original.withParams(original.filter, original.k(), original.numCands(), false);
            assertEquals(original, respawn);
            assertEquals(original.hashCode(), respawn.hashCode());
        }
    }

    /** The delegate flag changes results, so it must take part in equality. */
    public void testPostFilterDelegateFlagParticipatesInEquality() {
        IVFKnnFloatVectorQuery original = plain();
        IVFKnnFloatVectorQuery asDelegate = original.withParams(original.filter, original.k(), original.numCands(), true);
        assertNotEquals(original, asDelegate);
        assertNotEquals(original.hashCode(), asDelegate.hashCode());
    }

    /**
     * The query vector is carried by reference: nothing mutates it (preconditioning writes into a fresh
     * array per segment), so cloning on every respawn would be pure copying.
     */
    public void testRespawnCarriesTheSameVectorInstance() {
        for (IVFKnnFloatVectorQuery original : allFloatSubtypes()) {
            IVFKnnFloatVectorQuery delegate = (IVFKnnFloatVectorQuery) delegateOf(original, SELECTIVITY);
            assertArrayEquals(QUERY, delegate.getQuery(), 0f);
            assertSame(original.getQuery(), delegate.getQuery());
        }
    }

    public void testGetPostFilterCandidatesBeforeRewriteIsEmpty() {
        ScoreDoc[][] candidates = plain().getPostFilterCandidates();
        assertEquals(0, candidates.length);
    }

    /**
     * The retry asks for fewer docs, so numCands must shrink with it: for IVF the numCands/k ratio is the
     * codec's visit-ratio signal, and keeping the full numCands would make a 3-doc retry explore harder
     * than round 0 did. remainingK=3, k=10, numCands=20 -> ceil(20*3/10) = 6.
     */
    public void testCreateRetryQueryExcludesDocsAndScalesNumCandsDown() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
                for (int i = 0; i < 8; i++) {
                    w.addDocument(new Document());
                }
            }
            try (IndexReader reader = DirectoryReader.open(dir)) {
                int[] excluded = new int[] { 1, 3, 5 };
                int remainingK = 3;
                for (IVFKnnFloatVectorQuery original : allFloatSubtypes()) {
                    // Retries always come off a delegate, never off the user's query - see createRetryQuery.
                    AbstractIVFKnnVectorQuery delegate = (AbstractIVFKnnVectorQuery) delegateOf(original, SELECTIVITY);
                    AbstractIVFKnnVectorQuery retry = (AbstractIVFKnnVectorQuery) delegate.createRetryQuery(
                        reader,
                        excluded,
                        new int[0][],
                        remainingK
                    );

                    assertSame("retry must be the same concrete type", original.getClass(), retry.getClass());
                    assertEquals("retry asks only for the remaining k", remainingK, retry.k());
                    assertEquals("retry scales numCands down to preserve numCands/k", 6, retry.numCands());
                    assertTrue(retry.postFilterDelegate);
                    assertTrue("excluded docs must become an ExcludeDocsQuery", retry.filter instanceof ExcludeDocsQuery);
                }
            }
        }
    }

    /** numCands must never fall below k, or the constructor's own invariant would reject the retry. */
    public void testRetryNumCandsNeverDropsBelowK() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
                w.addDocument(new Document());
            }
            try (IndexReader reader = DirectoryReader.open(dir)) {
                IVFKnnFloatVectorQuery original = new IVFKnnFloatVectorQuery(FIELD, QUERY.clone(), K, K, filter(), VISIT_RATIO, RESOLVER);
                AbstractIVFKnnVectorQuery delegate = (AbstractIVFKnnVectorQuery) delegateOf(original, SELECTIVITY);
                AbstractIVFKnnVectorQuery retry = (AbstractIVFKnnVectorQuery) delegate.createRetryQuery(
                    reader,
                    new int[0],
                    new int[0][],
                    1
                );
                assertEquals(1, retry.k());
                assertThat(retry.numCands(), greaterThanOrEqualTo(retry.k()));
            }
        }
    }

    public void testCreateRetryQueryWithNoExclusionsIsFilterless() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
                w.addDocument(new Document());
            }
            try (IndexReader reader = DirectoryReader.open(dir)) {
                AbstractIVFKnnVectorQuery delegate = (AbstractIVFKnnVectorQuery) delegateOf(plain(), SELECTIVITY);
                AbstractIVFKnnVectorQuery retry = (AbstractIVFKnnVectorQuery) delegate.createRetryQuery(
                    reader,
                    new int[0],
                    new int[0][],
                    4
                );
                assertNull("no exclusions -> no filter", retry.filter);
                assertEquals(4, retry.k());
            }
        }
    }
}
