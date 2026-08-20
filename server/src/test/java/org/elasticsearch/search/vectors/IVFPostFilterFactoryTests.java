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
import org.elasticsearch.index.codec.vectors.diskbbq.IvfQueryConfigResolver;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Contract tests for the {@link PostFilterableKnnQuery} factory methods on the IVF query tree
 * ({@code createPostFilterDelegate}, {@code createRetryQuery}, {@code candidatePoolK}), exercising the
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
    // Non-calibrating resolver, mapping oversample 1.0 -> candidatePoolK() == k, so the delegate's k is the
    // binomial target itself. testDelegateUndoesInternalOversampleExpansion covers oversample > 1.
    private static final IvfQueryConfigResolver RESOLVER = IvfQueryConfigResolver.from(false, false, 4, 1.0f, null);

    // Derivation for selectivity=0.5, k=10, numCands=20, oversample=1.0:
    // candidatePoolK = shardMergeBudget(10, 1.0) = 10
    // zMargin = 2.5 * sqrt(10 * (1-0.5)/0.5) = 7.905
    // targetPool = clamp(ceil((10 + 7.905)/0.5)=36, ceil(10*1.2)=12, NUM_CANDS_LIMIT) = 36
    // delegateK = ceil(36 / 1.0) = 36
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

    /**
     * Sizes the delegate the way {@link PostFilterKnnQuery} does for a flat field: the target pool is the
     * query's own {@link AbstractIVFKnnVectorQuery#candidatePoolK()}, with no nested fanout applied.
     */
    private static Query delegateOf(AbstractIVFKnnVectorQuery original, float selectivity) {
        return original.createPostFilterDelegate(selectivity, original.candidatePoolK());
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
     * {@code candidatePoolK} reports the pool the query expands to internally, so the orchestrator can
     * target it instead of guessing from {@code k}.
     */
    public void testCandidatePoolKReflectsDeclaredOversample() {
        assertEquals("oversample 1.0 -> pool is k", K, plain().candidatePoolK());

        IvfQueryConfigResolver oversampling = IvfQueryConfigResolver.from(false, false, 1, 3.0f, null);
        IVFKnnFloatVectorQuery q = new IVFKnnFloatVectorQuery(FIELD, QUERY.clone(), K, NUM_CANDS, filter(), VISIT_RATIO, oversampling);
        assertEquals("oversample 3.0 -> pool is ceil(k*3)", 30, q.candidatePoolK());

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
        assertEquals("query-time oversample wins", 20, overridden.candidatePoolK());
    }

    /**
     * The delegate's ctor {@code k} must undo the oversample expansion {@code rewrite} will re-apply,
     * otherwise the binomial target is multiplied twice and the per-leaf collector blows up.
     * <p>
     * k=10, numCands=20, oversample=3, selectivity=0.9: pool=30, zMargin=2.5*sqrt(30*0.1/0.9)=4.564,
     * targetPool=ceil(34.564/0.9)=39, delegateK=ceil(39/3)=13, numCands=ceil(20*13/10)=26.
     * leafCollectorBudget(13,3)=78 - not the 234 that passing 39 straight through would produce.
     */
    public void testDelegateUndoesInternalOversampleExpansion() {
        IvfQueryConfigResolver oversampling = IvfQueryConfigResolver.from(false, false, 1, 3.0f, null);
        IVFKnnFloatVectorQuery original = new IVFKnnFloatVectorQuery(
            FIELD,
            QUERY.clone(),
            K,
            NUM_CANDS,
            filter(),
            VISIT_RATIO,
            oversampling
        );
        AbstractIVFKnnVectorQuery delegate = (AbstractIVFKnnVectorQuery) delegateOf(original, 0.9f);
        assertEquals(13, delegate.k());
        assertEquals(26, delegate.numCands());
    }

    /**
     * On a nested field the orchestrator inflates the target pool by the children-per-parent fanout, because
     * candidates are children but results are counted per parent. The delegate must honour the number it is
     * given rather than recomputing one from its own k.
     * <p>
     * targetPool=30 (pool 10 x fanout 3), selectivity=0.5, oversample=1.0:
     * zMargin = 2.5*sqrt(30*0.5/0.5) = 13.69 -> ceil(43.69/0.5) = 88, and with oversample 1 the ctor k is 88.
     */
    public void testDelegateHonoursAnInflatedTargetPool() {
        IVFKnnFloatVectorQuery original = plain();
        AbstractIVFKnnVectorQuery delegate = (AbstractIVFKnnVectorQuery) original.createPostFilterDelegate(SELECTIVITY, 30);
        assertEquals(88, delegate.k());
        assertThat(delegate.k(), greaterThan(EXPECTED_SCALED_K));
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

    public void testIvfDoesNotUseRetrySeeds() {
        assertFalse("IVF has no graph to seed", plain().usesRetrySeeds());
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
