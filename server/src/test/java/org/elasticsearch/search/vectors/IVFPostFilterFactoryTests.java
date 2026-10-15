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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidIndexFormat;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfQueryConfigResolver;
import org.elasticsearch.index.codec.vectors.diskbbq.QuantEncoding;
import org.elasticsearch.index.codec.vectors.diskbbq.TestIvfQueryConfigResolver;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Contract tests for the {@link PostFilterableKnnQuery} factory methods on the IVF query tree
 * ({@code createPostFilterDelegate}, {@code createRetryQuery}, {@code postFilterExpectedBaseQueryDocMatches}), exercising the
 * {@code cloneWithParams} respawn wiring directly without a diskbbq index so they pin the per-subtype
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

    // 0.7 is the low end of the range post-filtering actually runs in: it only engages once selectivity
    // reaches the configured threshold, so a more selective filter never gets here.
    // zMargin = 2.5 * sqrt(10 * (1-0.7)/0.7) = 5.175
    private static final float SELECTIVITY = 0.7f;
    private static final int EXPECTED_SCALED_K = 22; // clamp(ceil((10 + 5.175)/0.7)=22, ceil(10*1.2)=12, NUM_CANDS_LIMIT) = 22
    private static final int EXPECTED_SCALED_NUM_CANDS = 44; // clamp(ceil(20 * 22/10)=44, 22, NUM_CANDS_LIMIT) = 44

    private IVFKnnFloatVectorQuery floatPlain() {
        return new IVFKnnFloatVectorQuery(FIELD, QUERY.clone(), K, NUM_CANDS, filter(), VISIT_RATIO, RESOLVER);
    }

    private IVFKnnFloatSlicedVectorQuery floatSliced() {
        return new IVFKnnFloatSlicedVectorQuery(FIELD, QUERY.clone(), K, NUM_CANDS, filter(), VISIT_RATIO, RESOLVER, SLICE_FIELD, SLICE_ID);
    }

    private DiversifyingChildrenIVFKnnFloatVectorQuery floatDiversifying() {
        return new DiversifyingChildrenIVFKnnFloatVectorQuery(FIELD, QUERY.clone(), K, NUM_CANDS, filter(), PARENTS, VISIT_RATIO, RESOLVER);
    }

    private DiversifyingChildrenIVFKnnFloatSlicedVectorQuery floatDiversifyingSliced() {
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

    private IVFKnnByteVectorQuery bytePlain() {
        return new IVFKnnByteVectorQuery(FIELD, BYTE_QUERY.clone(), K, NUM_CANDS, filter(), VISIT_RATIO, RESOLVER);
    }

    private IVFKnnByteSlicedVectorQuery byteSliced() {
        return new IVFKnnByteSlicedVectorQuery(
            FIELD,
            BYTE_QUERY.clone(),
            K,
            NUM_CANDS,
            filter(),
            VISIT_RATIO,
            RESOLVER,
            SLICE_FIELD,
            SLICE_ID
        );
    }

    private DiversifyingChildrenIVFKnnByteVectorQuery byteDiversifying() {
        return new DiversifyingChildrenIVFKnnByteVectorQuery(
            FIELD,
            BYTE_QUERY.clone(),
            K,
            NUM_CANDS,
            filter(),
            PARENTS,
            VISIT_RATIO,
            RESOLVER
        );
    }

    private DiversifyingChildrenIVFKnnByteSlicedVectorQuery byteDiversifyingSliced() {
        return new DiversifyingChildrenIVFKnnByteSlicedVectorQuery(
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
        );
    }

    private List<IVFKnnFloatVectorQuery> allFloatSubtypes() {
        return Arrays.asList(floatPlain(), floatSliced(), floatDiversifying(), floatDiversifyingSliced());
    }

    private List<IVFKnnByteVectorQuery> allByteSubtypes() {
        return Arrays.asList(bytePlain(), byteSliced(), byteDiversifying(), byteDiversifyingSliced());
    }

    private static Query filter() {
        return new TermQuery(new Term("tag", "pass"));
    }

    private static Query postFilterDelegateFor(AbstractIVFKnnVectorQuery original, float selectivity) {
        return original.createPostFilterDelegate(selectivity);
    }

    public void testCreatePostFilterDelegateIsFilterlessAndScaled() {
        for (IVFKnnFloatVectorQuery original : allFloatSubtypes()) {
            AbstractIVFKnnVectorQuery delegate = (AbstractIVFKnnVectorQuery) postFilterDelegateFor(original, SELECTIVITY);

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
            AbstractIVFKnnVectorQuery delegate = (AbstractIVFKnnVectorQuery) postFilterDelegateFor(original, SELECTIVITY);

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
     * With no leaf to resolve, {@code postFilterExpectedBaseQueryDocMatches} falls back to what configuration declares - the
     * query-time override if there is one, otherwise the mapping default
     */
    public void testCandidatePoolSizeFallsBackToDeclaredOversample() throws IOException {
        assertEquals("oversample 1.0 -> pool is k", K, floatPlain().postFilterExpectedBaseQueryDocMatches(List.of()));

        IvfQueryConfigResolver oversampling = IvfQueryConfigResolver.from(false, false, 1, 3.0f, null);
        IVFKnnFloatVectorQuery q = new IVFKnnFloatVectorQuery(FIELD, QUERY.clone(), K, NUM_CANDS, filter(), VISIT_RATIO, oversampling);
        assertEquals("oversample 3.0 -> pool is ceil(k*3)", 30, q.postFilterExpectedBaseQueryDocMatches(List.of()));

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
        assertEquals("query-time oversample wins", 20, overridden.postFilterExpectedBaseQueryDocMatches(List.of()));
    }

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
                assertEquals("the segment's own oversample wins", 15, query.postFilterExpectedBaseQueryDocMatches(reader.leaves()));
            }
        }
        assertEquals("with nothing to resolve, configuration is all there is", 30, query.postFilterExpectedBaseQueryDocMatches(List.of()));
    }

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
            AbstractIVFKnnVectorQuery delegate = (AbstractIVFKnnVectorQuery) postFilterDelegateFor(original, 0.9f);
            kPerOversample[i] = delegate.k();
            numCandsPerOversample[i] = delegate.numCands();
        }
        assertEquals("delegate k must not depend on the rescore oversample", kPerOversample[0], kPerOversample[1]);
        assertEquals("delegate numCands must not depend on the rescore oversample", numCandsPerOversample[0], numCandsPerOversample[1]);
        assertEquals(15, kPerOversample[0]);
        assertEquals(30, numCandsPerOversample[0]);
    }

    public void testCreatePostFilterDelegatePreservesSliceRange() {
        for (IVFKnnFloatSlicedVectorQuery original : Arrays.asList(floatSliced(), floatDiversifyingSliced())) {
            IVFKnnFloatSlicedVectorQuery delegate = (IVFKnnFloatSlicedVectorQuery) postFilterDelegateFor(original, SELECTIVITY);
            assertEquals(SLICE_FIELD, delegate.sliceField);
            assertArrayEquals(new BytesRef[] { SLICE_ID }, delegate.sliceIds);
        }
    }

    public void testWithParamsRoundTripsToAnEqualQuery() {
        for (IVFKnnFloatVectorQuery original : allFloatSubtypes()) {
            AbstractIVFKnnVectorQuery respawn = original.clone(original.filter, original.k(), original.numCands(), false);
            assertEquals("respawn with identical params must equal the original", original, respawn);
            assertEquals(original.hashCode(), respawn.hashCode());
        }
        for (IVFKnnByteVectorQuery original : allByteSubtypes()) {
            AbstractIVFKnnVectorQuery respawn = original.clone(original.filter, original.k(), original.numCands(), false);
            assertEquals(original, respawn);
            assertEquals(original.hashCode(), respawn.hashCode());
        }
    }

    public void testEveryConcreteSubtypeIsExercisedThroughCloneWithParams() throws Exception {
        List<AbstractIVFKnnVectorQuery> exercised = new ArrayList<>();
        exercised.addAll(allFloatSubtypes());
        exercised.addAll(allByteSubtypes());
        Set<Class<?>> exercisedTypes = new HashSet<>();
        for (AbstractIVFKnnVectorQuery query : exercised) {
            exercisedTypes.add(query.getClass());
        }

        assertEquals(
            "The set of concrete AbstractIVFKnnVectorQuery subtypes changed. Each subtype must be built here "
                + "(allFloatSubtypes()/allByteSubtypes()) and handled by a switch arm in IVFKnnQueryFactory.cloneWithParams.",
            exercisedTypes,
            concreteIvfQuerySubtypes()
        );

        for (AbstractIVFKnnVectorQuery original : exercised) {
            AbstractIVFKnnVectorQuery respawn = original.clone(
                original.filter,
                original.k(),
                original.numCands(),
                original.postFilterDelegate
            );
            assertSame(
                "cloneWithParams must rebuild " + original.getClass().getSimpleName() + " as its own type",
                original.getClass(),
                respawn.getClass()
            );
        }
    }

    /** The negative of the completeness test above: a subtype with no switch arm must fail loudly via the default arm. */
    public void testCloneWithParamsRejectsAnUnknownSubtype() {
        UnregisteredIVFQuery unknown = new UnregisteredIVFQuery();
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> unknown.clone(unknown.filter, unknown.k(), unknown.numCands(), unknown.postFilterDelegate)
        );
        assertThat(e.getMessage(), containsString(UnregisteredIVFQuery.class.getName()));
    }

    /** The delegate flag changes results, so it must take part in equality. */
    public void testPostFilterDelegateFlagParticipatesInEquality() {
        IVFKnnFloatVectorQuery original = floatPlain();
        AbstractIVFKnnVectorQuery asDelegate = original.clone(original.filter, original.k(), original.numCands(), true);
        assertNotEquals(original, asDelegate);
        assertNotEquals(original.hashCode(), asDelegate.hashCode());
    }

    public void testRespawnCarriesTheSameVectorInstance() {
        for (IVFKnnFloatVectorQuery original : allFloatSubtypes()) {
            IVFKnnFloatVectorQuery delegate = (IVFKnnFloatVectorQuery) postFilterDelegateFor(original, SELECTIVITY);
            assertArrayEquals(QUERY, delegate.getQuery(), 0f);
            assertSame(original.getQuery(), delegate.getQuery());
        }
    }

    public void testGetPostFilterCandidatesBeforeRewriteIsEmpty() {
        ScoreDoc[][] candidates = floatPlain().getPostFilterCandidates();
        assertEquals(0, candidates.length);
    }

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
                    AbstractIVFKnnVectorQuery delegate = (AbstractIVFKnnVectorQuery) postFilterDelegateFor(original, SELECTIVITY);
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
                AbstractIVFKnnVectorQuery delegate = (AbstractIVFKnnVectorQuery) postFilterDelegateFor(original, SELECTIVITY);
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
                AbstractIVFKnnVectorQuery delegate = (AbstractIVFKnnVectorQuery) postFilterDelegateFor(floatPlain(), SELECTIVITY);
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

    private static Set<Class<?>> concreteIvfQuerySubtypes() throws Exception {
        Class<AbstractIVFKnnVectorQuery> base = AbstractIVFKnnVectorQuery.class;
        Path pkgRoot = PathUtils.get(base.getProtectionDomain().getCodeSource().getLocation().toURI())
            .resolve("org")
            .resolve("elasticsearch")
            .resolve("search")
            .resolve("vectors");
        Set<Class<?>> subtypes = new HashSet<>();
        try (Stream<Path> classFiles = Files.list(pkgRoot)) {
            for (Path classFile : (Iterable<Path>) classFiles::iterator) {
                String fileName = classFile.getFileName().toString();
                if (fileName.endsWith(".class") == false) {
                    continue;
                }
                String className = base.getPackageName() + "." + fileName.substring(0, fileName.length() - ".class".length());
                Class<?> clazz = base.getClassLoader().loadClass(className);
                if (base.isAssignableFrom(clazz) && base != clazz && Modifier.isAbstract(clazz.getModifiers()) == false) {
                    subtypes.add(clazz);
                }
            }
        }
        return subtypes;
    }

    /**
     * A concrete {@link AbstractIVFKnnVectorQuery} with no {@link IVFKnnQueryFactory} switch arm - a stand-in for a
     * subtype added without wiring up reconstruction. Search methods are stubbed; these tests only drive cloneWithParams.
     */
    private static class UnregisteredIVFQuery extends AbstractIVFKnnVectorQuery {
        UnregisteredIVFQuery() {
            super(FIELD, VISIT_RATIO, K, NUM_CANDS, filter(), RESOLVER);
        }

        @Override
        public int countTotalVectors(List<LeafReaderContext> leaves) {
            throw new UnsupportedOperationException();
        }

        @Override
        TopDocs getLeafResults(
            LeafReaderContext ctx,
            Weight filterWeight,
            AbstractIVFKnnVectorQuery.IVFCollectorManager knnCollectorManager,
            float visitRatio,
            boolean usePrecondition
        ) {
            return NO_RESULTS;
        }

        @Override
        Query getAutoRescoreQuery(IndexSearcher indexSearcher, Query approxTopN, int finalK, int rescoreK) {
            return null;
        }

        @Override
        public String toString(String field) {
            return "UnregisteredIVFQuery";
        }
    }
}
