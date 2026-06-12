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
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;

/**
 * Contract tests for the {@link PostFilterableKnnQuery} factory methods added to the IVF query
 * tree ({@link AbstractIVFKnnVectorQuery#createPostFilterDelegate} and
 * {@link AbstractIVFKnnVectorQuery#createRetryQuery}). These exercise the {@code withParams}
 * respawn wiring directly, without a diskbbq index, so they pin the per-subtype reconstruction
 * (type, filter, scaled k/numCands, slice range) independently of codec behavior. End-to-end
 * search behavior is covered by the diskbbq integration suite.
 */
public class IVFPostFilterFactoryTests extends ESTestCase {

    private static final String FIELD = "vector";
    private static final float[] QUERY = new float[] { 0.1f, 0.2f, 0.3f, 0.4f };
    private static final int K = 10;
    private static final int NUM_CANDS = 20;
    private static final float VISIT_RATIO = 0.5f;
    private static final String SLICE_FIELD = "_routing";
    private static final BytesRef SLICE_ID = new BytesRef("s1");
    // parentsFilter is only stored (never invoked) by these factory paths.
    private static final BitSetProducer PARENTS = context -> null;

    // Derivation for selectivity=0.5, k=10, numCands=20:
    // zMargin = 2.5 * sqrt(10 * (1-0.5)/0.5) = 7.905
    // scaledK = clamp(ceil((10 + 7.905)/0.5)=36, ceil(10*1.2)=12, NUM_CANDS_LIMIT) = 36
    // scaledNumCands = min(NUM_CANDS_LIMIT, ceil(36 * 20/10)=72) = 72
    private static final float SELECTIVITY = 0.5f;
    private static final int EXPECTED_SCALED_K = 36;
    private static final int EXPECTED_SCALED_NUM_CANDS = 72;

    private AbstractIVFKnnVectorQuery plain() {
        return new IVFKnnFloatVectorQuery(FIELD, QUERY.clone(), K, NUM_CANDS, filter(), VISIT_RATIO, false, 1.0f);
    }

    private AbstractIVFKnnVectorQuery sliced() {
        return new IVFKnnFloatSlicedVectorQuery(
            FIELD,
            QUERY.clone(),
            K,
            NUM_CANDS,
            filter(),
            VISIT_RATIO,
            false,
            1.0f,
            SLICE_FIELD,
            SLICE_ID
        );
    }

    private AbstractIVFKnnVectorQuery diversifying() {
        return new DiversifyingChildrenIVFKnnFloatVectorQuery(
            FIELD,
            QUERY.clone(),
            K,
            NUM_CANDS,
            filter(),
            PARENTS,
            VISIT_RATIO,
            false,
            1.0f
        );
    }

    private AbstractIVFKnnVectorQuery diversifyingSliced() {
        return new DiversifyingChildrenIVFKnnFloatSlicedVectorQuery(
            FIELD,
            QUERY.clone(),
            K,
            NUM_CANDS,
            filter(),
            PARENTS,
            VISIT_RATIO,
            false,
            1.0f,
            SLICE_FIELD,
            SLICE_ID
        );
    }

    private static Query filter() {
        return new TermQuery(new Term("tag", "pass"));
    }

    public void testCreatePostFilterDelegateIsFilterlessAndScaled() {
        for (AbstractIVFKnnVectorQuery original : Arrays.asList(plain(), sliced(), diversifying(), diversifyingSliced())) {
            AbstractIVFKnnVectorQuery delegate = (AbstractIVFKnnVectorQuery) original.createPostFilterDelegate(SELECTIVITY);

            assertSame("delegate must be the same concrete type", original.getClass(), delegate.getClass());
            assertNull("post-filter delegate must be filterless", delegate.filter);
            assertEquals(EXPECTED_SCALED_K, delegate.k());
            assertEquals(EXPECTED_SCALED_NUM_CANDS, delegate.numCands());
        }
    }

    public void testCreatePostFilterDelegatePreservesSliceRange() {
        for (AbstractIVFKnnVectorQuery original : Arrays.asList(sliced(), diversifyingSliced())) {
            IVFKnnFloatSlicedVectorQuery delegate = (IVFKnnFloatSlicedVectorQuery) original.createPostFilterDelegate(SELECTIVITY);
            assertEquals(SLICE_FIELD, delegate.sliceField);
            assertEquals(SLICE_ID, delegate.sliceId);
        }
    }

    public void testCreatePostFilterDelegateRespawnsFromOriginalVector() {
        // withParams must reuse the un-preconditioned query vector, not the (possibly transformed) live one.
        for (AbstractIVFKnnVectorQuery original : Arrays.asList(plain(), sliced(), diversifying(), diversifyingSliced())) {
            IVFKnnFloatVectorQuery delegate = (IVFKnnFloatVectorQuery) original.createPostFilterDelegate(SELECTIVITY);
            assertArrayEquals(QUERY, delegate.getQuery(), 0f);
        }
    }

    public void testCreateRetryQueryExcludesDocsAndKeepsDepth() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
                for (int i = 0; i < 8; i++) {
                    w.addDocument(new Document());
                }
            }
            try (IndexReader reader = DirectoryReader.open(dir)) {
                int[] excluded = new int[] { 1, 3, 5 };
                int remainingK = 3;
                for (AbstractIVFKnnVectorQuery original : Arrays.asList(plain(), sliced(), diversifying(), diversifyingSliced())) {
                    AbstractIVFKnnVectorQuery retry = (AbstractIVFKnnVectorQuery) original.createRetryQuery(
                        reader,
                        excluded,
                        new int[0],
                        remainingK
                    );

                    // scaledNumCands = ceil(NUM_CANDS * remainingK / K) = ceil(20 * 3 / 10) = 6
                    int expectedNumCands = (int) Math.ceil((double) NUM_CANDS * remainingK / K);
                    assertSame("retry must be the same concrete type", original.getClass(), retry.getClass());
                    assertEquals("retry asks only for the remaining k", remainingK, retry.k());
                    assertEquals("retry scales numCands proportionally", expectedNumCands, retry.numCands());
                    assertTrue("excluded docs must become an ExcludeDocsQuery", retry.filter instanceof ExcludeDocsQuery);
                }
            }
        }
    }

    public void testCreateRetryQueryWithNoExclusionsIsFilterless() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
                w.addDocument(new Document());
            }
            try (IndexReader reader = DirectoryReader.open(dir)) {
                AbstractIVFKnnVectorQuery retry = (AbstractIVFKnnVectorQuery) plain().createRetryQuery(reader, new int[0], new int[0], 4);
                assertNull("no exclusions -> no filter", retry.filter);
                assertEquals(4, retry.k());
            }
        }
    }
}
