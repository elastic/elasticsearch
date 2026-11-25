/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BitSetIterator;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

import static org.elasticsearch.search.internal.BreakerWeightTestUtils.assertChargeIsCeilingOnRetainedSet;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class PointRangeExecutionEstimatorTests extends ESTestCase {

    private static final String FIELD = "p";
    /** Large enough that the leaf's bit set dwarfs the BKD traversal scratch, which is charged on top of it. */
    private static final int NUM_DOCS = 1 << 18;
    /** The number of grown points past which {@code DocIdSetBuilder} upgrades to a bit set. */
    private static final int BUILDER_THRESHOLD = NUM_DOCS >>> 7;

    private Directory directory;

    @Before
    public void initDirectory() {
        directory = newDirectory();
    }

    @After
    public void closeDirectory() throws IOException {
        directory.close();
    }

    /**
     * A range small enough that {@code DocIdSetBuilder} keeps buffering doc ids: with the two leaf blocks crossing its bounds grown in
     * full, it grows well under the upgrade threshold.
     */
    public void testChargeIsCeilingOnSparseBuilder() throws IOException {
        int valuesPerDoc = between(1, 4);
        try (DirectoryReader reader = indexConsecutivePoints(valuesPerDoc)) {
            Query query = rangeQuery(valuesPerDoc, between(1, BUILDER_THRESHOLD / 4));
            assertThat("the builder must stay sparse for " + query, retainedIterator(reader, query), not(instanceOf(BitSetIterator.class)));
            assertChargeIsCeilingOnRetainedSet(searcherWithoutCache(reader), query);
        }
    }

    /**
     * A range matching more points than {@code maxDoc >>> 7} makes {@code DocIdSetBuilder} upgrade to a bit set, which the scorer
     * keeps, even though {@code 4 * cost} bytes of buffered doc ids would still be well below the size of that bit set.
     */
    public void testChargeIsCeilingOnBitSetPastBuilderThreshold() throws IOException {
        int valuesPerDoc = between(1, 4);
        try (DirectoryReader reader = indexConsecutivePoints(valuesPerDoc)) {
            Query query = rangeQuery(valuesPerDoc, between(BUILDER_THRESHOLD + 1, NUM_DOCS / 40));
            assertThat("the builder must upgrade for " + query, retainedIterator(reader, query), instanceOf(BitSetIterator.class));
            assertChargeIsCeilingOnRetainedSet(searcherWithoutCache(reader), query);
        }
    }

    /**
     * On a single-valued leaf a range whose estimated cost is more than half the documents builds the bit set of the ones it does not
     * match. The estimate counts the two leaf blocks crossing the range bounds as half matching, hence the margin over half.
     */
    public void testChargeIsCeilingOnInverseBitSet() throws IOException {
        try (DirectoryReader reader = indexConsecutivePoints(1)) {
            Query query = rangeQuery(1, between(NUM_DOCS / 2 + 1024, NUM_DOCS - 2));
            assertThat(
                "Lucene takes the inverse path above half the documents",
                scorerSupplier(reader, query).cost(),
                greaterThan(NUM_DOCS / 2L)
            );
            assertThat(
                "the inverse path must keep a bit set for " + query,
                retainedIterator(reader, query),
                instanceOf(BitSetIterator.class)
            );
            assertChargeIsCeilingOnRetainedSet(searcherWithoutCache(reader), query);
        }
    }

    /** Any range that matches neither every value nor none of them, on a single-valued or multivalued leaf. */
    public void testChargeIsCeilingOnRetainedSet() throws IOException {
        int valuesPerDoc = between(1, 4);
        try (DirectoryReader reader = indexConsecutivePoints(valuesPerDoc)) {
            assertChargeIsCeilingOnRetainedSet(
                searcherWithoutCache(reader),
                rangeQuery(valuesPerDoc, between(1, NUM_DOCS * valuesPerDoc - 2))
            );
        }
    }

    /**
     * One segment in which every doc has {@code valuesPerDoc} distinct, consecutive values, so range costs are near exact. The codec is
     * pinned because the builder grows by whole BKD leaf blocks: random codecs use blocks of up to 2048 points, and the two blocks
     * crossing a small range would then push the builder past its upgrade threshold.
     */
    private DirectoryReader indexConsecutivePoints(int valuesPerDoc) throws IOException {
        try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(null).setCodec(TestUtil.getDefaultCodec()))) {
            for (int docId = 0; docId < NUM_DOCS; docId++) {
                Document doc = new Document();
                for (int v = 0; v < valuesPerDoc; v++) {
                    doc.add(new IntPoint(FIELD, docId * valuesPerDoc + v));
                }
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
        return DirectoryReader.open(directory);
    }

    /** A range over {@code matchingPoints} consecutive values that leaves at least one value out on each side. */
    private static Query rangeQuery(int valuesPerDoc, int matchingPoints) {
        int lower = between(1, NUM_DOCS * valuesPerDoc - matchingPoints - 1);
        return IntPoint.newRangeQuery(FIELD, lower, lower + matchingPoints - 1);
    }

    /** The iterator the scorer of the only leaf keeps, to check which path Lucene took. */
    private static DocIdSetIterator retainedIterator(DirectoryReader reader, Query query) throws IOException {
        return scorerSupplier(reader, query).get(Long.MAX_VALUE).iterator();
    }

    private static ScorerSupplier scorerSupplier(DirectoryReader reader, Query query) throws IOException {
        IndexSearcher searcher = searcherWithoutCache(reader);
        Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);
        return weight.scorerSupplier(reader.leaves().getFirst());
    }

    private static IndexSearcher searcherWithoutCache(DirectoryReader reader) {
        IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setQueryCache(null);
        return searcher;
    }
}
