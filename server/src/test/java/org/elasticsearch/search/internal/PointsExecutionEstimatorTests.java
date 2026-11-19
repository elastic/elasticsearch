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
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.LongRange;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.RamUsageTester;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class PointsExecutionEstimatorTests extends ESTestCase {

    private static final String POINT_FIELD = "p";
    private static final String RANGE_FIELD = "r";
    private static final long MAX_VALUE = 1000;

    private Directory directory;
    private DirectoryReader reader;

    @Before
    public void indexRandomPoints() throws IOException {
        directory = newDirectory();
        boolean multiValued = randomBoolean();
        try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(null))) {
            int numDocs = between(1000, 20_000);
            for (int docId = 0; docId < numDocs; docId++) {
                Document doc = new Document();
                int values = multiValued ? between(1, 4) : 1;
                for (int v = 0; v < values; v++) {
                    doc.add(new LongPoint(POINT_FIELD, randomLongBetween(0, MAX_VALUE)));
                    long min = randomLongBetween(0, MAX_VALUE);
                    doc.add(new LongRange(RANGE_FIELD, new long[] { min }, new long[] { min + randomLongBetween(0, 50) }));
                }
                writer.addDocument(doc);
            }
            if (randomBoolean()) {
                writer.forceMerge(1);
            }
        }
        reader = DirectoryReader.open(directory);
    }

    @After
    public void closeReader() throws IOException {
        IOUtils.close(reader, directory);
    }

    /**
     * Intersects each leaf's points with a random range through a visitor that counts what the BKD reader asks to {@code grow}
     * by, and checks {@link PointsExecutionEstimator#grownValues} bounds it given the doc count the same visitor estimates.
     */
    public void testGrownValuesBoundsRealIntersection() throws IOException {
        for (int iter = 0; iter < 20; iter++) {
            long lower = randomLongBetween(0, MAX_VALUE);
            CountingRangeVisitor visitor = new CountingRangeVisitor(lower, lower + randomLongBetween(0, MAX_VALUE));
            for (LeafReaderContext ctx : reader.leaves()) {
                PointValues values = ctx.reader().getPointValues(POINT_FIELD);
                long cost = values.estimateDocCount(visitor);
                visitor.grown = 0;
                values.intersect(visitor);
                assertThat(
                    String.format(Locale.ROOT, "[cost=%d, size=%d, docCount=%d]", cost, values.size(), values.getDocCount()),
                    visitor.grown,
                    lessThanOrEqualTo(PointsExecutionEstimator.grownValues(cost, values))
                );
            }
        }
    }

    /** Checks the charge is a ceiling on the doc id set each leaf's scorer keeps, for set queries and range field queries. */
    public void testChargeIsCeilingOnRetainedSet() throws IOException {
        IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setQueryCache(null);
        for (int iter = 0; iter < 20; iter++) {
            long lower = randomLongBetween(0, MAX_VALUE);
            long upper = lower + randomLongBetween(0, MAX_VALUE);
            assertChargeIsCeiling(searcher, LongPoint.newSetQuery(POINT_FIELD, randomSet()), POINT_FIELD);
            assertChargeIsCeiling(
                searcher,
                LongRange.newIntersectsQuery(RANGE_FIELD, new long[] { lower }, new long[] { upper }),
                RANGE_FIELD
            );
            assertChargeIsCeiling(searcher, LongRange.newWithinQuery(RANGE_FIELD, new long[] { lower }, new long[] { upper }), RANGE_FIELD);
        }
    }

    private void assertChargeIsCeiling(IndexSearcher searcher, Query query, String field) throws IOException {
        PointsExecutionEstimator estimator = new PointsExecutionEstimator(field);
        Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);
        for (LeafReaderContext ctx : reader.leaves()) {
            ScorerSupplier supplier = weight.scorerSupplier(ctx);
            if (supplier == null) {
                continue;
            }
            long charged = estimator.bytesForLeaf(ctx, supplier, Long.MAX_VALUE);
            boolean matchesNothing = supplier.cost() == 0;
            long retained = RamUsageTester.ramUsed(supplier.get(Long.MAX_VALUE).iterator());
            // An intersection that matches nothing keeps an empty array, which is not charged.
            if (matchesNothing == false) {
                assertThat(
                    String.format(Locale.ROOT, "%s [maxDoc=%d, charged=%d, retained=%d]", query, ctx.reader().maxDoc(), charged, retained),
                    charged,
                    greaterThanOrEqualTo(retained)
                );
            }
        }
    }

    private static long[] randomSet() {
        long[] values = new long[between(1, 200)];
        for (int i = 0; i < values.length; i++) {
            values[i] = randomLongBetween(-10, MAX_VALUE + 10);
        }
        return values;
    }

    /** Matches points in {@code [lower, upper]} and counts what the BKD reader grows the result by. */
    private static final class CountingRangeVisitor implements PointValues.IntersectVisitor {
        private final byte[] lower = new byte[Long.BYTES];
        private final byte[] upper = new byte[Long.BYTES];
        private long grown;

        CountingRangeVisitor(long lower, long upper) {
            LongPoint.encodeDimension(lower, this.lower, 0);
            LongPoint.encodeDimension(upper, this.upper, 0);
        }

        @Override
        public void grow(int count) {
            grown += count;
        }

        @Override
        public void visit(int docID) {}

        @Override
        public void visit(int docID, byte[] packedValue) {}

        @Override
        public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            if (Arrays.compareUnsigned(minPackedValue, upper) > 0 || Arrays.compareUnsigned(maxPackedValue, lower) < 0) {
                return PointValues.Relation.CELL_OUTSIDE_QUERY;
            }
            if (Arrays.compareUnsigned(minPackedValue, lower) >= 0 && Arrays.compareUnsigned(maxPackedValue, upper) <= 0) {
                return PointValues.Relation.CELL_INSIDE_QUERY;
            }
            return PointValues.Relation.CELL_CROSSES_QUERY;
        }
    }
}
