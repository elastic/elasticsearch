/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.filter;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class MergedPointRangeQueryTests extends ESTestCase {
    public void testDifferentField() {
        assertThat(merge(LongPoint.newExactQuery("a", 0), LongPoint.newExactQuery("b", 0)), nullValue());
    }

    public void testDifferentDimensionCount() {
        assertThat(
            merge(LongPoint.newExactQuery("a", 0), LongPoint.newRangeQuery("a", new long[] { 1, 2 }, new long[] { 1, 2 })),
            nullValue()
        );
    }

    public void testDifferentDimensionSize() {
        assertThat(merge(LongPoint.newExactQuery("a", 0), IntPoint.newExactQuery("a", 0)), nullValue());
    }

    public void testSame() {
        Query lhs = LongPoint.newRangeQuery("a", 0, 100);
        assertThat(merge(lhs, LongPoint.newRangeQuery("a", 0, 100)), equalTo(lhs));
    }

    public void testOverlap() throws IOException {
        MergedPointRangeQuery overlapping = mergeToMergedQuery(
            LongPoint.newRangeQuery("a", -100, 100),
            LongPoint.newRangeQuery("a", 0, 100)
        );
        assertDelegateForSingleValuedSegmentsEqualPointRange(overlapping, LongPoint.newRangeQuery("a", 0, 100));
        assertFalse(matches1d(overlapping, -50));       // Point not in range
        assertTrue(matches1d(overlapping, 50));         // Point in range
        assertTrue(matches1d(overlapping, -50, 10));    // Both points in range matches the doc
        assertTrue(matches1d(overlapping, -200, 50));   // One point in range matches
        assertFalse(matches1d(overlapping, -50, 200));  // No points in range doesn't match
    }

    public void testNonOverlap() throws IOException {
        MergedPointRangeQuery disjoint = mergeToMergedQuery(LongPoint.newRangeQuery("a", -100, -10), LongPoint.newRangeQuery("a", 10, 100));
        assertThat(disjoint.delegateForSingleValuedSegments(), instanceOf(MatchNoDocsQuery.class));
        assertFalse(matches1d(disjoint, randomLong()));   // No single point can match
        assertFalse(matches1d(disjoint, -50, -20));       // Both points in lower
        assertFalse(matches1d(disjoint, 20, 50));         // Both points in upper
        assertTrue(matches1d(disjoint, -50, 50));         // One in lower, one in upper
        assertFalse(matches1d(disjoint, -50, 200));       // No point in lower
        assertFalse(matches1d(disjoint, -200, 50));       // No point in upper
    }

    public void test2dSimpleOverlap() throws IOException {
        MergedPointRangeQuery overlapping = mergeToMergedQuery(
            LongPoint.newRangeQuery("a", new long[] { -100, -100 }, new long[] { 100, 100 }),
            LongPoint.newRangeQuery("a", new long[] { 0, 0 }, new long[] { 100, 100 })
        );
        assertDelegateForSingleValuedSegmentsEqualPointRange(
            overlapping,
            LongPoint.newRangeQuery("a", new long[] { 0, 0 }, new long[] { 100, 100 })
        );
        assertFalse(matches2d(overlapping, -50, -50));
        assertTrue(matches2d(overlapping, 10, 10));
        assertTrue(matches2d(overlapping, -50, -50, 10, 10));
    }

    public void test2dComplexOverlap() throws IOException {
        MergedPointRangeQuery overlapping = mergeToMergedQuery(
            LongPoint.newRangeQuery("a", new long[] { -100, 0 }, new long[] { 100, 100 }),
            LongPoint.newRangeQuery("a", new long[] { 0, -100 }, new long[] { 100, 100 })
        );
        assertDelegateForSingleValuedSegmentsEqualPointRange(
            overlapping,
            LongPoint.newRangeQuery("a", new long[] { 0, 0 }, new long[] { 100, 100 })
        );
        assertFalse(matches2d(overlapping, -50, -50));
        assertTrue(matches2d(overlapping, 10, 10));
        assertTrue(matches2d(overlapping, -50, -50, 10, 10));
    }

    public void test2dNoOverlap() throws IOException {
        MergedPointRangeQuery disjoint = mergeToMergedQuery(
            LongPoint.newRangeQuery("a", new long[] { -100, -100 }, new long[] { -10, -10 }),
            LongPoint.newRangeQuery("a", new long[] { 10, 10 }, new long[] { 100, 100 })
        );
        assertThat(disjoint.delegateForSingleValuedSegments(), instanceOf(MatchNoDocsQuery.class));
        assertFalse(matches2d(disjoint, randomLong(), randomLong()));
        assertFalse(matches2d(disjoint, -50, -50));
        assertFalse(matches2d(disjoint, 50, 50));
        assertTrue(matches2d(disjoint, -50, -50, 50, 50));
    }

    public void test2dNoOverlapInOneDimension() throws IOException {
        MergedPointRangeQuery disjoint = mergeToMergedQuery(
            LongPoint.newRangeQuery("a", new long[] { -100, -100 }, new long[] { 100, -10 }),
            LongPoint.newRangeQuery("a", new long[] { 0, 10 }, new long[] { 100, 100 })
        );
        assertThat(disjoint.delegateForSingleValuedSegments(), instanceOf(MatchNoDocsQuery.class));
        assertFalse(matches2d(disjoint, randomLong(), randomLong()));
        assertFalse(matches2d(disjoint, -50, -50));
        assertFalse(matches2d(disjoint, 50, 50));
        assertTrue(matches2d(disjoint, 50, -50, 50, 50));
    }

    public void testEqualsAndHashCode() {
        String field = randomAlphaOfLength(5);
        int dims = randomBoolean() ? 1 : between(2, 16);
        Supplier<Query> supplier = randomFrom(
            List.of(
                () -> randomIntPointRangequery(field, dims),
                () -> randomLongPointRangequery(field, dims),
                () -> randomDoublePointRangequery(field, dims)
            )
        );
        Query lhs = supplier.get();
        Query rhs = randomValueOtherThan(lhs, supplier);
        MergedPointRangeQuery query = mergeToMergedQuery(lhs, rhs);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            query,
            ignored -> mergeToMergedQuery(lhs, rhs),
            ignored -> mergeToMergedQuery(lhs, randomValueOtherThan(lhs, () -> randomValueOtherThan(rhs, supplier)))
        );
    }

    private Query randomIntPointRangequery(String field, int dims) {
        int[] lower = new int[dims];
        int[] upper = new int[dims];
        for (int i = 0; i < dims; i++) {
            lower[i] = randomIntBetween(Integer.MIN_VALUE, Integer.MAX_VALUE - 1);
            upper[i] = randomIntBetween(lower[i], Integer.MAX_VALUE);
        }
        return IntPoint.newRangeQuery(field, lower, upper);
    }

    private Query randomLongPointRangequery(String field, int dims) {
        long[] lower = new long[dims];
        long[] upper = new long[dims];
        for (int i = 0; i < dims; i++) {
            lower[i] = randomLongBetween(Long.MIN_VALUE, Long.MAX_VALUE - 1);
            upper[i] = randomLongBetween(lower[i], Long.MAX_VALUE);
        }
        return LongPoint.newRangeQuery(field, lower, upper);
    }

    private Query randomDoublePointRangequery(String field, int dims) {
        double[] lower = new double[dims];
        double[] upper = new double[dims];
        for (int i = 0; i < dims; i++) {
            lower[i] = randomDoubleBetween(Double.MIN_VALUE, 0, true);
            upper[i] = randomDoubleBetween(lower[i], Double.MAX_VALUE, true);
        }
        return DoublePoint.newRangeQuery(field, lower, upper);
    }

    private Query merge(Query lhs, Query rhs) {
        assertThat("error in test assumptions", lhs, instanceOf(PointRangeQuery.class));
        assertThat("error in test assumptions", rhs, instanceOf(PointRangeQuery.class));
        return MergedPointRangeQuery.merge((PointRangeQuery) lhs, (PointRangeQuery) rhs);
    }

    private MergedPointRangeQuery mergeToMergedQuery(Query lhs, Query rhs) {
        Query merged = merge(lhs, rhs);
        assertThat(merged, instanceOf(MergedPointRangeQuery.class));
        return (MergedPointRangeQuery) merged;
    }

    private boolean matches1d(Query query, long... values) throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
            List<IndexableField> doc = new ArrayList<>();
            for (long v : values) {
                doc.add(new LongPoint("a", v));
            }
            iw.addDocument(doc);
            try (IndexReader r = iw.getReader()) {
                IndexSearcher searcher = new IndexSearcher(r);
                return searcher.count(query) > 0;
            }
        }
    }

    private boolean matches2d(Query query, long... values) throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
            List<IndexableField> doc = new ArrayList<>();
            assertEquals(values.length % 2, 0);
            for (int i = 0; i < values.length; i += 2) {
                doc.add(new LongPoint("a", values[i], values[i + 1]));
            }
            iw.addDocument(doc);
            try (IndexReader r = iw.getReader()) {
                IndexSearcher searcher = new IndexSearcher(r);
                return searcher.count(query) > 0;
            }
        }
    }

    private void assertDelegateForSingleValuedSegmentsEqualPointRange(MergedPointRangeQuery actual, Query expected) {
        /*
         * This is a lot like asserThat(actual.delegateForSingleValuedSegments(), equalTo(expected)); but
         * that doesn't work because the subclasses aren't the same.
         */
        assertThat(expected, instanceOf(PointRangeQuery.class));
        assertThat(actual.delegateForSingleValuedSegments(), instanceOf(PointRangeQuery.class));
        assertThat(
            ((PointRangeQuery) actual.delegateForSingleValuedSegments()).getLowerPoint(),
            equalTo(((PointRangeQuery) expected).getLowerPoint())
        );
        assertThat(
            ((PointRangeQuery) actual.delegateForSingleValuedSegments()).getUpperPoint(),
            equalTo(((PointRangeQuery) expected).getUpperPoint())
        );
    }
}
