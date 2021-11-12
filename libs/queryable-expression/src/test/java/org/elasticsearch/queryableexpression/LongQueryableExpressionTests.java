/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.queryableexpression;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class LongQueryableExpressionTests extends ESTestCase {
    public void testAddToLong() throws IOException {
        withIndexedLong((indexed, searcher, foo) -> {
            long added = randomInterestingLong();
            long result = indexed + added;
            logger.info("{} + {} = {}", indexed, added, result);

            QueryableExpression expression = foo.add(QueryableExpression.constant(added));
            assertThat(expression.toString(), equalTo(added == 0 ? "foo" : "foo + " + added));
            checkApproximations(searcher, expression, result);
            checkPerfectApproximation(searcher, expression, result);
        });
    }

    private void checkPerfectApproximation(IndexSearcher searcher, QueryableExpression expression, long result) throws IOException {
        assertCount(searcher, expression.approximateTermQuery(randomValueOtherThan(result, this::randomInterestingLong)), 0);
        Tuple<Long, Long> bounds = randomValueOtherThanMany(p -> p.v1() <= result && result <= p.v2(), this::randomBounds);
        assertCount(searcher, expression.approximateRangeQuery(bounds.v1(), bounds.v2()), 0);
    }

    private Tuple<Long, Long> randomBounds() {
        long min = randomInterestingLong();
        long max = randomValueOtherThan(min, this::randomInterestingLong);
        return Tuple.tuple(Math.min(min, max), Math.max(min, max));
    }

    public void testMultiplyWithLong() throws IOException {
        withIndexedLong((indexed, searcher, foo) -> {
            long multiplied = randomInterestingLong();
            long result = indexed * multiplied;
            logger.info("{} * {} = {}", indexed, multiplied, result);

            QueryableExpression expression = foo.multiply(QueryableExpression.constant(multiplied));
            checkApproximations(searcher, expression, result);
            if (multiplied == 0) {
                checkPerfectApproximation(searcher, expression, result);
                assertThat(expression.toString(), equalTo("0"));
            } else if (multiplied == 1) {
                assertThat(expression.toString(), equalTo("foo"));
                checkPerfectApproximation(searcher, expression, result);
            } else if (multiplied == -1) {
                assertThat(expression.toString(), equalTo("-(foo)"));
                checkPerfectApproximation(searcher, expression, result);
            } else {
                assertThat(expression.toString(), equalTo("foo * " + multiplied));
            }
        });
    }

    public void testDivideByLong() throws IOException {
        withIndexedLong((indexed, searcher, foo) -> {
            long divisor = randomValueOtherThan(0L, this::randomInterestingLong);
            long result = indexed / divisor;
            logger.info("{} / {} = {}", indexed, divisor, result);

            QueryableExpression expression = foo.divide(QueryableExpression.constant(divisor));
            checkApproximations(searcher, expression, result);
            if (divisor == 0) {
                checkPerfectApproximation(searcher, expression, result);
                assertThat(expression.toString(), equalTo("0"));
            } else if (divisor == 1) {
                assertThat(expression.toString(), equalTo("foo"));
                checkPerfectApproximation(searcher, expression, result);
            } else if (divisor == -1) {
                assertThat(expression.toString(), equalTo("-(foo)"));
                checkPerfectApproximation(searcher, expression, result);
            } else {
                assertThat(expression.toString(), equalTo("foo / " + divisor));
            }
        });
    }

    private long randomInterestingLong() {
        switch (between(0, 4)) {
            case 0:
                return 0;
            case 1:
                return randomBoolean() ? 1 : -1;
            case 2:
                return randomInt();
            case 3:
                return randomLong();
            case 4:
                return randomBoolean() ? Long.MIN_VALUE : Long.MAX_VALUE;
            default:
                throw new IllegalArgumentException("Unsupported case");
        }
    }

    @FunctionalInterface
    interface WithIndexedLong {
        void accept(long indexed, IndexSearcher searcher, QueryableExpression foo) throws IOException;
    }

    private void withIndexedLong(WithIndexedLong callback) throws IOException {
        QueryableExpression foo = new LongQueryableExpression.Field() {
            @Override
            public Query approximateTermQuery(long term) {
                return LongPoint.newExactQuery("foo", term);
            }

            @Override
            public Query approximateRangeQuery(long lower, long upper) {
                return LongPoint.newRangeQuery("foo", lower, upper);
            }

            @Override
            public String toString() {
                return "foo";
            }
        };
        long indexed = randomLong();
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", indexed), new LongPoint("foo", indexed)));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                callback.accept(indexed, searcher, foo);
            }
        }
    }

    private void checkApproximations(IndexSearcher searcher, QueryableExpression expression, long result) throws IOException {
        assertCount(searcher, expression.approximateTermQuery(result), 1);
        assertCount(searcher, expression.approximateRangeQuery(result - 1, result + 1), 1);
        assertCount(searcher, expression.approximateRangeQuery(Long.MIN_VALUE, Long.MAX_VALUE), 1);
        if (result > Long.MIN_VALUE + 1) {
            assertCount(searcher, expression.approximateRangeQuery(randomLongBetween(Long.MIN_VALUE, result - 1), result + 1), 1);
        }
        if (result < Long.MAX_VALUE - 1) {
            assertCount(searcher, expression.approximateRangeQuery(result - 1, randomLongBetween(result + 1, Long.MAX_VALUE)), 1);
        }
    }

    private void assertCount(IndexSearcher searcher, Query query, int count) throws IOException {
        assertThat("count for " + query, searcher.count(query), equalTo(count));
    }
}
