/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.queryableexpression;

import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;
import java.util.function.BiConsumer;

import static org.elasticsearch.queryableexpression.IntQueryableExpressionTests.randomInterestingInt;
import static org.hamcrest.Matchers.equalTo;

public class LongQueryableExpressionTests extends ESTestCase {

    public void testLongField() throws IOException {
        QueryableExpressionBuilder builder = QueryableExpressionBuilder.field("foo");
        withIndexedLong((indexed, searcher, foo) -> {
            logger.info("{} = {}", indexed, indexed);

            LongQueryableExpression expression = builder.build(f -> foo, null).castToLong();
            assertThat(expression.toString(), equalTo("foo"));
            checkApproximations(searcher, expression, indexed);
            checkPerfectApproximation(searcher, expression, indexed);
        });
    }

    public void testLongFieldAppliedToUnknownOp() throws IOException {
        QueryableExpressionBuilder builder = QueryableExpressionBuilder.unknownOp(QueryableExpressionBuilder.field("foo"));

        withIndexedLong((indexed, searcher, foo) -> {
            LongQueryableExpression expression = builder.build(f -> foo, null).castToLong();
            assertThat(expression.toString(), equalTo("unknown(foo)"));
            checkApproximations(searcher, expression, indexed);
        });
    }

    public void testMissingLongFieldAppliedToUnknownOp() throws IOException {
        QueryableExpressionBuilder builder = QueryableExpressionBuilder.unknownOp(QueryableExpressionBuilder.field("foo"));

        withIndexedLongMissing((searcher, foo) -> {
            LongQueryableExpression expression = builder.build(f -> foo, null).castToLong();
            assertThat(expression.toString(), equalTo("unknown(foo)"));
            assertCount(searcher, expression.approximateTermQuery(randomInterestingLong()), 0);
        });
    }

    public void testLongFieldPlusLongConstant() throws IOException {
        QueryableExpressionBuilder builder = QueryableExpressionBuilder.add(
            QueryableExpressionBuilder.field("foo"),
            QueryableExpressionBuilder.param("added")
        );
        withIndexedLong((indexed, searcher, foo) -> {
            long added = randomInterestingLong();
            long result = indexed + added;
            logger.info("{} + {} = {}", indexed, added, result);

            LongQueryableExpression expression = builder.build(f -> foo, k -> added).castToLong();
            assertThat(expression.toString(), equalTo(added == 0 ? "foo" : "foo + " + added));
            checkApproximations(searcher, expression, result);
            checkPerfectApproximation(searcher, expression, result);
        });
    }

    public void testLongFieldTimesLongConstant() throws IOException {
        QueryableExpressionBuilder builder = QueryableExpressionBuilder.multiply(
            QueryableExpressionBuilder.field("foo"),
            QueryableExpressionBuilder.param("multiplied")
        );
        withIndexedLong((indexed, searcher, foo) -> {
            long multiplied = randomInterestingLong();
            long result = indexed * multiplied;
            logger.info("{} * {} = {}", indexed, multiplied, result);

            LongQueryableExpression expression = builder.build(f -> foo, k -> multiplied).castToLong();
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

    public void testLongFieldDividedByLongConstant() throws IOException {
        QueryableExpressionBuilder builder = QueryableExpressionBuilder.divide(
            QueryableExpressionBuilder.field("foo"),
            QueryableExpressionBuilder.param("divisor")
        );
        withIndexedLong((indexed, searcher, foo) -> {
            long divisor = randomValueOtherThan(0L, LongQueryableExpressionTests::randomInterestingLong);
            long result = indexed / divisor;
            logger.info("{} / {} = {}", indexed, divisor, result);

            LongQueryableExpression expression = builder.build(f -> foo, k -> divisor).castToLong();
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

    public void testLongConstantDividedByLongField() throws IOException {
        QueryableExpressionBuilder builder = QueryableExpressionBuilder.divide(
            QueryableExpressionBuilder.param("divisor"),
            QueryableExpressionBuilder.field("foo")
        );
        withIndexedLong((indexed, searcher, foo) -> {
            long divisor = randomValueOtherThan(0L, LongQueryableExpressionTests::randomInterestingLong);
            QueryableExpression expression = builder.build(f -> foo, k -> divisor);
            assertThat(expression, equalTo(UnqueryableExpression.UNQUERYABLE));
        });
    }

    public void testLongFieldPlusIntConstant() throws IOException {
        QueryableExpressionBuilder builder = QueryableExpressionBuilder.add(
            QueryableExpressionBuilder.field("foo"),
            QueryableExpressionBuilder.param("added")
        );
        withIndexedLong((indexed, searcher, foo) -> {
            int added = randomInterestingInt();
            long result = indexed + added;
            logger.info("{} + {} = {}", indexed, added, result);

            LongQueryableExpression expression = builder.build(f -> foo, k -> added).castToLong();
            assertThat(expression.toString(), equalTo(added == 0 ? "foo" : "foo + " + added));
            checkApproximations(searcher, expression, result);
            checkPerfectApproximation(searcher, expression, result);
        });
    }

    public void testLongFieldTimesIntConstant() throws IOException {
        QueryableExpressionBuilder builder = QueryableExpressionBuilder.multiply(
            QueryableExpressionBuilder.field("foo"),
            QueryableExpressionBuilder.param("multiplied")
        );
        withIndexedLong((indexed, searcher, foo) -> {
            int multiplied = randomInterestingInt();
            long result = indexed * multiplied;
            logger.info("{} * {} = {}", indexed, multiplied, result);

            LongQueryableExpression expression = builder.build(f -> foo, k -> multiplied).castToLong();
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

    public void testLongFieldDividedByIntConstant() throws IOException {
        QueryableExpressionBuilder builder = QueryableExpressionBuilder.divide(
            QueryableExpressionBuilder.field("foo"),
            QueryableExpressionBuilder.param("divisor")
        );
        withIndexedLong((indexed, searcher, foo) -> {
            int divisor = randomValueOtherThan(0, IntQueryableExpressionTests::randomInterestingInt);
            long result = indexed / divisor;
            logger.info("{} / {} = {}", indexed, divisor, result);

            LongQueryableExpression expression = builder.build(f -> foo, k -> divisor).castToLong();
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

    public void testIntFieldPlusLongConstant() throws IOException {
        QueryableExpressionBuilder builder = QueryableExpressionBuilder.add(
            QueryableExpressionBuilder.field("foo"),
            QueryableExpressionBuilder.param("added")
        );
        withIndexedInt((indexed, searcher, foo) -> {
            long added = randomInterestingInt();
            long result = indexed + added;
            logger.info("{} + {} = {}", indexed, added, result);

            LongQueryableExpression expression = builder.build(f -> foo, k -> added).castToLong();
            assertThat(expression.toString(), equalTo(added == 0 ? "foo" : "foo + " + added));
            checkApproximations(searcher, expression, result);
            checkPerfectApproximation(searcher, expression, result);
        });
    }

    public void testIntFieldTimesLongConstant() throws IOException {
        QueryableExpressionBuilder builder = QueryableExpressionBuilder.multiply(
            QueryableExpressionBuilder.field("foo"),
            QueryableExpressionBuilder.param("multiplied")
        );
        withIndexedInt((indexed, searcher, foo) -> {
            long multiplied = randomInterestingInt();
            long result = indexed * multiplied;
            logger.info("{} * {} = {}", indexed, multiplied, result);

            LongQueryableExpression expression = builder.build(f -> foo, k -> multiplied).castToLong();
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

    public void testIntFieldDividedByLongConstant() throws IOException {
        QueryableExpressionBuilder builder = QueryableExpressionBuilder.divide(
            QueryableExpressionBuilder.field("foo"),
            QueryableExpressionBuilder.param("divisor")
        );
        withIndexedInt((indexed, searcher, foo) -> {
            long divisor = randomValueOtherThan(0, IntQueryableExpressionTests::randomInterestingInt);
            long result = indexed / divisor;
            logger.info("{} / {} = {}", indexed, divisor, result);

            LongQueryableExpression expression = builder.build(f -> foo, k -> divisor).castToLong();
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

    public void testLongConstantPlusLongConstant() throws IOException {
        long lhs = randomInterestingLong();
        long rhs = randomInterestingLong();
        long result = lhs + rhs;
        logger.info("{} + {} = {}", lhs, rhs, result);

        QueryableExpressionBuilder builder = QueryableExpressionBuilder.add(
            QueryableExpressionBuilder.constant(lhs),
            QueryableExpressionBuilder.constant(rhs)
        );
        QueryableExpression expression = builder.build(null, null);
        assertThat(expression, equalTo(QueryableExpressionBuilder.constant(result).build(null, null)));
        assertThat(expression.toString(), equalTo(Long.toString(result)));
    }

    public void testLongConstantTimesLongConstant() throws IOException {
        long lhs = randomInterestingLong();
        long rhs = randomInterestingLong();
        long result = lhs * rhs;
        logger.info("{} * {} = {}", lhs, rhs, result);

        QueryableExpressionBuilder builder = QueryableExpressionBuilder.multiply(
            QueryableExpressionBuilder.constant(lhs),
            QueryableExpressionBuilder.constant(rhs)
        );
        QueryableExpression expression = builder.build(null, null);
        assertThat(expression, equalTo(QueryableExpressionBuilder.constant(result).build(null, null)));
        assertThat(expression.toString(), equalTo(Long.toString(result)));
    }

    public void testLongConstantDividedByLongConstant() throws IOException {
        long lhs = randomInterestingLong();
        long rhs = randomValueOtherThan(0L, LongQueryableExpressionTests::randomInterestingLong);
        long result = lhs / rhs;
        logger.info("{} */ {} = {}", lhs, rhs, result);

        QueryableExpressionBuilder builder = QueryableExpressionBuilder.divide(
            QueryableExpressionBuilder.constant(lhs),
            QueryableExpressionBuilder.constant(rhs)
        );
        QueryableExpression expression = builder.build(null, null);
        assertThat(expression, equalTo(QueryableExpressionBuilder.constant(result).build(null, null)));
        assertThat(expression.toString(), equalTo(Long.toString(result)));
    }

    @FunctionalInterface
    interface WithIndexedLong {
        void accept(long indexed, IndexSearcher searcher, QueryableExpression foo) throws IOException;
    }

    private static void withIndexedLong(WithIndexedLong callback) throws IOException {
        long indexed = randomInterestingLong();
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new LongPoint("foo", indexed)));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                callback.accept(indexed, searcher, LongQueryableExpression.field("foo", new LongQueryableExpression.LongQueries() {
                    @Override
                    public Query approximateExists() {
                        return new MatchAllDocsQuery();
                    }

                    @Override
                    public Query approximateTermQuery(long term) {
                        return LongPoint.newExactQuery("foo", term);
                    }

                    @Override
                    public Query approximateRangeQuery(long lower, long upper) {
                        return LongPoint.newRangeQuery("foo", lower, upper);
                    }
                }));
            }
        }
    }

    private static void withIndexedLongMissing(BiConsumer<IndexSearcher, QueryableExpression> callback) throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of());
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                callback.accept(searcher, LongQueryableExpression.field("foo", new LongQueryableExpression.LongQueries() {
                    @Override
                    public Query approximateExists() {
                        return new MatchNoDocsQuery();
                    }

                    @Override
                    public Query approximateTermQuery(long term) {
                        return LongPoint.newExactQuery("foo", term);
                    }

                    @Override
                    public Query approximateRangeQuery(long lower, long upper) {
                        return LongPoint.newRangeQuery("foo", lower, upper);
                    }
                }));
            }
        }
    }

    @FunctionalInterface
    interface WithIndexedInt {
        void accept(int indexed, IndexSearcher searcher, QueryableExpression foo) throws IOException;
    }

    static void withIndexedInt(WithIndexedInt callback) throws IOException {
        QueryableExpression foo = LongQueryableExpression.field("foo", new LongQueryableExpression.IntQueries() {
            @Override
            public Query approximateExists() {
                return new DocValuesFieldExistsQuery("foo");
            }

            @Override
            public Query approximateTermQuery(int term) {
                return IntPoint.newExactQuery("foo", term);
            }

            @Override
            public Query approximateRangeQuery(int lower, int upper) {
                return IntPoint.newRangeQuery("foo", lower, upper);
            }
        });
        int indexed = randomInterestingInt();
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new IntPoint("foo", indexed)));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                callback.accept(indexed, searcher, foo);
            }
        }
    }

    static long randomInterestingLong() {
        switch (between(0, 4)) {
            case 0:
                return 0;
            case 1:
                return randomBoolean() ? 1 : -1;
            case 2:
                return randomInt(); // Even multiplication won't overflow
            case 3:
                return randomLong();
            case 4:
                return randomBoolean() ? Long.MIN_VALUE : Long.MAX_VALUE;
            default:
                throw new IllegalArgumentException("Unsupported case");
        }
    }

    private static Tuple<Long, Long> randomBounds() {
        long min = randomInterestingLong();
        long max = randomValueOtherThan(min, LongQueryableExpressionTests::randomInterestingLong);
        return Tuple.tuple(Math.min(min, max), Math.max(min, max));
    }

    private void checkApproximations(IndexSearcher searcher, LongQueryableExpression expression, long result) {
        assertCount(searcher, expression.approximateTermQuery(result), 1);
        assertCount(searcher, expression.approximateRangeQuery(Long.MIN_VALUE, Long.MAX_VALUE), 1);
        assertCount(searcher, expression.approximateRangeQuery(randomLongBetween(Long.MIN_VALUE, result), result), 1);
        assertCount(searcher, expression.approximateRangeQuery(result, randomLongBetween(result, Long.MAX_VALUE)), 1);
    }

    private void checkPerfectApproximation(IndexSearcher searcher, LongQueryableExpression expression, long result) {
        assertCount(
            searcher,
            expression.approximateTermQuery(randomValueOtherThan(result, LongQueryableExpressionTests::randomInterestingLong)),
            0
        );
        Tuple<Long, Long> bounds = randomValueOtherThanMany(
            p -> p.v1() <= result && result <= p.v2(),
            LongQueryableExpressionTests::randomBounds
        );
        assertCount(searcher, expression.approximateRangeQuery(bounds.v1(), bounds.v2()), 0);
    }

    private void assertCount(IndexSearcher searcher, Query query, int count) {
        try {
            assertThat("count for " + query, searcher.count(query), equalTo(count));
        } catch (IOException e) {
            assumeNoException("count", e);
        }
    }
}
