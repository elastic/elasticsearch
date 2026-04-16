/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.optimizer.rules.physical.local.SplitFilterClassifier.SplitMatch.AMBIGUOUS;
import static org.elasticsearch.xpack.esql.optimizer.rules.physical.local.SplitFilterClassifier.SplitMatch.MATCH;
import static org.elasticsearch.xpack.esql.optimizer.rules.physical.local.SplitFilterClassifier.SplitMatch.MISS;

public class SplitFilterClassifierTests extends ESTestCase {

    // --- GreaterThan ---

    public void testGreaterThanMatch() {
        Map<String, Object> stats = splitStats(10L, 20L, 100L, 0L);
        Expression filter = gt(ref("age"), lit(5L));
        assertEquals(MATCH, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    public void testGreaterThanMiss() {
        Map<String, Object> stats = splitStats(10L, 20L, 100L, 0L);
        Expression filter = gt(ref("age"), lit(25L));
        assertEquals(MISS, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    public void testGreaterThanMissAtBoundary() {
        Map<String, Object> stats = splitStats(10L, 20L, 100L, 0L);
        Expression filter = gt(ref("age"), lit(20L));
        assertEquals(MISS, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    public void testGreaterThanAmbiguous() {
        Map<String, Object> stats = splitStats(10L, 20L, 100L, 0L);
        Expression filter = gt(ref("age"), lit(15L));
        assertEquals(AMBIGUOUS, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    public void testGreaterThanAmbiguousWithNulls() {
        Map<String, Object> stats = splitStats(10L, 20L, 100L, 5L);
        Expression filter = gt(ref("age"), lit(5L));
        assertEquals(AMBIGUOUS, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    // --- GreaterThanOrEqual ---

    public void testGreaterThanOrEqualMatch() {
        Map<String, Object> stats = splitStats(10L, 20L, 100L, 0L);
        Expression filter = gte(ref("age"), lit(10L));
        assertEquals(MATCH, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    public void testGreaterThanOrEqualMiss() {
        Map<String, Object> stats = splitStats(10L, 20L, 100L, 0L);
        Expression filter = gte(ref("age"), lit(21L));
        assertEquals(MISS, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    public void testGreaterThanOrEqualAmbiguous() {
        Map<String, Object> stats = splitStats(10L, 20L, 100L, 0L);
        Expression filter = gte(ref("age"), lit(15L));
        assertEquals(AMBIGUOUS, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    // --- LessThan ---

    public void testLessThanMatch() {
        Map<String, Object> stats = splitStats(10L, 20L, 100L, 0L);
        Expression filter = lt(ref("age"), lit(25L));
        assertEquals(MATCH, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    public void testLessThanMiss() {
        Map<String, Object> stats = splitStats(10L, 20L, 100L, 0L);
        Expression filter = lt(ref("age"), lit(5L));
        assertEquals(MISS, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    public void testLessThanMissAtBoundary() {
        Map<String, Object> stats = splitStats(10L, 20L, 100L, 0L);
        Expression filter = lt(ref("age"), lit(10L));
        assertEquals(MISS, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    public void testLessThanAmbiguous() {
        Map<String, Object> stats = splitStats(10L, 20L, 100L, 0L);
        Expression filter = lt(ref("age"), lit(15L));
        assertEquals(AMBIGUOUS, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    // --- LessThanOrEqual ---

    public void testLessThanOrEqualMatch() {
        Map<String, Object> stats = splitStats(10L, 20L, 100L, 0L);
        Expression filter = lte(ref("age"), lit(20L));
        assertEquals(MATCH, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    public void testLessThanOrEqualMiss() {
        Map<String, Object> stats = splitStats(10L, 20L, 100L, 0L);
        Expression filter = lte(ref("age"), lit(9L));
        assertEquals(MISS, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    // --- Equals ---

    public void testEqualsMatch() {
        Map<String, Object> stats = splitStats(42L, 42L, 100L, 0L);
        Expression filter = eq(ref("age"), lit(42L));
        assertEquals(MATCH, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    public void testEqualsMissBelowRange() {
        Map<String, Object> stats = splitStats(10L, 20L, 100L, 0L);
        Expression filter = eq(ref("age"), lit(5L));
        assertEquals(MISS, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    public void testEqualsMissAboveRange() {
        Map<String, Object> stats = splitStats(10L, 20L, 100L, 0L);
        Expression filter = eq(ref("age"), lit(25L));
        assertEquals(MISS, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    public void testEqualsAmbiguous() {
        Map<String, Object> stats = splitStats(10L, 20L, 100L, 0L);
        Expression filter = eq(ref("age"), lit(15L));
        assertEquals(AMBIGUOUS, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    // --- NotEquals ---

    public void testNotEqualsMatch() {
        Map<String, Object> stats = splitStats(42L, 42L, 100L, 0L);
        Expression filter = neq(ref("age"), lit(99L));
        assertEquals(MATCH, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    public void testNotEqualsMiss() {
        Map<String, Object> stats = splitStats(42L, 42L, 100L, 0L);
        Expression filter = neq(ref("age"), lit(42L));
        assertEquals(MISS, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    public void testNotEqualsAmbiguous() {
        Map<String, Object> stats = splitStats(10L, 20L, 100L, 0L);
        Expression filter = neq(ref("age"), lit(15L));
        assertEquals(AMBIGUOUS, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    // --- Reversed operand order (literal on left) ---

    public void testReversedGreaterThan() {
        Map<String, Object> stats = splitStats(10L, 20L, 100L, 0L);
        // 25 > age ⟹ age < 25
        Expression filter = gt(lit(25L), ref("age"));
        assertEquals(MATCH, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    public void testReversedLessThan() {
        Map<String, Object> stats = splitStats(10L, 20L, 100L, 0L);
        // 5 < age ⟹ age > 5
        Expression filter = lt(lit(5L), ref("age"));
        assertEquals(MATCH, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    // --- AND conjunction ---

    public void testConjunctionAllMatch() {
        Map<String, Object> stats = splitStats(10L, 20L, 100L, 0L);
        Expression f1 = gte(ref("age"), lit(10L));
        Expression f2 = lte(ref("age"), lit(20L));
        assertEquals(MATCH, SplitFilterClassifier.classifySplit(List.of(f1, f2), stats));
    }

    public void testConjunctionOneMiss() {
        Map<String, Object> stats = splitStats(10L, 20L, 100L, 0L);
        Expression f1 = gte(ref("age"), lit(10L));
        Expression f2 = lt(ref("age"), lit(5L));
        assertEquals(MISS, SplitFilterClassifier.classifySplit(List.of(f1, f2), stats));
    }

    public void testConjunctionMixedMatchAndAmbiguous() {
        Map<String, Object> stats = splitStats(10L, 20L, 100L, 0L);
        Expression f1 = gte(ref("age"), lit(10L));  // MATCH
        Expression f2 = lt(ref("age"), lit(15L));    // AMBIGUOUS
        assertEquals(AMBIGUOUS, SplitFilterClassifier.classifySplit(List.of(f1, f2), stats));
    }

    // --- Edge cases ---

    public void testEmptyConjuncts() {
        Map<String, Object> stats = splitStats(10L, 20L, 100L, 0L);
        assertEquals(AMBIGUOUS, SplitFilterClassifier.classifySplit(List.of(), stats));
    }

    public void testNullStats() {
        Expression filter = gt(ref("age"), lit(5L));
        assertEquals(AMBIGUOUS, SplitFilterClassifier.classifySplit(List.of(filter), null));
    }

    public void testEmptyStats() {
        Expression filter = gt(ref("age"), lit(5L));
        assertEquals(AMBIGUOUS, SplitFilterClassifier.classifySplit(List.of(filter), Map.of()));
    }

    public void testMissingColumnStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 100L);
        Expression filter = gt(ref("age"), lit(5L));
        assertEquals(AMBIGUOUS, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    public void testMissingNullCount() {
        Map<String, Object> stats = new HashMap<>();
        stats.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 100L);
        stats.put("_stats.columns.age.min", 10L);
        stats.put("_stats.columns.age.max", 20L);
        // min > 5, but no null_count → can't confirm MATCH (could have nulls)
        Expression filter = gt(ref("age"), lit(5L));
        assertEquals(AMBIGUOUS, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    public void testMissDoesNotRequireNullCount() {
        Map<String, Object> stats = new HashMap<>();
        stats.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 100L);
        stats.put("_stats.columns.age.min", 10L);
        stats.put("_stats.columns.age.max", 20L);
        // max <= 5, so all non-null rows miss; null rows also miss (NULL > 5 → UNKNOWN → FALSE)
        Expression filter = gt(ref("age"), lit(25L));
        assertEquals(MISS, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    // --- Type coercion ---

    public void testIntegerStatLongLiteral() {
        Map<String, Object> stats = splitStats(10, 20, 100L, 0L);
        Expression filter = gt(ref("age"), lit(5L));
        assertEquals(MATCH, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    public void testDoubleStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 100L);
        stats.put("_stats.columns.score.min", 1.5);
        stats.put("_stats.columns.score.max", 9.5);
        stats.put("_stats.columns.score.null_count", 0L);
        Expression filter = gt(ref("score"), lit(0.5));
        assertEquals(MATCH, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    public void testStringStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 100L);
        stats.put("_stats.columns.name.min", "alice");
        stats.put("_stats.columns.name.max", "charlie");
        stats.put("_stats.columns.name.null_count", 0L);
        Expression filter = gt(ref("name"), lit("aaron"));
        assertEquals(MATCH, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    // --- Or ---

    public void testOrBothMatch() {
        Map<String, Object> stats = splitStats(30L, 50L, 100L, 0L);
        Expression filter = or(gt(ref("age"), lit(20L)), lt(ref("age"), lit(60L)));
        assertEquals(MATCH, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    public void testOrLeftMatchRightMiss() {
        Map<String, Object> stats = splitStats(30L, 50L, 100L, 0L);
        Expression filter = or(gt(ref("age"), lit(20L)), lt(ref("age"), lit(10L)));
        assertEquals(MATCH, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    public void testOrLeftMissRightMatch() {
        Map<String, Object> stats = splitStats(30L, 50L, 100L, 0L);
        Expression filter = or(gt(ref("age"), lit(60L)), lt(ref("age"), lit(60L)));
        assertEquals(MATCH, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    public void testOrBothMiss() {
        Map<String, Object> stats = splitStats(30L, 50L, 100L, 0L);
        Expression filter = or(gt(ref("age"), lit(60L)), lt(ref("age"), lit(20L)));
        assertEquals(MISS, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    public void testOrOneAmbiguous() {
        Map<String, Object> stats = splitStats(30L, 50L, 100L, 0L);
        Expression filter = or(gt(ref("age"), lit(40L)), lt(ref("age"), lit(20L)));
        assertEquals(AMBIGUOUS, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    public void testOrBothAmbiguous() {
        Map<String, Object> stats = splitStats(30L, 50L, 100L, 0L);
        Expression filter = or(gt(ref("age"), lit(35L)), lt(ref("age"), lit(45L)));
        assertEquals(AMBIGUOUS, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    // --- Not ---

    public void testNotOfMatchBecomesMiss() {
        Map<String, Object> stats = splitStats(30L, 50L, 100L, 0L);
        Expression filter = not(gt(ref("age"), lit(20L)));
        assertEquals(MISS, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    public void testNotOfMissBecomesAmbiguous() {
        Map<String, Object> stats = splitStats(30L, 50L, 100L, 0L);
        Expression filter = not(gt(ref("age"), lit(60L)));
        assertEquals(AMBIGUOUS, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    public void testNotOfAmbiguousStaysAmbiguous() {
        Map<String, Object> stats = splitStats(30L, 50L, 100L, 0L);
        Expression filter = not(gt(ref("age"), lit(40L)));
        assertEquals(AMBIGUOUS, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    public void testNotNotOfMatch() {
        Map<String, Object> stats = splitStats(30L, 50L, 100L, 0L);
        Expression filter = not(not(gt(ref("age"), lit(20L))));
        assertEquals(AMBIGUOUS, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    // --- Nested And Or Not ---

    public void testAndInsideOr() {
        Map<String, Object> stats = splitStats(30L, 50L, 100L, 0L);
        Expression filter = or(and(gt(ref("age"), lit(20L)), lt(ref("age"), lit(60L))), gt(ref("age"), lit(70L)));
        assertEquals(MATCH, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    public void testOrInsideAnd() {
        Map<String, Object> stats = splitStats(30L, 50L, 100L, 0L);
        Expression filter = and(or(gt(ref("age"), lit(60L)), lt(ref("age"), lit(20L))), gt(ref("age"), lit(20L)));
        assertEquals(MISS, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    public void testNotInsideOr() {
        Map<String, Object> stats = splitStats(30L, 50L, 100L, 0L);
        Expression filter = or(not(gt(ref("age"), lit(60L))), gt(ref("age"), lit(20L)));
        assertEquals(MATCH, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    // --- IS NULL tests ---

    public void testIsNullMissWhenNoNulls() {
        Map<String, Object> stats = splitStats(30L, 50L, 100L, 0L);
        Expression filter = isNull(ref("age"));
        assertEquals(MISS, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    public void testIsNullMatchWhenAllNull() {
        Map<String, Object> stats = splitStats(30L, 50L, 100L, 100L);
        Expression filter = isNull(ref("age"));
        assertEquals(MATCH, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    public void testIsNullAmbiguousWhenSomeNulls() {
        Map<String, Object> stats = splitStats(30L, 50L, 100L, 50L);
        Expression filter = isNull(ref("age"));
        assertEquals(AMBIGUOUS, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    public void testIsNullAmbiguousWhenNoNullCount() {
        Map<String, Object> stats = new HashMap<>();
        stats.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 100L);
        stats.put("_stats.columns.age.min", 30L);
        stats.put("_stats.columns.age.max", 50L);
        Expression filter = isNull(ref("age"));
        assertEquals(AMBIGUOUS, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    // --- IS NOT NULL tests ---

    public void testIsNotNullMatchWhenNoNulls() {
        Map<String, Object> stats = splitStats(30L, 50L, 100L, 0L);
        Expression filter = isNotNull(ref("age"));
        assertEquals(MATCH, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    public void testIsNotNullMissWhenAllNull() {
        Map<String, Object> stats = splitStats(30L, 50L, 100L, 100L);
        Expression filter = isNotNull(ref("age"));
        assertEquals(MISS, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    public void testIsNotNullAmbiguousWhenSomeNulls() {
        Map<String, Object> stats = splitStats(30L, 50L, 100L, 50L);
        Expression filter = isNotNull(ref("age"));
        assertEquals(AMBIGUOUS, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    // --- IN tests ---

    public void testInMissAllLiteralsOutOfRange() {
        Map<String, Object> stats = splitStats(30L, 50L, 100L, 0L);
        Expression filter = in(ref("age"), lit(10L), lit(15L), lit(60L));
        assertEquals(MISS, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    public void testInAmbiguousWhenLiteralInRange() {
        Map<String, Object> stats = splitStats(30L, 50L, 100L, 0L);
        Expression filter = in(ref("age"), lit(10L), lit(40L), lit(60L));
        assertEquals(AMBIGUOUS, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    public void testInMatchWhenConstantColumnEqualsLiteral() {
        Map<String, Object> stats = splitStats(42L, 42L, 100L, 0L);
        Expression filter = in(ref("age"), lit(10L), lit(42L), lit(60L));
        assertEquals(MATCH, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    public void testInAmbiguousWhenConstantColumnNotInList() {
        Map<String, Object> stats = splitStats(42L, 42L, 100L, 0L);
        Expression filter = in(ref("age"), lit(10L), lit(20L), lit(60L));
        assertEquals(MISS, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    public void testInAmbiguousWithNulls() {
        Map<String, Object> stats = splitStats(42L, 42L, 100L, 5L);
        Expression filter = in(ref("age"), lit(42L));
        assertEquals(AMBIGUOUS, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    public void testInWithAndFilter() {
        Map<String, Object> stats = splitStats(30L, 50L, 100L, 0L);
        Expression filter = and(in(ref("age"), lit(10L), lit(15L)), gt(ref("age"), lit(20L)));
        assertEquals(MISS, SplitFilterClassifier.classifySplit(List.of(filter), stats));
    }

    // --- compareValues ---

    public void testCompareValuesLongs() {
        assertEquals(-1, Integer.signum(SplitFilterClassifier.compareValues(5L, 10L)));
        assertEquals(0, SplitFilterClassifier.compareValues(10L, 10L));
        assertEquals(1, Integer.signum(SplitFilterClassifier.compareValues(15L, 10L)));
    }

    public void testCompareValuesIntAndLong() {
        assertEquals(0, SplitFilterClassifier.compareValues(10, 10L));
        assertEquals(-1, Integer.signum(SplitFilterClassifier.compareValues(5, 10L)));
    }

    public void testCompareValuesDoubles() {
        assertEquals(-1, Integer.signum(SplitFilterClassifier.compareValues(1.5, 2.5)));
        assertEquals(0, SplitFilterClassifier.compareValues(1.5, 1.5));
    }

    public void testCompareValuesNull() {
        assertEquals(Integer.MIN_VALUE, SplitFilterClassifier.compareValues(null, 10L));
        assertEquals(Integer.MIN_VALUE, SplitFilterClassifier.compareValues(10L, null));
    }

    // --- helpers ---

    private static Map<String, Object> splitStats(Object min, Object max, long rowCount, long nullCount) {
        Map<String, Object> stats = new HashMap<>();
        stats.put(SourceStatisticsSerializer.STATS_ROW_COUNT, rowCount);
        stats.put("_stats.columns.age.min", min);
        stats.put("_stats.columns.age.max", max);
        stats.put("_stats.columns.age.null_count", nullCount);
        return stats;
    }

    private static ReferenceAttribute ref(String name) {
        return new ReferenceAttribute(Source.EMPTY, name, DataType.LONG);
    }

    private static Literal lit(long value) {
        return new Literal(Source.EMPTY, value, DataType.LONG);
    }

    private static Literal lit(double value) {
        return new Literal(Source.EMPTY, value, DataType.DOUBLE);
    }

    private static Literal lit(String value) {
        return new Literal(Source.EMPTY, new BytesRef(value), DataType.KEYWORD);
    }

    private static Expression gt(Expression left, Expression right) {
        return new GreaterThan(Source.EMPTY, left, right, null);
    }

    private static Expression gte(Expression left, Expression right) {
        return new GreaterThanOrEqual(Source.EMPTY, left, right, null);
    }

    private static Expression lt(Expression left, Expression right) {
        return new LessThan(Source.EMPTY, left, right, null);
    }

    private static Expression lte(Expression left, Expression right) {
        return new LessThanOrEqual(Source.EMPTY, left, right, null);
    }

    private static Expression eq(Expression left, Expression right) {
        return new Equals(Source.EMPTY, left, right, null);
    }

    private static Expression neq(Expression left, Expression right) {
        return new NotEquals(Source.EMPTY, left, right, null);
    }

    private static Expression or(Expression left, Expression right) {
        return new Or(Source.EMPTY, left, right);
    }

    private static Expression not(Expression child) {
        return new Not(Source.EMPTY, child);
    }

    private static Expression and(Expression left, Expression right) {
        return new And(Source.EMPTY, left, right);
    }

    private static Expression isNull(Expression child) {
        return new IsNull(Source.EMPTY, child);
    }

    private static Expression isNotNull(Expression child) {
        return new IsNotNull(Source.EMPTY, child);
    }

    private static Expression in(Expression value, Literal... list) {
        return new In(Source.EMPTY, value, List.of(list));
    }
}
