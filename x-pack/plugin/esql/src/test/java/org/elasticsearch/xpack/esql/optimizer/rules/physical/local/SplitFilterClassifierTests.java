/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.SplitStats;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.equalsOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.greaterThanOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.greaterThanOrEqualOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.lessThanOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.lessThanOrEqualOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.notEqualsOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.of;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.elasticsearch.xpack.esql.optimizer.rules.physical.local.SplitFilterClassifier.SplitMatch.AMBIGUOUS;
import static org.elasticsearch.xpack.esql.optimizer.rules.physical.local.SplitFilterClassifier.SplitMatch.MATCH;
import static org.elasticsearch.xpack.esql.optimizer.rules.physical.local.SplitFilterClassifier.SplitMatch.MISS;
import static org.elasticsearch.xpack.esql.optimizer.rules.physical.local.SplitFilterClassifier.classifyExpression;
import static org.elasticsearch.xpack.esql.optimizer.rules.physical.local.SplitFilterClassifier.classifySplit;
import static org.elasticsearch.xpack.esql.optimizer.rules.physical.local.SplitFilterClassifier.compareValues;

public class SplitFilterClassifierTests extends ESTestCase {

    private static final String COL = "age";
    private static final ReferenceAttribute AGE = referenceAttribute(COL, DataType.LONG);

    /** Standard stats: age in [10, 20], 100 rows, no nulls. */
    private static final SplitStats STATS_10_20 = colStats(COL, 10L, 20L, 100L, 0L);
    /** Stats: age in [30, 50], 100 rows, no nulls. */
    private static final SplitStats STATS_30_50 = colStats(COL, 30L, 50L, 100L, 0L);
    /** Stats: constant column age = 42, 100 rows, no nulls. */
    private static final SplitStats STATS_CONST_42 = colStats(COL, 42L, 42L, 100L, 0L);

    // --- GreaterThan ---

    public void testGreaterThanMatch() {
        assertEquals(MATCH, classify(greaterThanOf(AGE, of(5L)), STATS_10_20));
    }

    public void testGreaterThanMiss() {
        assertEquals(MISS, classify(greaterThanOf(AGE, of(25L)), STATS_10_20));
    }

    public void testGreaterThanMissAtBoundary() {
        assertEquals(MISS, classify(greaterThanOf(AGE, of(20L)), STATS_10_20));
    }

    public void testGreaterThanAmbiguous() {
        assertEquals(AMBIGUOUS, classify(greaterThanOf(AGE, of(15L)), STATS_10_20));
    }

    public void testGreaterThanAmbiguousWithNulls() {
        var stats = colStats(COL, 10L, 20L, 100L, 5L);
        assertEquals(AMBIGUOUS, classify(greaterThanOf(AGE, of(5L)), stats));
    }

    // --- GreaterThanOrEqual ---

    public void testGreaterThanOrEqualMatch() {
        assertEquals(MATCH, classify(greaterThanOrEqualOf(AGE, of(10L)), STATS_10_20));
    }

    public void testGreaterThanOrEqualMiss() {
        assertEquals(MISS, classify(greaterThanOrEqualOf(AGE, of(21L)), STATS_10_20));
    }

    public void testGreaterThanOrEqualAmbiguous() {
        assertEquals(AMBIGUOUS, classify(greaterThanOrEqualOf(AGE, of(15L)), STATS_10_20));
    }

    // --- LessThan ---

    public void testLessThanMatch() {
        assertEquals(MATCH, classify(lessThanOf(AGE, of(25L)), STATS_10_20));
    }

    public void testLessThanMiss() {
        assertEquals(MISS, classify(lessThanOf(AGE, of(5L)), STATS_10_20));
    }

    public void testLessThanMissAtBoundary() {
        assertEquals(MISS, classify(lessThanOf(AGE, of(10L)), STATS_10_20));
    }

    public void testLessThanAmbiguous() {
        assertEquals(AMBIGUOUS, classify(lessThanOf(AGE, of(15L)), STATS_10_20));
    }

    // --- LessThanOrEqual ---

    public void testLessThanOrEqualMatch() {
        assertEquals(MATCH, classify(lessThanOrEqualOf(AGE, of(20L)), STATS_10_20));
    }

    public void testLessThanOrEqualMiss() {
        assertEquals(MISS, classify(lessThanOrEqualOf(AGE, of(9L)), STATS_10_20));
    }

    // --- Equals ---

    public void testEqualsMatch() {
        assertEquals(MATCH, classify(equalsOf(AGE, of(42L)), STATS_CONST_42));
    }

    public void testEqualsMissBelowRange() {
        assertEquals(MISS, classify(equalsOf(AGE, of(5L)), STATS_10_20));
    }

    public void testEqualsMissAboveRange() {
        assertEquals(MISS, classify(equalsOf(AGE, of(25L)), STATS_10_20));
    }

    public void testEqualsAmbiguous() {
        assertEquals(AMBIGUOUS, classify(equalsOf(AGE, of(15L)), STATS_10_20));
    }

    // --- NotEquals ---

    public void testNotEqualsMatch() {
        assertEquals(MATCH, classify(notEqualsOf(AGE, of(99L)), STATS_CONST_42));
    }

    public void testNotEqualsMiss() {
        assertEquals(MISS, classify(notEqualsOf(AGE, of(42L)), STATS_CONST_42));
    }

    public void testNotEqualsAmbiguous() {
        assertEquals(AMBIGUOUS, classify(notEqualsOf(AGE, of(15L)), STATS_10_20));
    }

    // --- Reversed operand order (literal on left) ---

    public void testReversedGreaterThan() {
        // 25 > age => age < 25
        assertEquals(MATCH, classify(greaterThanOf(of(25L), AGE), STATS_10_20));
    }

    public void testReversedLessThan() {
        // 5 < age => age > 5
        assertEquals(MATCH, classify(lessThanOf(of(5L), AGE), STATS_10_20));
    }

    // --- AND conjunction ---

    public void testConjunctionAllMatch() {
        assertEquals(MATCH, classifySplit(List.of(greaterThanOrEqualOf(AGE, of(10L)), lessThanOrEqualOf(AGE, of(20L))), STATS_10_20, true));
    }

    public void testConjunctionOneMiss() {
        assertEquals(MISS, classifySplit(List.of(greaterThanOrEqualOf(AGE, of(10L)), lessThanOf(AGE, of(5L))), STATS_10_20, true));
    }

    public void testConjunctionMixedMatchAndAmbiguous() {
        assertEquals(AMBIGUOUS, classifySplit(List.of(greaterThanOrEqualOf(AGE, of(10L)), lessThanOf(AGE, of(15L))), STATS_10_20, true));
    }

    // --- Edge cases ---

    public void testEmptyConjuncts() {
        assertEquals(AMBIGUOUS, classifySplit(List.of(), STATS_10_20, true));
    }

    public void testNullStats() {
        assertEquals(AMBIGUOUS, classifySplit(List.of(greaterThanOf(AGE, of(5L))), null, true));
    }

    public void testEmptyStats() {
        assertEquals(AMBIGUOUS, classifyExpression(greaterThanOf(AGE, of(5L)), SplitStats.EMPTY, true));
    }

    public void testMissingColumnStats() {
        SplitStats stats = new SplitStats.Builder().rowCount(100).build();
        assertEquals(AMBIGUOUS, classify(greaterThanOf(AGE, of(5L)), stats));
    }

    public void testMissingNullCount() {
        SplitStats.Builder builder = new SplitStats.Builder();
        builder.rowCount(100);
        int col = builder.addColumn("age");
        builder.min(col, 10L);
        builder.max(col, 20L);
        SplitStats stats = builder.build();
        assertEquals(AMBIGUOUS, classify(greaterThanOf(AGE, of(5L)), stats));
    }

    public void testMissDoesNotRequireNullCount() {
        SplitStats.Builder builder = new SplitStats.Builder();
        builder.rowCount(100);
        int col = builder.addColumn("age");
        builder.min(col, 10L);
        builder.max(col, 20L);
        SplitStats stats = builder.build();
        assertEquals(MISS, classify(greaterThanOf(AGE, of(25L)), stats));
    }

    // --- Type coercion ---

    public void testIntegerStatLongLiteral() {
        var stats = colStats(COL, 10, 20, 100L, 0L);
        assertEquals(MATCH, classify(greaterThanOf(AGE, of(5L)), stats));
    }

    public void testDoubleStats() {
        ReferenceAttribute score = referenceAttribute("score", DataType.DOUBLE);
        SplitStats.Builder builder = new SplitStats.Builder();
        builder.rowCount(100);
        int scoreCol = builder.addColumn("score", 0L, 1.5, 9.5, -1);
        builder.valueCount(scoreCol, 100); // single-valued (one value per row) — required for comparison MATCH
        SplitStats stats = builder.build();
        assertEquals(MATCH, classify(greaterThanOf(score, of(0.5)), stats));
    }

    public void testStringStats() {
        ReferenceAttribute name = referenceAttribute("name", DataType.KEYWORD);
        SplitStats.Builder builder = new SplitStats.Builder();
        builder.rowCount(100);
        int nameCol = builder.addColumn("name", 0L, "alice", "charlie", -1);
        builder.valueCount(nameCol, 100); // single-valued (one value per row) — required for comparison MATCH
        SplitStats stats = builder.build();
        assertEquals(MATCH, classify(greaterThanOf(name, of("aaron")), stats));
    }

    // --- Or ---

    public void testOrBothMatch() {
        assertEquals(MATCH, classify(or(greaterThanOf(AGE, of(20L)), lessThanOf(AGE, of(60L))), STATS_30_50));
    }

    public void testOrLeftMatchRightMiss() {
        assertEquals(MATCH, classify(or(greaterThanOf(AGE, of(20L)), lessThanOf(AGE, of(10L))), STATS_30_50));
    }

    public void testOrLeftMissRightMatch() {
        assertEquals(MATCH, classify(or(greaterThanOf(AGE, of(60L)), lessThanOf(AGE, of(60L))), STATS_30_50));
    }

    public void testOrBothMiss() {
        assertEquals(MISS, classify(or(greaterThanOf(AGE, of(60L)), lessThanOf(AGE, of(20L))), STATS_30_50));
    }

    public void testOrOneAmbiguous() {
        assertEquals(AMBIGUOUS, classify(or(greaterThanOf(AGE, of(40L)), lessThanOf(AGE, of(20L))), STATS_30_50));
    }

    public void testOrBothAmbiguous() {
        assertEquals(AMBIGUOUS, classify(or(greaterThanOf(AGE, of(35L)), lessThanOf(AGE, of(45L))), STATS_30_50));
    }

    // --- Not ---

    public void testNotOfMatchBecomesMiss() {
        assertEquals(MISS, classify(not(greaterThanOf(AGE, of(20L))), STATS_30_50));
    }

    public void testNotOfMissBecomesAmbiguous() {
        assertEquals(AMBIGUOUS, classify(not(greaterThanOf(AGE, of(60L))), STATS_30_50));
    }

    public void testNotOfAmbiguousStaysAmbiguous() {
        assertEquals(AMBIGUOUS, classify(not(greaterThanOf(AGE, of(40L))), STATS_30_50));
    }

    public void testNotNotOfMatch() {
        assertEquals(AMBIGUOUS, classify(not(not(greaterThanOf(AGE, of(20L)))), STATS_30_50));
    }

    // --- Nested And/Or/Not ---

    public void testAndInsideOr() {
        Expression filter = or(and(greaterThanOf(AGE, of(20L)), lessThanOf(AGE, of(60L))), greaterThanOf(AGE, of(70L)));
        assertEquals(MATCH, classify(filter, STATS_30_50));
    }

    public void testOrInsideAnd() {
        Expression filter = and(or(greaterThanOf(AGE, of(60L)), lessThanOf(AGE, of(20L))), greaterThanOf(AGE, of(20L)));
        assertEquals(MISS, classify(filter, STATS_30_50));
    }

    public void testNotInsideOr() {
        assertEquals(MATCH, classify(or(not(greaterThanOf(AGE, of(60L))), greaterThanOf(AGE, of(20L))), STATS_30_50));
    }

    // --- IS NULL ---

    public void testIsNullMissWhenNoNulls() {
        assertEquals(MISS, classify(isNull(AGE), STATS_30_50));
    }

    public void testIsNullMatchWhenAllNull() {
        assertEquals(MATCH, classify(isNull(AGE), colStats(COL, 30L, 50L, 100L, 100L)));
    }

    public void testIsNullAmbiguousWhenSomeNulls() {
        assertEquals(AMBIGUOUS, classify(isNull(AGE), colStats(COL, 30L, 50L, 100L, 50L)));
    }

    public void testIsNullAmbiguousWhenNoNullCount() {
        SplitStats.Builder builder = new SplitStats.Builder();
        builder.rowCount(100);
        int col = builder.addColumn("age");
        builder.min(col, 30L);
        builder.max(col, 50L);
        SplitStats stats = builder.build();
        assertEquals(AMBIGUOUS, classify(isNull(AGE), stats));
    }

    /**
     * WRONG-DATA regression: for a format WITHOUT implicit nulls (partially-harvested text stats), an
     * ABSENT column means "not harvested" — its null count is unknowable, NOT rowCount. Classifying it
     * as all-null would MATCH {@code IS NULL} (COUNT(*) served as N where the truth is 0) or MISS
     * {@code IS NOT NULL} (COUNT(*) served as 0 where the truth is N). Both must be AMBIGUOUS so the
     * engine re-scans. Under implicit nulls (footer formats) the all-null classification stays correct.
     */
    public void testIsNullFamilyAmbiguousForUnharvestedTextColumn() {
        // Stats harvested only for "other" — the filtered column "age" was never observed.
        SplitStats partial = colStats("other", 1L, 9L, 100L, 0L);
        assertEquals(AMBIGUOUS, classifyExpression(isNull(AGE), partial, false));
        assertEquals(AMBIGUOUS, classifyExpression(isNotNull(AGE), partial, false));
        // Footer semantics: absent == all-null stays classifiable.
        assertEquals(MATCH, classifyExpression(isNull(AGE), partial, true));
        assertEquals(MISS, classifyExpression(isNotNull(AGE), partial, true));
        // A column the text harvest DID observe classifies normally without implicit nulls.
        SplitStats harvested = colStats(COL, 30L, 50L, 100L, 0L);
        assertEquals(MISS, classifyExpression(isNull(AGE), harvested, false));
        assertEquals(MATCH, classifyExpression(isNotNull(AGE), harvested, false));
    }

    // --- IS NOT NULL ---

    public void testIsNotNullMatchWhenNoNulls() {
        assertEquals(MATCH, classify(isNotNull(AGE), STATS_30_50));
    }

    public void testIsNotNullMissWhenAllNull() {
        assertEquals(MISS, classify(isNotNull(AGE), colStats(COL, 30L, 50L, 100L, 100L)));
    }

    public void testIsNotNullAmbiguousWhenSomeNulls() {
        assertEquals(AMBIGUOUS, classify(isNotNull(AGE), colStats(COL, 30L, 50L, 100L, 50L)));
    }

    // --- IN ---

    public void testInMissAllLiteralsOutOfRange() {
        assertEquals(MISS, classify(in(AGE, of(10L), of(15L), of(60L)), STATS_30_50));
    }

    public void testInAmbiguousWhenLiteralInRange() {
        assertEquals(AMBIGUOUS, classify(in(AGE, of(10L), of(40L), of(60L)), STATS_30_50));
    }

    public void testInMatchWhenConstantColumnEqualsLiteral() {
        assertEquals(MATCH, classify(in(AGE, of(10L), of(42L), of(60L)), STATS_CONST_42));
    }

    public void testInAmbiguousWhenConstantColumnNotInList() {
        assertEquals(MISS, classify(in(AGE, of(10L), of(20L), of(60L)), STATS_CONST_42));
    }

    public void testInAmbiguousWithNulls() {
        assertEquals(AMBIGUOUS, classify(in(AGE, of(42L)), colStats(COL, 42L, 42L, 100L, 5L)));
    }

    public void testInWithAndFilter() {
        Expression filter = and(in(AGE, of(10L), of(15L)), greaterThanOf(AGE, of(20L)));
        assertEquals(MISS, classify(filter, STATS_30_50));
    }

    // --- compareValues ---

    public void testCompareValuesLongs() {
        assertEquals(-1, Integer.signum(compareValues(5L, 10L)));
        assertEquals(0, compareValues(10L, 10L));
        assertEquals(1, Integer.signum(compareValues(15L, 10L)));
    }

    public void testCompareValuesIntAndLong() {
        assertEquals(0, compareValues(10, 10L));
        assertEquals(-1, Integer.signum(compareValues(5, 10L)));
    }

    public void testCompareValuesDoubles() {
        assertEquals(-1, Integer.signum(compareValues(1.5, 2.5)));
        assertEquals(0, compareValues(1.5, 1.5));
    }

    public void testCompareValuesNull() {
        assertEquals(Integer.MIN_VALUE, compareValues(null, 10L));
        assertEquals(Integer.MIN_VALUE, compareValues(10L, null));
    }

    // --- helpers ---

    private static SplitFilterClassifier.SplitMatch classify(Expression filter, SplitStats stats) {
        return classifyExpression(filter, stats, true);
    }

    private static SplitStats colStats(String colName, Object min, Object max, long rowCount, long nullCount) {
        // Single-valued: value_count == rowCount - nullCount (one value per non-null row). A comparison MATCH is
        // only sound for single-valued columns, so the classifier requires this; these fixtures model that reality.
        return colStats(colName, min, max, rowCount, nullCount, rowCount - nullCount);
    }

    private static SplitStats colStats(String colName, Object min, Object max, long rowCount, long nullCount, long valueCount) {
        SplitStats.Builder builder = new SplitStats.Builder();
        builder.rowCount(rowCount);
        int col = builder.addColumn(colName);
        builder.nullCount(col, nullCount);
        builder.valueCount(col, valueCount);
        builder.min(col, min);
        builder.max(col, max);
        return builder.build();
    }

    public void testComparisonOnMultivaluedColumnIsAmbiguousNotMatch() {
        // B3 (elastic/elasticsearch#150920): a MULTIVALUED column's comparison evaluates to NULL in the engine
        // (e.g. [5,5] == 5 -> NULL, so the row does NOT match), yet min==max would classify MATCH and serve the
        // full count -> an over-count. A split where value_count > rowCount - nullCount (some row has >1 value)
        // must NOT MATCH a comparison/IN -> AMBIGUOUS (the engine re-scans). MISS stays sound.
        SplitStats mv = colStats(COL, 10L, 20L, 100L, 0L, 150L); // 100 rows, 0 nulls, 150 values -> multivalued
        assertEquals(AMBIGUOUS, classify(greaterThanOf(AGE, of(5L)), mv)); // min 10 > 5 -> MATCH if single-valued
        assertEquals(AMBIGUOUS, classify(equalsOf(AGE, of(15L)), colStats(COL, 15L, 15L, 100L, 0L, 200L))); // == on MV
        // MISS is still sound on a multivalued column (every value out of range -> no row can match).
        assertEquals(MISS, classify(greaterThanOf(AGE, of(25L)), mv));
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
