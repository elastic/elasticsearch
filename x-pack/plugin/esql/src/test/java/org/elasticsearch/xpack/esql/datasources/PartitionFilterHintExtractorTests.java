/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.PartitionFilterHintExtractor.Operator;
import org.elasticsearch.xpack.esql.datasources.PartitionFilterHintExtractor.PartitionFilterHint;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;

import java.util.List;
import java.util.Map;

public class PartitionFilterHintExtractorTests extends ESTestCase {

    private static final Source SRC = Source.EMPTY;

    public void testEqualsHint() {
        LogicalPlan plan = filterAboveExternal(new Equals(SRC, unresolved("year"), intLiteral(2024)), "s3://bucket/data/*.parquet");

        Map<String, List<PartitionFilterHint>> hints = PartitionFilterHintExtractor.extract(plan);

        assertEquals(1, hints.size());
        List<PartitionFilterHint> pathHints = hints.get("s3://bucket/data/*.parquet");
        assertNotNull(pathHints);
        assertEquals(1, pathHints.size());
        assertEquals("year", pathHints.get(0).columnName());
        assertEquals(Operator.EQUALS, pathHints.get(0).operator());
        assertEquals(List.of(2024), pathHints.get(0).values());
    }

    public void testNotEqualsHint() {
        LogicalPlan plan = filterAboveExternal(
            new NotEquals(SRC, unresolved("status"), keywordLiteral("deleted")),
            "s3://bucket/data/*.parquet"
        );

        Map<String, List<PartitionFilterHint>> hints = PartitionFilterHintExtractor.extract(plan);

        assertEquals(1, hints.size());
        List<PartitionFilterHint> pathHints = hints.get("s3://bucket/data/*.parquet");
        assertEquals(1, pathHints.size());
        assertEquals(Operator.NOT_EQUALS, pathHints.get(0).operator());
        assertEquals("deleted", pathHints.get(0).values().get(0));
    }

    public void testGreaterThanHint() {
        LogicalPlan plan = filterAboveExternal(new GreaterThan(SRC, unresolved("year"), intLiteral(2020)), "s3://bucket/data/*.parquet");

        Map<String, List<PartitionFilterHint>> hints = PartitionFilterHintExtractor.extract(plan);

        List<PartitionFilterHint> pathHints = hints.get("s3://bucket/data/*.parquet");
        assertEquals(1, pathHints.size());
        assertEquals(Operator.GREATER_THAN, pathHints.get(0).operator());
    }

    public void testGreaterThanOrEqualHint() {
        LogicalPlan plan = filterAboveExternal(
            new GreaterThanOrEqual(SRC, unresolved("year"), intLiteral(2020)),
            "s3://bucket/data/*.parquet"
        );

        List<PartitionFilterHint> pathHints = PartitionFilterHintExtractor.extract(plan).get("s3://bucket/data/*.parquet");
        assertEquals(Operator.GREATER_THAN_OR_EQUAL, pathHints.get(0).operator());
    }

    public void testLessThanHint() {
        LogicalPlan plan = filterAboveExternal(new LessThan(SRC, unresolved("year"), intLiteral(2025)), "s3://bucket/data/*.parquet");

        List<PartitionFilterHint> pathHints = PartitionFilterHintExtractor.extract(plan).get("s3://bucket/data/*.parquet");
        assertEquals(Operator.LESS_THAN, pathHints.get(0).operator());
    }

    public void testLessThanOrEqualHint() {
        LogicalPlan plan = filterAboveExternal(
            new LessThanOrEqual(SRC, unresolved("year"), intLiteral(2025)),
            "s3://bucket/data/*.parquet"
        );

        List<PartitionFilterHint> pathHints = PartitionFilterHintExtractor.extract(plan).get("s3://bucket/data/*.parquet");
        assertEquals(Operator.LESS_THAN_OR_EQUAL, pathHints.get(0).operator());
    }

    public void testReversedComparison() {
        LogicalPlan plan = filterAboveExternal(new LessThan(SRC, intLiteral(2020), unresolved("year")), "s3://bucket/data/*.parquet");

        List<PartitionFilterHint> pathHints = PartitionFilterHintExtractor.extract(plan).get("s3://bucket/data/*.parquet");
        assertEquals(1, pathHints.size());
        assertEquals("year", pathHints.get(0).columnName());
        assertEquals(Operator.GREATER_THAN, pathHints.get(0).operator());
    }

    public void testInHint() {
        LogicalPlan plan = filterAboveExternal(
            new In(SRC, unresolved("month"), List.of(intLiteral(1), intLiteral(2), intLiteral(3))),
            "s3://bucket/data/*.parquet"
        );

        Map<String, List<PartitionFilterHint>> hints = PartitionFilterHintExtractor.extract(plan);

        List<PartitionFilterHint> pathHints = hints.get("s3://bucket/data/*.parquet");
        assertEquals(1, pathHints.size());
        assertEquals("month", pathHints.get(0).columnName());
        assertEquals(Operator.IN, pathHints.get(0).operator());
        assertEquals(List.of(1, 2, 3), pathHints.get(0).values());
    }

    public void testAndCombinedHints() {
        Expression condition = new And(
            SRC,
            new Equals(SRC, unresolved("year"), intLiteral(2024)),
            new In(SRC, unresolved("month"), List.of(intLiteral(1), intLiteral(2), intLiteral(3)))
        );

        LogicalPlan plan = filterAboveExternal(condition, "s3://bucket/data/*.parquet");

        List<PartitionFilterHint> pathHints = PartitionFilterHintExtractor.extract(plan).get("s3://bucket/data/*.parquet");
        assertEquals(2, pathHints.size());
        assertEquals("year", pathHints.get(0).columnName());
        assertEquals(Operator.EQUALS, pathHints.get(0).operator());
        assertEquals("month", pathHints.get(1).columnName());
        assertEquals(Operator.IN, pathHints.get(1).operator());
    }

    public void testNoFilterReturnsEmpty() {
        UnresolvedExternalRelation rel = new UnresolvedExternalRelation(SRC, Literal.keyword(SRC, "s3://bucket/data/*.parquet"), Map.of());

        Map<String, List<PartitionFilterHint>> hints = PartitionFilterHintExtractor.extract(rel);
        assertTrue(hints.isEmpty());
    }

    public void testUnsupportedExpressionIgnored() {
        // func(year) = 2024 — function call on column side, should be ignored
        // We simulate this by using a Literal on both sides (neither is UnresolvedAttribute)
        Expression condition = new Equals(SRC, intLiteral(1), intLiteral(2024));

        LogicalPlan plan = filterAboveExternal(condition, "s3://bucket/data/*.parquet");

        Map<String, List<PartitionFilterHint>> hints = PartitionFilterHintExtractor.extract(plan);
        assertTrue(hints.isEmpty());
    }

    public void testInWithNonLiteralListIgnored() {
        // IN where one list item is not a literal — should be ignored
        Expression condition = new In(SRC, unresolved("month"), List.of(intLiteral(1), unresolved("other")));

        LogicalPlan plan = filterAboveExternal(condition, "s3://bucket/data/*.parquet");

        Map<String, List<PartitionFilterHint>> hints = PartitionFilterHintExtractor.extract(plan);
        assertTrue(hints.isEmpty());
    }

    public void testKeywordLiteralNormalizedToString() {
        LogicalPlan plan = filterAboveExternal(new Equals(SRC, unresolved("country"), keywordLiteral("US")), "s3://bucket/data/*.parquet");

        Map<String, List<PartitionFilterHint>> hints = PartitionFilterHintExtractor.extract(plan);

        List<PartitionFilterHint> pathHints = hints.get("s3://bucket/data/*.parquet");
        assertEquals(1, pathHints.size());
        assertEquals("country", pathHints.get(0).columnName());
        assertEquals(Operator.EQUALS, pathHints.get(0).operator());
        Object value = pathHints.get(0).values().get(0);
        assertFalse("hint value should not be a BytesRef", value instanceof BytesRef);
        assertEquals("US", value);
    }

    public void testInWithKeywordLiteralsNormalizedToString() {
        LogicalPlan plan = filterAboveExternal(
            new In(SRC, unresolved("country"), List.of(keywordLiteral("US"), keywordLiteral("DE"))),
            "s3://bucket/data/*.parquet"
        );

        Map<String, List<PartitionFilterHint>> hints = PartitionFilterHintExtractor.extract(plan);

        List<PartitionFilterHint> pathHints = hints.get("s3://bucket/data/*.parquet");
        assertEquals(1, pathHints.size());
        assertEquals(Operator.IN, pathHints.get(0).operator());
        for (Object val : pathHints.get(0).values()) {
            assertFalse("hint value should not be a BytesRef", val instanceof BytesRef);
        }
        assertEquals(List.of("US", "DE"), pathHints.get(0).values());
    }

    public void testMultipleExternalRelations() {
        UnresolvedExternalRelation rel1 = new UnresolvedExternalRelation(SRC, Literal.keyword(SRC, "s3://bucket/a/*.parquet"), Map.of());
        UnresolvedExternalRelation rel2 = new UnresolvedExternalRelation(SRC, Literal.keyword(SRC, "s3://bucket/b/*.parquet"), Map.of());

        // Filter -> rel1 (only rel1 gets the hint)
        Filter filterPlan = new Filter(SRC, rel1, new Equals(SRC, unresolved("year"), intLiteral(2024)));

        Map<String, List<PartitionFilterHint>> hints = PartitionFilterHintExtractor.extract(filterPlan);
        assertEquals(1, hints.size());
        assertNotNull(hints.get("s3://bucket/a/*.parquet"));
        assertNull(hints.get("s3://bucket/b/*.parquet"));
    }

    private static LogicalPlan filterAboveExternal(Expression condition, String path) {
        UnresolvedExternalRelation rel = new UnresolvedExternalRelation(SRC, Literal.keyword(SRC, path), Map.of());
        return new Filter(SRC, rel, condition);
    }

    private static UnresolvedAttribute unresolved(String name) {
        return new UnresolvedAttribute(SRC, name);
    }

    private static Literal intLiteral(int value) {
        return new Literal(SRC, value, DataType.INTEGER);
    }

    private static Literal keywordLiteral(String value) {
        return Literal.keyword(SRC, value);
    }
}
