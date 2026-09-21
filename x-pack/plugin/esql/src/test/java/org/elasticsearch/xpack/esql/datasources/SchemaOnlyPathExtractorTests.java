/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Which relations a query asks for a schema and no rows. The set gates how much of a glob the
 * resolver must list, so a false positive bounds a listing that then reaches split discovery, and the query
 * returns a fraction of its rows and reports success; a narrower set of partition columns is the lesser
 * consequence. A false negative only costs the listing we pay today. The cases below pin that asymmetry: every shape
 * whose rows are actually consumed must stay out of the set, including when the same path is also
 * named by a branch that discards them.
 */
public class SchemaOnlyPathExtractorTests extends ESTestCase {

    private static final Source SRC = Source.EMPTY;
    private static final String PATH = "s3://bucket/data/*.parquet";
    private static final String OTHER = "s3://bucket/other/*.parquet";

    public void testLimitZeroReadsNoRows() {
        LogicalPlan plan = new Limit(SRC, intLiteral(0), externalRelation(PATH));

        assertEquals(Set.of(PATH), SchemaOnlyPathExtractor.pathsReadingNoRows(plan));
    }

    public void testPositiveLimitReadsRows() {
        LogicalPlan plan = new Limit(SRC, intLiteral(5), externalRelation(PATH));

        assertEquals(Set.of(), SchemaOnlyPathExtractor.pathsReadingNoRows(plan));
    }

    public void testNoLimitReadsRows() {
        assertEquals(Set.of(), SchemaOnlyPathExtractor.pathsReadingNoRows(externalRelation(PATH)));
    }

    public void testLimitZeroAboveAFilterStillReadsNoRows() {
        // The filter changes which rows would be produced, not how many are kept: none.
        Filter filter = new Filter(SRC, externalRelation(PATH), new GreaterThan(SRC, unresolved("x"), intLiteral(1)));
        LogicalPlan plan = new Limit(SRC, intLiteral(0), filter);

        assertEquals(Set.of(PATH), SchemaOnlyPathExtractor.pathsReadingNoRows(plan));
    }

    public void testLimitZeroAboveProjectionStillReadsNoRows() {
        Keep keep = new Keep(SRC, externalRelation(PATH), List.of(unresolved("a")));
        LogicalPlan plan = new Limit(SRC, intLiteral(0), keep);

        assertEquals(Set.of(PATH), SchemaOnlyPathExtractor.pathsReadingNoRows(plan));
    }

    public void testAggregateUnderLimitZeroStillReadsRows() {
        // STATS consumes rows to produce its one output row, and the outer LIMIT 0 discards that
        // row rather than the scan. Marking this schema-only would resolve from one file and
        // answer COUNT(*) wrongly if the shape ever survived to execution.
        Aggregate aggregate = new Aggregate(SRC, externalRelation(PATH), List.of(), List.<NamedExpression>of());
        LogicalPlan plan = new Limit(SRC, intLiteral(0), aggregate);

        assertEquals("an aggregate below the zero limit still consumes rows", Set.of(), SchemaOnlyPathExtractor.pathsReadingNoRows(plan));
    }

    /**
     * INLINESTATS wraps its aggregate as a child, so it consumes every row to compute the values it appends —
     * the outer zero limit discards the appended rows, not the scan. The class clears the flag for it by name;
     * without this case, removing that clause leaves every other test green.
     */
    public void testInlineStatsUnderLimitZeroStillReadsRows() {
        Aggregate aggregate = new Aggregate(SRC, externalRelation(PATH), List.of(), List.<NamedExpression>of());
        LogicalPlan plan = new Limit(SRC, intLiteral(0), new InlineStats(SRC, aggregate));

        assertEquals("INLINESTATS below the zero limit still consumes rows", Set.of(), SchemaOnlyPathExtractor.pathsReadingNoRows(plan));
    }

    public void testInnerLimitZeroReadsNoRowsRegardlessOfOuterLimit() {
        Limit inner = new Limit(SRC, intLiteral(0), externalRelation(PATH));
        LogicalPlan plan = new Limit(SRC, intLiteral(10), inner);

        assertEquals(Set.of(PATH), SchemaOnlyPathExtractor.pathsReadingNoRows(plan));
    }

    public void testPathReadByAnyBranchIsNotSchemaOnly() {
        // One resolution feeds every branch, so a path whose rows one arm consumes must resolve as
        // a reading query even though the other arm discards them. This is the case that makes the
        // extractor subtract rather than union.
        Fork fork = new Fork(
            SRC,
            List.of(new Limit(SRC, intLiteral(0), externalRelation(PATH)), new Limit(SRC, intLiteral(5), externalRelation(PATH))),
            List.of()
        );

        assertEquals("a path read by one arm is not schema-only", Set.of(), SchemaOnlyPathExtractor.pathsReadingNoRows(fork));
    }

    public void testEachPathDecidedIndependently() {
        Fork fork = new Fork(
            SRC,
            List.of(new Limit(SRC, intLiteral(0), externalRelation(PATH)), new Limit(SRC, intLiteral(5), externalRelation(OTHER))),
            List.of()
        );

        assertEquals(Set.of(PATH), SchemaOnlyPathExtractor.pathsReadingNoRows(fork));
    }

    public void testNonLiteralPathIsOmittedRatherThanThrowing() {
        UnresolvedExternalRelation relation = new UnresolvedExternalRelation(SRC, unresolved("p"), Map.of());
        LogicalPlan plan = new Limit(SRC, intLiteral(0), relation);

        assertEquals(Set.of(), SchemaOnlyPathExtractor.pathsReadingNoRows(plan));
    }

    private static UnresolvedExternalRelation externalRelation(String path) {
        return new UnresolvedExternalRelation(SRC, Literal.keyword(SRC, path), Map.of());
    }

    private static UnresolvedAttribute unresolved(String name) {
        return new UnresolvedAttribute(SRC, name);
    }

    private static Expression intLiteral(int value) {
        return new Literal(SRC, value, DataType.INTEGER);
    }
}
