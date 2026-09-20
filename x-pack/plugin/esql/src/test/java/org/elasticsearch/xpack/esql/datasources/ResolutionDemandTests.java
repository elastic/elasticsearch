/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * How much of a path a query needs. The state decides how much of that path's glob gets listed, so the cases
 * below pin the two directions separately: a path may only be bounded when nothing reads its rows, and the
 * legacy {@code null} set must keep resolving every path eagerly, because that is what every caller that
 * predates query-shape analysis relies on.
 */
public class ResolutionDemandTests extends ESTestCase {

    private static final Source SRC = Source.EMPTY;
    private static final String PATH = "s3://bucket/data/*.parquet";

    public void testNullStatsSetKeepsLegacyEagerBehaviour() {
        // Callers that pass no query-shape information must resolve exactly as they did before it existed.
        assertEquals(ResolutionDemand.EAGER_STATS, ResolutionDemand.of(PATH, null, null));
        assertEquals(
            "a null stats set outranks anything else",
            ResolutionDemand.EAGER_STATS,
            ResolutionDemand.of(PATH, null, Set.of(PATH))
        );
    }

    public void testPathUnderUngroupedAggregateRequiresStats() {
        ResolutionDemand demand = ResolutionDemand.of(PATH, Set.of(PATH), Set.of());
        assertEquals(ResolutionDemand.EAGER_STATS, demand);
        assertTrue(demand.requiresStats());
        assertFalse("an eagerly-aggregated path reads every file, so it cannot be bounded", demand.schemaOnly());
    }

    public void testPathReadingNoRowsIsSchemaOnly() {
        ResolutionDemand demand = ResolutionDemand.of(PATH, Set.of(), Set.of(PATH));
        assertEquals(ResolutionDemand.SCHEMA_ONLY, demand);
        assertTrue(demand.schemaOnly());
        assertFalse(demand.requiresStats());
    }

    public void testPathInNeitherSetReadsRows() {
        ResolutionDemand demand = ResolutionDemand.of(PATH, Set.of(), Set.of());
        assertEquals(ResolutionDemand.ROWS, demand);
        assertFalse("a reading query must list the whole glob, as before", demand.schemaOnly());
        assertFalse(demand.requiresStats());
    }

    public void testUnexaminedQueryShapeReadsRows() {
        assertEquals(ResolutionDemand.ROWS, ResolutionDemand.of(PATH, Set.of(), null));
    }

    /**
     * The exclusivity the enum exists to hold, asserted against the two extractors rather than restated: the
     * shape that puts a path in the stats set is an ungrouped aggregate above it, and an aggregate above a
     * relation is exactly what stops the other extractor calling it schema-only. If either extractor is changed
     * so that both can claim one path, this goes red and {@link ResolutionDemand#of} has to choose explicitly.
     */
    public void testTheTwoExtractorsCannotClaimTheSamePath() {
        UnresolvedExternalRelation relation = new UnresolvedExternalRelation(SRC, Literal.keyword(SRC, PATH), Map.of());
        List<LogicalPlan> shapes = List.of(
            // STATS COUNT(*) — the eager-stats shape.
            new Aggregate(SRC, relation, List.of(), List.of()),
            // FROM ds | LIMIT 0 — the schema-only shape.
            new Limit(SRC, new Literal(SRC, 0, DataType.INTEGER), relation),
            // STATS COUNT(*) | LIMIT 0 — the shape that looks like both and is neither.
            new Limit(SRC, new Literal(SRC, 0, DataType.INTEGER), new Aggregate(SRC, relation, List.of(), List.of())),
            // FROM ds | LIMIT 10 — neither.
            new Limit(SRC, new Literal(SRC, 10, DataType.INTEGER), relation)
        );

        for (LogicalPlan plan : shapes) {
            Set<String> stats = ExternalStatsRequirementExtractor.pathsRequiringEagerStats(plan);
            Set<String> noRows = SchemaOnlyPathExtractor.pathsReadingNoRows(plan);
            assertFalse("no plan may put one path in both sets: " + plan, stats.contains(PATH) && noRows.contains(PATH));
        }
    }

    /** {@code STATS COUNT(*) | LIMIT 0} consumes every row to produce the one the limit discards. */
    public void testAggregateUnderZeroLimitIsNotSchemaOnly() {
        UnresolvedExternalRelation relation = new UnresolvedExternalRelation(SRC, Literal.keyword(SRC, PATH), Map.of());
        LogicalPlan plan = new Limit(SRC, new Literal(SRC, 0, DataType.INTEGER), new Aggregate(SRC, relation, List.of(), List.of()));

        ResolutionDemand demand = ResolutionDemand.of(
            PATH,
            ExternalStatsRequirementExtractor.pathsRequiringEagerStats(plan),
            SchemaOnlyPathExtractor.pathsReadingNoRows(plan)
        );

        assertFalse("bounding this would answer COUNT(*) from a prefix of the dataset", demand.schemaOnly());
    }
}
