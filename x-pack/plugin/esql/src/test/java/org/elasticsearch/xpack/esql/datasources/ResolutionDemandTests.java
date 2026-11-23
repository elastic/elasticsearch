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
    // The relations below carry an empty config, so this is the spec each of them denotes.
    private static final ExternalSourceSpec SPEC = new ExternalSourceSpec(PATH, Map.of());

    public void testNullStatsSetKeepsLegacyEagerBehaviour() {
        // Callers that pass no query-shape information must resolve exactly as they did before it existed.
        assertEquals(ResolutionDemand.EAGER_STATS, ResolutionDemand.of(SPEC, null, null));
        assertEquals(
            "a null stats set outranks anything else",
            ResolutionDemand.EAGER_STATS,
            ResolutionDemand.of(SPEC, null, Set.of(SPEC))
        );
    }

    public void testPathUnderUngroupedAggregateRequiresStats() {
        ResolutionDemand demand = ResolutionDemand.of(SPEC, Set.of(SPEC), Set.of());
        assertEquals(ResolutionDemand.EAGER_STATS, demand);
        assertTrue(demand.requiresStats());
        assertFalse("an eagerly-aggregated path reads every file, so it cannot be bounded", demand.isSchemaDiscovery());
    }

    public void testPathReadingNoRowsNeedsSchemaDiscoveryOnly() {
        ResolutionDemand demand = ResolutionDemand.of(SPEC, Set.of(), Set.of(SPEC));
        assertEquals(ResolutionDemand.SCHEMA_DISCOVERY, demand);
        assertTrue(demand.isSchemaDiscovery());
        assertFalse(demand.requiresStats());
    }

    public void testPathInNeitherSetReadsRows() {
        ResolutionDemand demand = ResolutionDemand.of(SPEC, Set.of(), Set.of());
        assertEquals(ResolutionDemand.ROWS, demand);
        assertFalse("a reading query must list the whole glob, as before", demand.isSchemaDiscovery());
        assertFalse(demand.requiresStats());
    }

    public void testUnexaminedQueryShapeReadsRows() {
        assertEquals(ResolutionDemand.ROWS, ResolutionDemand.of(SPEC, Set.of(), null));
    }

    /**
     * The exclusivity the enum exists to hold, asserted against the two extractors rather than restated: the
     * shape that puts a path in the stats set is an ungrouped aggregate above it, and an aggregate above a
     * relation is exactly what stops the other extractor calling it schema discovery. If either extractor is changed
     * so that both can claim one path, this goes red and {@link ResolutionDemand#of} has to choose explicitly.
     */
    public void testTheTwoExtractorsCannotClaimTheSameSpec() {
        UnresolvedExternalRelation relation = new UnresolvedExternalRelation(SRC, Literal.keyword(SRC, PATH), Map.of());
        // Each shape names the set it belongs in. Asserting only "not both" would also hold if both extractors
        // regressed to returning nothing, which is the failure this test exists to notice.
        record Shape(String name, LogicalPlan plan, boolean expectStats, boolean expectNoRows) {}
        List<Shape> shapes = List.of(
            new Shape("STATS COUNT(*)", new Aggregate(SRC, relation, List.of(), List.of()), true, false),
            new Shape("FROM ds | LIMIT 0", new Limit(SRC, new Literal(SRC, 0, DataType.INTEGER), relation), false, true),
            new Shape(
                "STATS COUNT(*) | LIMIT 0",
                new Limit(SRC, new Literal(SRC, 0, DataType.INTEGER), new Aggregate(SRC, relation, List.of(), List.of())),
                true,
                false
            ),
            new Shape("FROM ds | LIMIT 10", new Limit(SRC, new Literal(SRC, 10, DataType.INTEGER), relation), false, false)
        );

        for (Shape shape : shapes) {
            Set<ExternalSourceSpec> stats = ExternalStatsRequirementExtractor.specsRequiringEagerStats(shape.plan());
            Set<ExternalSourceSpec> noRows = SchemaDiscoveryPathExtractor.specsReadingNoRows(shape.plan());
            assertEquals(shape.name() + ": eager stats", shape.expectStats(), stats.contains(SPEC));
            assertEquals(shape.name() + ": reads no rows", shape.expectNoRows(), noRows.contains(SPEC));
            assertFalse(shape.name() + ": no plan may put one spec in both sets", stats.contains(SPEC) && noRows.contains(SPEC));
        }
    }

    /**
     * Two specs over one resource, the shape this whole change exists for. {@code of} reads the sets by spec, so a
     * sibling's membership must not answer for this one -- and every other case in this file uses a single spec,
     * where deciding membership by path and by spec agree. Without this, re-spelling {@code contains(spec)} as
     * "any spec over this path is in the set" leaves the suite green while the strict path skips its coercibility
     * check for the spec that reads rows.
     */
    public void testASiblingSpecsDemandIsNotThisSpecs() {
        ExternalSourceSpec other = new ExternalSourceSpec(PATH, Map.of("format", "tsv"));
        assertNotEquals("same path, different configuration, or the cases below assert nothing", SPEC, other);

        assertEquals(
            "a sibling under an ungrouped aggregate does not make this spec eagerly aggregated",
            ResolutionDemand.ROWS,
            ResolutionDemand.of(SPEC, Set.of(other), Set.of())
        );
        assertEquals(
            "a sibling that discards every row does not bound this spec's read",
            ResolutionDemand.ROWS,
            ResolutionDemand.of(SPEC, Set.of(), Set.of(other))
        );

        // Positive controls over the same sets: without them the two cases above would also hold if of() had
        // stopped reading the sets at all.
        assertEquals(ResolutionDemand.EAGER_STATS, ResolutionDemand.of(other, Set.of(other), Set.of()));
        assertEquals(ResolutionDemand.SCHEMA_DISCOVERY, ResolutionDemand.of(other, Set.of(), Set.of(other)));
    }

    /** {@code STATS COUNT(*) | LIMIT 0} consumes every row to produce the one the limit discards. */
    public void testAggregateUnderZeroLimitNeedsMoreThanSchemaDiscovery() {
        UnresolvedExternalRelation relation = new UnresolvedExternalRelation(SRC, Literal.keyword(SRC, PATH), Map.of());
        LogicalPlan plan = new Limit(SRC, new Literal(SRC, 0, DataType.INTEGER), new Aggregate(SRC, relation, List.of(), List.of()));

        // Asserted against the extractor directly as well: this shape is also in the eager-stats set, so of()
        // answers EAGER_STATS — and therefore not schema discovery — even if the schema discovery extractor wrongly
        // claimed the path, which would make the demand assertion alone unable to fail.
        assertFalse(
            "an aggregate below the zero limit consumes every row, so the relation is read",
            SchemaDiscoveryPathExtractor.specsReadingNoRows(plan).contains(SPEC)
        );
        ResolutionDemand demand = ResolutionDemand.of(
            SPEC,
            ExternalStatsRequirementExtractor.specsRequiringEagerStats(plan),
            SchemaDiscoveryPathExtractor.specsReadingNoRows(plan)
        );

        assertFalse("bounding this would answer COUNT(*) from a prefix of the dataset", demand.isSchemaDiscovery());
    }
}
