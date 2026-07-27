/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.DataSourceReference;
import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolution;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PushAggregateThroughUnionAll;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;

/**
 * Golden (plan) tests for {@link PushAggregateThroughUnionAll}:
 * pushing aggregates through the leaf {@code UnionAll} a heterogeneous {@code FROM} produces.
 *
 * <p>The branches are two external datasets with an <b>identical</b> schema ({@code emp_no}/{@code salary}/{@code dept}).
 * Identical schemas are deliberate: when branch schemas differ, union alignment inserts {@code Eval} nodes for the
 * null-filled columns, which makes the {@code UnionAll} non-leaf ({@code isLeafUnionAll} only accepts
 * {@code (Project) > EsRelation | ExternalRelation}) and the heavy aggregate would not push. Two same-schema datasets
 * give a clean leaf {@code UnionAll} so the pushdown actually fires; the rewrite is identical whether a branch is an ES
 * index or an external relation, so this is representative of the index+dataset case too.
 *
 * <p>Data correctness for these shapes (including the heterogeneous index+dataset case) is covered by csv-spec tests;
 * these tests only snapshot the plan.
 */
public class HeterogeneousFromPushdownGoldenTests extends GoldenTestCase {

    private static final EnumSet<Stage> STAGES = EnumSet.of(
        Stage.LOGICAL_OPTIMIZATION,
        Stage.PHYSICAL_OPTIMIZATION,
        Stage.LOCAL_PHYSICAL_OPTIMIZATION
    );

    private static final String RESOURCE_A = "s3://bucket/heavy_a.parquet";
    private static final String RESOURCE_B = "s3://bucket/heavy_b.parquet";

    /** Intermediate-state path: {@code COUNT_DISTINCT} pushes an HLL sketch ({@code ToPartial}) into each branch. */
    public void testCountDistinctPushed() {
        runHeavyGoldenTest("FROM heavy_a, heavy_b | STATS d = COUNT_DISTINCT(emp_no)");
    }

    /** Grouped intermediate-state path: per-group HLL sketch per branch, merged per group on the coordinator. */
    public void testGroupedCountDistinctPushed() {
        runHeavyGoldenTest("FROM heavy_a, heavy_b | STATS d = COUNT_DISTINCT(emp_no) BY dept");
    }

    /** Intermediate-state path: {@code PERCENTILE} pushes a t-digest ({@code ToPartial}) into each branch. */
    public void testPercentilePushed() {
        runHeavyGoldenTest("FROM heavy_a, heavy_b | STATS p = PERCENTILE(salary, 50)");
    }

    /** Intermediate-state path: {@code STD_DEV} pushes a Welford state ({@code ToPartial}) into each branch. */
    public void testStdDevPushed() {
        runHeavyGoldenTest("FROM heavy_a, heavy_b | STATS s = STD_DEV(salary)");
    }

    /** {@code MEDIAN} is surrogate-substituted to {@code PERCENTILE} before the rule runs, so it pushes transitively. */
    public void testMedianPushedViaPercentile() {
        runHeavyGoldenTest("FROM heavy_a, heavy_b | STATS m = MEDIAN(salary)");
    }

    /** {@code AVG} is surrogate-substituted to {@code SUM}/{@code COUNT}, so it pushes through the algebraic path. */
    public void testAvgPushedViaSumCount() {
        runHeavyGoldenTest("FROM heavy_a, heavy_b | STATS a = AVG(salary) BY dept");
    }

    /** Mixed {@code STATS}: the algebraic {@code COUNT} and the intermediate-state {@code COUNT_DISTINCT} both push. */
    public void testMixedAlgebraicAndHeavyPushed() {
        runHeavyGoldenTest("FROM heavy_a, heavy_b | STATS c = COUNT(*), d = COUNT_DISTINCT(emp_no)");
    }

    /** Algebraic aggregates push even with a per-aggregate filter ({@code ToPartial} is not involved). */
    public void testFilteredAlgebraicPushed() {
        runHeavyGoldenTest("FROM heavy_a, heavy_b | STATS s = SUM(salary) WHERE salary > 0");
    }

    /**
     * Ungrouped {@code STATS} where every aggregate shares one filter: {@code ExtractAggregateCommonFilter} hoists the
     * predicate to a query-level {@code WHERE} before this rule runs. That {@code WHERE} is pushed into the branches,
     * making the {@code UnionAll} non-leaf, so the heavy aggregate stays on the coordinator over the filtered rows.
     */
    public void testFilteredHeavyExtractedToQueryFilter() {
        runHeavyGoldenTest("FROM heavy_a, heavy_b | STATS d = COUNT_DISTINCT(emp_no) WHERE salary > 0");
    }

    /**
     * Ungrouped {@code STATS} mixing an unfiltered {@code COUNT(*)} and a filtered heavy aggregate. The filters differ,
     * so {@code ExtractAggregateCommonFilter} cannot hoist a common {@code WHERE}; the filtered heavy aggregate keeps
     * its per-aggregate filter, which rides on the per-branch {@code ToPartial} while the inner aggregate is unfiltered.
     */
    public void testMixedFilteredHeavyPushed() {
        runHeavyGoldenTest("FROM heavy_a, heavy_b | STATS c = COUNT(*), d = COUNT_DISTINCT(emp_no) WHERE salary > 0");
    }

    /** Grouped + filtered heavy aggregate: the per-group intermediate state is computed over only the matching rows. */
    public void testFilteredGroupedHeavyPushed() {
        runHeavyGoldenTest("FROM heavy_a, heavy_b | STATS d = COUNT_DISTINCT(emp_no) WHERE salary > 0 BY dept");
    }

    /** The filtered intermediate-state path is aggregate-agnostic: a grouped filtered {@code PERCENTILE} pushes too. */
    public void testFilteredGroupedPercentilePushed() {
        runHeavyGoldenTest("FROM heavy_a, heavy_b | STATS p = PERCENTILE(salary, 50) WHERE salary > 0 BY dept");
    }

    /** The filtered intermediate-state path is aggregate-agnostic: a grouped filtered {@code STD_DEV} pushes too. */
    public void testFilteredGroupedStdDevPushed() {
        runHeavyGoldenTest("FROM heavy_a, heavy_b | STATS s = STD_DEV(salary) WHERE salary > 0 BY dept");
    }

    /**
     * A query-level {@code WHERE} (before {@code STATS}) is pushed into the branches, which makes the {@code UnionAll}
     * non-leaf; the heavy aggregate therefore stays on the coordinator over the already-filtered branch rows.
     */
    public void testQueryFilterHeavyNotPushed() {
        runHeavyGoldenTest("FROM heavy_a, heavy_b | WHERE salary > 0 | STATS d = COUNT_DISTINCT(emp_no)");
    }

    /** Two identical heavy aggregates: {@code DeduplicateAggs} runs first; the combined plan must still decompose. */
    public void testDuplicateHeavyPushed() {
        runHeavyGoldenTest("FROM heavy_a, heavy_b | STATS d1 = COUNT_DISTINCT(emp_no), d2 = COUNT_DISTINCT(emp_no)");
    }

    private void runHeavyGoldenTest(String query) {
        assumeTrue("Requires external data source FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());
        builder(query).stages(STAGES)
            .transportVersion(TransportVersion.current())
            .datasetMetadata(heavyDatasetMetadata())
            .externalSourceResolution(heavyExternalSourceResolution())
            .run();
    }

    /** Registers {@code heavy_a} and {@code heavy_b} as external datasets so {@code FROM heavy_a, heavy_b} is a UnionAll. */
    private static ProjectMetadata heavyDatasetMetadata() {
        DataSource dataSource = new DataSource("heavy_ds", "test", null, Map.of());
        Dataset a = new Dataset("heavy_a", new DataSourceReference("heavy_ds"), RESOURCE_A, null, Map.of());
        Dataset b = new Dataset("heavy_b", new DataSourceReference("heavy_ds"), RESOURCE_B, null, Map.of());
        return ProjectMetadata.builder(ProjectId.DEFAULT)
            .putCustom(DataSourceMetadata.TYPE, new DataSourceMetadata(Map.of("heavy_ds", dataSource)))
            .datasets(Map.of("heavy_a", a, "heavy_b", b))
            .build();
    }

    /** Identical {@code emp_no}/{@code salary}/{@code dept} schema for both datasets, so the UnionAll stays leaf. */
    private static ExternalSourceResolution heavyExternalSourceResolution() {
        return new ExternalSourceResolution(
            Map.of(
                RESOURCE_A,
                new ExternalSourceResolution.ResolvedSource(schemaFor(RESOURCE_A), FileList.UNRESOLVED, Map.of()),
                RESOURCE_B,
                new ExternalSourceResolution.ResolvedSource(schemaFor(RESOURCE_B), FileList.UNRESOLVED, Map.of())
            )
        );
    }

    private static ExternalSourceMetadata schemaFor(String resource) {
        List<Attribute> schema = List.of(
            referenceAttribute("emp_no", DataType.INTEGER),
            referenceAttribute("salary", DataType.INTEGER),
            referenceAttribute("dept", DataType.INTEGER)
        );
        return new ExternalSourceMetadata() {
            @Override
            public String location() {
                return resource;
            }

            @Override
            public List<Attribute> schema() {
                return schema;
            }

            @Override
            public String sourceType() {
                return "parquet";
            }
        };
    }
}
