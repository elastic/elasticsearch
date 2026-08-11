/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

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

import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;

/**
 * Captures the analyzed and logically-optimized plans for nested subquery scenarios.
 */
public class LogicalPlanOptimizerSubqueryGoldenTests extends GoldenTestCase {

    @ParametersFactory(argumentFormatting = "%1$s")
    public static Iterable<Object[]> parameters() {
        return goldenModes();
    }

    public LogicalPlanOptimizerSubqueryGoldenTests(@Name("mode") String mode) {
        super(mode);
    }

    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.ANALYSIS, Stage.LOGICAL_OPTIMIZATION);

    public void testSingleBranchUnionAllIsFlattened() {
        runGoldenTest("""
            FROM employees, (FROM languages)
            | WHERE emp_no > 10000
            """, STAGES);
    }

    public void testMultipleNestedSingleBranchUnionAllsAreFlattened() {
        runGoldenTest("""
            FROM languages,
                 (FROM languages,
                       (FROM languages,
                             (FROM employees | WHERE salary > 0)
                       )
                 )
            | WHERE emp_no > 10000
            """, STAGES);
    }

    public void testNestedSubqueries() {
        runGoldenTest("""
            FROM employees,
                 (FROM employees,
                       (FROM employees | WHERE salary > 0))
            | WHERE emp_no > 10000
            """, STAGES);
    }

    public void testNestedSubqueriesWithUnionAllOnTopOfMultipleUnionAlls() {
        runGoldenTest("""
            FROM employees,
                 (FROM employees,
                       (FROM languages | WHERE language_code > 0)),
                 (FROM languages,
                       (FROM employees | WHERE salary > 0))
            """, STAGES);
    }

    public void testNestedSubqueriesWithUnionAllOnTopOfMultipleUnionAllsWithPredicatePushdown() {
        runGoldenTest("""
            FROM employees,
                 (FROM employees,
                       (FROM languages | WHERE language_code > 0)),
                 (FROM languages,
                       (FROM employees | WHERE salary > 0))
            | WHERE emp_no > 10000
            """, STAGES);
    }

    public void testUnboundedSortInNestedBranchDoesNotLimitTheNestedUnion() {
        runGoldenTest("""
            FROM (FROM (FROM employees | SORT emp_no),
                       (FROM employees | LIMIT 10)
                 ),
                 (FROM languages)
            | STATS c = COUNT(*)
            """, STAGES);
    }

    public void testUnboundedSortInNestedBranchIsBranchOrderIndependent() {
        runGoldenTest("""
            FROM (FROM (FROM employees | LIMIT 10),
                       (FROM employees | SORT emp_no)
                 ),
                 (FROM languages)
            | STATS c = COUNT(*)
            """, STAGES);
    }

    public void testBoundedSortInsideInSubqueryInUnionAllBranch() {
        runGoldenTest("""
            FROM (FROM employees
                  | WHERE emp_no IN (FROM employees | SORT emp_no | LIMIT 5 | KEEP emp_no)
                 ),
                 (FROM languages)
            | STATS c = COUNT(*)
            """, STAGES);
    }

    public void testSiblingUnionAllsUnderInSubqueryJoin() {
        runGoldenTest("""
            FROM employees,
                 (FROM employees | WHERE salary > 0)
            | WHERE emp_no IN (FROM employees,
                                    (FROM employees | WHERE languages > 0)
                               | KEEP emp_no)
            """, STAGES);
    }

    public void testKnnLimitAppendedInNestedUnionAllBranch() {
        runGoldenTest("""
            FROM (FROM (FROM colors METADATA _score | WHERE knn(rgb_vector, "007800")),
                       (FROM colors) METADATA _score),
                 (FROM colors) METADATA _score
            | LIMIT 5
            """, STAGES);
    }

    public void testNoKnnLimitAppendedWhenNestedBranchAlreadyBounded() {
        runGoldenTest("""
            FROM (FROM (FROM colors | LIMIT 5),
                       (FROM colors METADATA _score | WHERE knn(rgb_vector, "007800") | LIMIT 7) METADATA _score),
                 (FROM colors) METADATA _score
            | LIMIT 5
            """, STAGES);
    }

    public void testKnnInsideInSubqueryInUnionAll() {
        runGoldenTest("""
            FROM colors,
                 (FROM colors
                  | WHERE id IN (FROM colors METADATA _score | WHERE knn(rgb_vector, "007800") | KEEP id))
            """, STAGES);
    }

    public void testKnnInsideInSubqueryInNestedUnionAll() {
        runGoldenTest("""
            FROM (FROM (FROM colors | WHERE id IN (FROM colors METADATA _score | WHERE knn(rgb_vector, "007800") | KEEP id)),
                       (FROM colors)),
                 (FROM colors)
            """, STAGES);
    }

    public void testKnnOnUnionBranchLeftOfInSubqueryStillGetsLimit() {
        runGoldenTest("""
            FROM colors,
                 (FROM colors METADATA _score
                  | WHERE knn(rgb_vector, "007800")
                  | WHERE id IN (FROM colors | KEEP id))
            | LIMIT 5
            """, STAGES);
    }

    public void testBoundedKnnInsideInSubqueryKeepsLimitOnJoinRight() {
        runGoldenTest("""
            FROM colors,
                 (FROM colors
                  | WHERE id IN (FROM colors METADATA _score
                                 | WHERE knn(rgb_vector, "007800")
                                 | LIMIT 7
                                 | KEEP id)
                 )
            | STATS c = COUNT(*)
            """, STAGES);
    }

    // -- nested UnionAll + INLINE STATS in the main query --

    public void testNestedSubqueriesWithWhereAndInlineStats() {
        runGoldenTest("""
            FROM employees,
                 (FROM (FROM employees | WHERE salary > 50000),
                       (FROM employees | WHERE emp_no < 10010))
            | INLINE STATS c = COUNT(*)
            """, STAGES);
    }

    public void testNestedSubqueriesWithStatsInsideAndInlineStats() {
        runGoldenTest("""
            FROM employees,
                 (FROM (FROM employees | WHERE emp_no <= 10010 | STATS c1 = COUNT(*)),
                       (FROM employees | WHERE emp_no > 10090 | STATS c2 = COUNT(*)))
            | INLINE STATS total = COUNT(*)
            """, STAGES);
    }

    public void testNestedSubqueriesWithLookupJoinAndInlineStats() {
        runGoldenTest("""
            FROM (FROM (FROM employees
                        | WHERE emp_no <= 10005
                        | EVAL language_code = languages
                        | LOOKUP JOIN languages_lookup ON language_code),
                       (FROM employees | WHERE emp_no > 10095)),
                 (FROM languages)
            | INLINE STATS c = COUNT(*) BY language_name
            """, STAGES);
    }

    public void testNestedSubqueriesWithInlineStatsInsideAndInlineStats() {
        runGoldenTest("""
            FROM employees,
                 (FROM (FROM employees | WHERE emp_no <= 10005 | INLINE STATS max_sal = MAX(salary)),
                       (FROM employees | WHERE emp_no > 10095))
            | INLINE STATS c = COUNT(*)
            """, STAGES);
    }

    // -- nested UnionAll + external dataset + aggregation pushdown --

    public void testNestedSubqueriesWithExternalDatasetWithAggPushdown() {
        runNestedHeavyGoldenTest("""
            FROM employees,
                 (FROM heavy_a, heavy_b)
            | STATS c = COUNT(*), mx = MAX(salary)
            """);
    }

    public void testNestedSubqueriesWithExternalDatasetWithAggPushdownWithGrouping() {
        runNestedHeavyGoldenTest("""
            FROM employees,
                 (FROM heavy_a, heavy_b)
            | STATS c = COUNT(*), mx = MAX(salary) BY dept
            """);
    }

    public void testThreeLevelNestedSubqueriesWithExternalDatasetWithAggPushdown() {
        runNestedHeavyGoldenTest("""
            FROM employees,
                 (FROM languages,
                       (FROM heavy_a, heavy_b)
                 )
            | STATS c = COUNT(*), mx = MAX(salary)
            """);
    }

    // -- nested UnionAll + unmapped field resolution (nullify / load) --

    /**
     * Outer-level truly unmapped field in NULLIFY mode across 3-level nested UnionAll. Verifies that the optimizer
     * correctly handles plans after {@code nullify()} adds null-typed fields to all three EsRelations.
     */
    public void testNestedSubqueryNullifyWithUnmappedFieldReferencedInMainQueryKeep() {
        runGoldenTest("""
            SET unmapped_fields="nullify";
            FROM employees, (FROM languages, (FROM sample_data))
            | KEEP emp_no, does_not_exist_field
            """, STAGES);
    }

    /**
     * ROW source inside a nested subquery combined with a truly unmapped field in NULLIFY mode. Verifies that the
     * optimizer handles plans where {@code nullifyNonEsRelationSources} injected {@code EVAL does_not_exist_field = NULL}
     * atop the inner ROW source.
     */
    public void testNestedSubqueryNullifyWithRowSourceInSubqueryUnmappedFieldInMainQueryKeep() {
        runGoldenTest("""
            SET unmapped_fields="nullify";
            FROM employees, (FROM languages, (ROW x = 1))
            | KEEP does_not_exist_field, x
            """, STAGES);
    }

    /**
     * Outer-level truly unmapped field in LOAD mode across 3-level nested UnionAll. Verifies that the optimizer
     * correctly handles plans after {@code _source} keyword loaders are broadcast into all three EsRelations.
     */
    public void testNestedSubqueryLoadOuterReference() {
        runGoldenTest("""
            SET unmapped_fields="load";
            FROM employees, (FROM languages, (FROM sample_data))
            | KEEP emp_no, does_not_exist_field
            """, STAGES);
    }

    /**
     * STATS aggregation grouping by a null-typed field produced by the NULLIFY pass. Verifies that the
     * optimizer correctly propagates the null-typed attribute through the Aggregate plan node without
     * folding or mistyping it.
     */
    public void testNestedSubqueryNullifyWithUnmappedFieldReferencedInMainQueryStats() {
        runGoldenTest("""
            SET unmapped_fields="nullify";
            FROM employees, (FROM languages, (FROM sample_data))
            | STATS c = COUNT(*), emp_max = MAX(emp_no) BY is_null = does_not_exist_field IS NULL
            """, STAGES);
    }

    /**
     * LOOKUP JOIN inside the innermost nested subquery branch in NULLIFY mode. The outer query references
     * {@code does_not_exist_field} (truly unmapped), which is null-typed by the nullify pass. Verifies that
     * the LOOKUP JOIN node coexists correctly with the null-typed EsRelations and that the optimizer
     * propagates the null-typed attribute correctly.
     */
    public void testNestedSubqueryNullifyWithLookupJoinInSubqueryUnmappedFieldReferencedInMainQueryKeep() {
        runGoldenTest("""
            SET unmapped_fields="nullify";
            FROM employees,
                 (FROM languages,
                       (FROM employees
                        | EVAL language_code = languages
                        | LOOKUP JOIN languages_lookup ON language_code
                        | KEEP emp_no, language_name))
            | KEEP emp_no, does_not_exist_field, language_name
            """, STAGES);
    }

    /**
     * STATS in the main query over a 3-level nested UnionAll in LOAD mode. Verifies that {@code _source}
     * keyword loaders broadcast by the LOAD pass into all three EsRelations are compatible with the
     * subsequent outer STATS aggregation, and that the optimizer correctly handles the plan.
     */
    public void testNestedSubqueryLoadWithUnmappedFieldReferencedInMainQueryStats() {
        runGoldenTest("""
            SET unmapped_fields="load";
            FROM employees, (FROM languages, (FROM sample_data))
            | STATS c = COUNT(*), emp_max = MAX(emp_no) BY has_emp = emp_no IS NOT NULL
            """, STAGES);
    }

    /**
     * LOOKUP JOIN inside the innermost nested subquery combined with an outer-level unmapped field in LOAD
     * mode. Verifies that the optimizer correctly propagates the {@code _source} keyword loaders placed by
     * the analyzer through the nested UnionAll and LOOKUP JOIN node.
     */
    public void testNestedSubqueryLoadWithUnmappedFieldReferencedInSubqueryLookupJoinAndMainQuery() {
        runGoldenTest("""
            SET unmapped_fields="load";
            FROM employees,
                 (FROM languages,
                       (FROM employees
                        | EVAL language_code = languages
                        | LOOKUP JOIN languages_lookup ON language_code
                        | KEEP emp_no, language_name, does_not_exist_field))
            | KEEP emp_no, does_not_exist_field, language_name
            """, STAGES);
    }

    private void runNestedHeavyGoldenTest(String query) {
        assumeTrue("Requires external data source FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());
        builder(query).stages(STAGES)
            .datasetMetadata(heavyDatasetMetadata())
            .externalSourceResolution(heavyExternalSourceResolution())
            .run();
    }

    private static final String RESOURCE_A = "s3://bucket/heavy_a.parquet";
    private static final String RESOURCE_B = "s3://bucket/heavy_b.parquet";

    private static ProjectMetadata heavyDatasetMetadata() {
        DataSource dataSource = new DataSource("heavy_ds", "test", null, Map.of());
        Dataset a = new Dataset("heavy_a", new DataSourceReference("heavy_ds"), RESOURCE_A, null, Map.of());
        Dataset b = new Dataset("heavy_b", new DataSourceReference("heavy_ds"), RESOURCE_B, null, Map.of());
        return ProjectMetadata.builder(ProjectId.DEFAULT)
            .putCustom(DataSourceMetadata.TYPE, new DataSourceMetadata(Map.of("heavy_ds", dataSource)))
            .datasets(Map.of("heavy_a", a, "heavy_b", b))
            .build();
    }

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
