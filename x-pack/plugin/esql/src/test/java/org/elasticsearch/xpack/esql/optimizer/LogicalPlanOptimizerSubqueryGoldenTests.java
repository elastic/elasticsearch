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
 * Captures the analyzed and logically-optimized plans for subquery-in-{@code FROM} scenarios.
 * Negative tests live in {@code LogicalPlanOptimizerSubqueryTests}.
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

    public void testMatchAfterSubquerySortWithoutLimit() {
        runGoldenTest("""
            FROM (FROM employees | SORT first_name), (FROM employees | WHERE emp_no > 0)
            | WHERE match(first_name, "Meditation")
            """, STAGES);
    }

    public void testMatchOperatorAfterSubquerySortWithoutLimit() {
        runGoldenTest("""
            FROM (FROM employees | SORT first_name), (FROM employees | WHERE emp_no > 0)
            | WHERE first_name:"Meditation"
            """, STAGES);
    }

    public void testMatchPhraseAfterSubquerySortWithoutLimit() {
        runGoldenTest("""
            FROM (FROM employees | SORT first_name), (FROM employees | WHERE emp_no > 0)
            | WHERE match_phrase(first_name, "Meditation")
            """, STAGES);
    }

    /**
     * Verifies the plan shape when a type conversion is applied to a grouping key after STATS above a
     * multi-subquery FROM. The conversion must remain above the Aggregate (reading aggregate output), not
     * be pushed into the UnionAll branches. esql-planning#1987
     */
    public void testConvertGroupKeyAfterStats() {
        runGoldenTest("""
            FROM (FROM employees), (FROM employees)
            | STATS max_salary = MAX(salary) BY gender
            | EVAL g = TO_STRING(gender)
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

    public void testNestedSubqueriesWithMetadata() {
        runGoldenTest("""
            FROM employees,
                 (FROM employees,
                       (FROM employees METADATA _index | WHERE salary > 0)
                       METADATA _index)
                 METADATA _index
            | WHERE emp_no > 10000
            | SORT _index
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

    public void testNestedSubqueriesWithUnionAllOnTopOfMultipleUnionAllsWithMetadata() {
        runGoldenTest("""
            FROM employees,
                 (FROM employees,
                       (FROM languages | WHERE language_code > 0)
                       METADATA _index),
                 (FROM languages,
                       (FROM employees METADATA _index | WHERE salary > 0))
                 METADATA _index
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

    public void testSiblingUnionAllsUnderInSubqueryJoin() {
        runGoldenTest("""
            FROM employees,
                 (FROM employees | WHERE salary > 0)
            | WHERE emp_no IN (FROM employees,
                                    (FROM employees | WHERE languages > 0)
                               | KEEP emp_no)
            """, STAGES);
    }

    // validate sort and knn related changes in PushdownFilterAndLimitIntoUnionAll
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

    public void testUnboundedSortAtAllNestedUnionLevels() {
        runGoldenTest("""
            FROM (FROM (FROM employees | SORT last_name),
                       (FROM employees | SORT first_name)
                  | SORT emp_no),
                 (FROM languages | SORT language_name)
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

    public void testKnnPushedFromMiddleLevelIntoNestedUnionAllBranches() {
        runGoldenTest("""
            FROM (FROM (FROM colors METADATA _score | WHERE knn(rgb_vector, "007800")),
                       (FROM colors METADATA _score | WHERE knn(rgb_vector, "0000ff"))
                  METADATA _score | WHERE knn(rgb_vector, "ff0000")),
                 (FROM colors) METADATA _score
            | LIMIT 5
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

    public void testThreeLevelNestedSubqueriesWithExternalDatasetWithMetadata() {
        runNestedHeavyGoldenTest("""
            FROM employees,
                 (FROM languages,
                       (FROM heavy_a, heavy_b METADATA _index)
                       METADATA _index
                 )
                 METADATA _index
            """);
    }

    // validate the fix to unmapped field resolution (nullify / load) for nested unionall

    public void testNestedSubqueryNullifyWithUnmappedFieldReferencedInMainQueryKeep() {
        runGoldenTest("""
            SET unmapped_fields="nullify";
            FROM employees, (FROM languages, (FROM sample_data))
            | KEEP emp_no, does_not_exist_field
            """, STAGES);
    }

    public void testNestedSubqueryNullifyWithMultipleUnmappedFieldsReferencedInMainQueryKeep() {
        runGoldenTest("""
            SET unmapped_fields="nullify";
            FROM employees, (FROM languages, (FROM sample_data))
            | KEEP emp_no, does_not_exist_field1, does_not_exist_field2
            """, STAGES);
    }

    public void testNestedSubqueryNullifyWithUnmappedFieldReferencedInSubqueryStats() {
        runGoldenTest("""
            SET unmapped_fields="nullify";
            FROM employees, (FROM languages, (FROM sample_data | STATS count(*) BY does_not_exist_field))
            | KEEP emp_no, does_not_exist_field
            """, STAGES);
    }

    public void testNestedSubqueryNullifyWithUnmappedFieldReferencedInMainQueryStats() {
        runGoldenTest("""
            SET unmapped_fields="nullify";
            FROM employees, (FROM languages, (FROM sample_data))
            | STATS c = COUNT(*), emp_max = MAX(emp_no) BY is_null = does_not_exist_field IS NULL
            """, STAGES);
    }

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

    public void testNestedSubqueryNullifyWithRowSourceInSubqueryUnmappedFieldInMainQueryKeep() {
        runGoldenTest("""
            SET unmapped_fields="nullify";
            FROM employees, (FROM languages, (ROW x = 1))
            | KEEP does_not_exist_field, x
            """, STAGES);
    }

    public void testNestedSubqueryLoadWithUnmappedFieldReferencedInMainQueryKeep() {
        runGoldenTest("""
            SET unmapped_fields="load";
            FROM employees, (FROM languages, (FROM sample_data))
            | KEEP emp_no, does_not_exist_field
            """, STAGES);
    }

    public void testNestedSubqueryLoadWithUnmappedFieldReferencedInSubqueryStats() {
        runGoldenTest("""
            SET unmapped_fields="load";
            FROM employees, (FROM languages, (FROM sample_data | STATS count(*) BY does_not_exist_field))
            | KEEP emp_no, does_not_exist_field
            """, STAGES);
    }

    public void testNestedSubqueryLoadWithUnmappedFieldReferencedInMainQueryStats() {
        runGoldenTest("""
            SET unmapped_fields="load";
            FROM employees, (FROM languages, (FROM sample_data))
            | STATS c = COUNT(*), emp_max = MAX(emp_no) BY is_null = does_not_exist_field IS NULL
            """, STAGES);
    }

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

    public void testNineUnionAllSubqueriesInFromCommand() {
        runGoldenTest("""
            FROM employees,
                 (FROM languages),
                 (FROM languages),
                 (FROM languages),
                 (FROM languages),
                 (FROM languages),
                 (FROM languages),
                 (FROM languages),
                 (FROM languages),
                 (FROM languages)
            """, STAGES);
    }

    // -- nested UnionAll + implicit datetime/date_nanos and explicit casting --

    public void testNestedSubqueryImplicitDateAndDateNanosCast() {
        runGoldenTest("""
            FROM sample_data,
                 (FROM sample_data_ts_nanos,
                       (FROM sample_data))
            | KEEP @timestamp
            """, STAGES);
    }

    public void testNestedSubqueryImplicitDateAndDateNanosCastWithTimestampFilter() {
        runGoldenTest("""
            FROM sample_data,
                 (FROM sample_data_ts_nanos,
                       (FROM sample_data))
            | WHERE @timestamp > "2023-10-23T13:00:00Z"
            | KEEP @timestamp
            """, STAGES);
    }

    public void testNestedSubqueryExplicitDateNanosCastOnLongInInnerBranch() {
        runGoldenTest("""
            FROM sample_data,
                 (FROM sample_data_ts_nanos,
                       (FROM sample_data_ts_long | EVAL @timestamp = @timestamp::date_nanos))
            | KEEP @timestamp
            """, STAGES);
    }

    public void testNestedSubqueryExplicitLongCastOnMixedDateAndLong() {
        runGoldenTest("""
            FROM sample_data,
                 (FROM sample_data, (FROM sample_data_ts_long)
                  | EVAL @timestamp = @timestamp::long)
            | EVAL @timestamp = @timestamp::long
            | KEEP @timestamp
            """, STAGES);
    }

    public void testNestedSubqueryRenameAfterImplicitDateAndDateNanosCast() {
        runGoldenTest("""
            FROM sample_data,
                 (FROM sample_data_ts_nanos, (FROM sample_data))
            | KEEP @timestamp
            | RENAME @timestamp AS x
            | KEEP *
            """, STAGES);
    }

    public void testNestedSubqueryRenameAfterExplicitLongCastOnMixedDateAndLong() {
        runGoldenTest("""
            FROM sample_data,
                 (FROM sample_data, (FROM sample_data_ts_long)
                  | EVAL @timestamp = @timestamp::long)
            | EVAL @timestamp = @timestamp::long
            | KEEP @timestamp
            | RENAME @timestamp AS x
            | KEEP *
            """, STAGES);
    }

    // helpers

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
