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
import org.elasticsearch.test.junit.annotations.TestLogging;
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
@TestLogging(value = "org.elasticsearch.xpack.esql:TRACE", reason = "debug")
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

    public void testKnnLimitAppendedInNestedUnionAllBranchIsBranchOrderIndependent() {
        // validate the fix to PushLimitToKnn
        runGoldenTest("""
                FROM (FROM (FROM colors | LIMIT 10),
                     (FROM colors METADATA _score | WHERE knn(rgb_vector, "007800"))
                 METADATA _score),
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

    // nested fork, views, datasets

    public void testForksInsideAndAfterSubquery() {
        runGoldenTest("""
            FROM (FROM employees
                  | FORK (WHERE emp_no > 10000) (WHERE emp_no <= 10000)),
                 (FROM languages | EVAL emp_no = language_code | KEEP emp_no)
            | FORK (WHERE emp_no > 0) (WHERE emp_no <= 0)
            """, STAGES);
    }

    public void testForkAfterSubquery() {
        runGoldenTest("""
            FROM employees, (FROM employees_incompatible
                                 | WHERE languages > 0
                                 | EVAL emp_no = emp_no::int
                                 | KEEP emp_no)
            | FORK (WHERE emp_no > 10000) (WHERE emp_no <= 10000)
            | KEEP emp_no
            """, STAGES);
    }

    public void testForkAfterUnionTypeConversionAfterSubquery() {
        runGoldenTest("""
            FROM employees_incompatible, (FROM employees
                                           | MV_EXPAND job_positions
                                           | KEEP emp_no, first_name, last_name, job_positions)
            | EVAL emp_no = emp_no::long,
                   first_name = first_name::keyword,
                   last_name = last_name::keyword,
                   job_positions = job_positions::keyword
            | KEEP emp_no, first_name, last_name, job_positions
            | FORK (WHERE true | LIMIT 300) (WHERE true)
            | LIMIT 300
            | WHERE _fork == "fork1"
            | DROP _fork
            """, STAGES);
    }

    public void testForkAfterRenameAndDateDateNanosImplicitCastingAndSubquery() {
        runGoldenTest("""
            FROM (ROW ts = TO_DATETIME("2023-01-01T00:00:00Z")),
                 (ROW ts = TO_DATE_NANOS("2023-01-01T00:00:00.123456789Z"))
            | RENAME ts AS x
            | FORK (WHERE true) (WHERE true)
            | KEEP x
            """, STAGES);
    }

    public void testForkAfterRenameAndCounterTypeAfterSubquery() {
        runGoldenTest("""
            FROM k8s-downsampled, (ROW other = 1)
            | KEEP network.total_bytes_in
            | RENAME network.total_bytes_in AS x, x AS y
            | RENAME y AS z
            | FORK (WHERE true) (WHERE true)
            | KEEP z
            """, STAGES);
    }

    public void testForkOutputUpdatedAfterConflictingTypesInSubquery() {
        runGoldenTest("""
            FROM (ROW x = 1), (ROW x = "abc")
            | FORK (WHERE true) (WHERE true)
            | KEEP x
            """, STAGES);
    }

    public void testForkOutputUpdatedAfterConflictingTypesFromExternalDatasetSubqueries() {
        salariesExternalDatasetBuilder("""
            FROM (FROM salaries_int), (FROM salaries_long)
            | KEEP salary
            | FORK (WHERE true) (WHERE true)
            | KEEP salary
            """).run();
    }

    public void testSubQueryInsideView() {
        runGoldenTest(
            "FROM subquery_view",
            STAGES,
            Map.of("subquery_view", "FROM (FROM employees | LIMIT 10), (FROM employees | LIMIT 20)")
        );
    }

    public void testSubqueryInsideViewReferencedBySubquery() {
        runGoldenTest(
            "FROM (FROM subquery_view | LIMIT 10), (FROM employees | LIMIT 10)",
            STAGES,
            Map.of("subquery_view", "FROM (FROM employees | LIMIT 10), (FROM employees | LIMIT 20)")
        );
    }

    public void testNestedViewsWithSubquery() {
        runGoldenTest(
            "FROM outer_view",
            STAGES,
            Map.of(
                "subquery_view",
                "FROM (FROM employees | LIMIT 10), (FROM employees | LIMIT 20)",
                "outer_view",
                "FROM subquery_view | LIMIT 5"
            )
        );
    }

    public void testForkAfterViewInSubquery() {
        runGoldenTest(
            "FROM (FROM view_0 | FORK (WHERE emp_no > 0) (WHERE emp_no <= 0)), (FROM employees)",
            STAGES,
            Map.of("view_0", "FROM employees")
        );
    }

    public void testForkAfterMultipleViews() {
        runGoldenTest(
            "FROM view_0, view_1 | FORK (WHERE emp_no > 0) (WHERE emp_no <= 0)",
            STAGES,
            Map.of("view_0", "FROM employees", "view_1", "FROM employees")
        );
    }

    public void testSubqueryReferencingMultipleDatasets() {
        runNestedHeavyGoldenTest("FROM (FROM heavy_a, heavy_b), (FROM heavy_b) | WHERE emp_no > 10");
    }

    public void testInSubqueryReferencingMultipleDatasets() {
        runNestedHeavyGoldenTest("FROM heavy_a | WHERE emp_no IN (FROM heavy_a, heavy_b | KEEP emp_no)");
    }

    public void testViewsReferencingMultipleDatasets() {
        runNestedHeavyGoldenTest("FROM view_datasets | WHERE salary > 1000", Map.of("view_datasets", "FROM heavy_a, heavy_b"));
    }

    public void testForkAfterMultipleDatasets() {
        runNestedHeavyGoldenTest("FROM heavy_a, heavy_b | FORK (WHERE emp_no > 10) (WHERE emp_no <= 10)");
    }

    public void testForkInsideSubqueryReferencingMultipleDatasets() {
        runNestedHeavyGoldenTest("""
            FROM (FROM heavy_a, heavy_b | FORK (WHERE emp_no > 10) (WHERE emp_no <= 10)),
                 (FROM heavy_a, heavy_b)
            """);
    }

    public void testUnionAllOfViewAndSubqueryWithFork() {
        runNestedHeavyGoldenTest("""
            FROM view_datasets,
                 (FROM heavy_a, heavy_b | WHERE salary > 1000)
            | FORK (WHERE emp_no > 10) (WHERE emp_no <= 10)
            """, Map.of("view_datasets", "FROM heavy_a, heavy_b"));
    }

    // explicit casting, synthetic conversion attributes across nested merge boundaries, validate the fix to
    // carryOverSyntheticAttributesThroughProjects in ResolveUnionTypesInUnionAll

    public void testConversionInsideInnerUnions() {
        runGoldenTest("""
            FROM (FROM
                    (ROW client_ip = "172.21.0.5"),
                    (ROW client_ip = "172.21.3.15")
                  | EVAL client_ip = client_ip::ip
                  | EVAL _subquery = 1),
                 (FROM
                    (ROW client_ip = "172.21.0.5"),
                    (ROW client_ip = "172.21.3.15")
                  | EVAL client_ip = client_ip::ip
                  | EVAL _subquery = 2)
            | WHERE _subquery == 1
            | DROP _subquery
            | LIMIT 10
            """, STAGES);
    }

    public void testConversionInlineStatsInsideInnerUnions() {
        runGoldenTest("""
            FROM (FROM
                    (ROW client_ip = "172.21.0.5"),
                    (ROW client_ip = "172.21.3.15")
                  | EVAL client_ip = client_ip::ip
                  | INLINE STATS cnt = COUNT(*) BY client_ip
                  | EVAL _subquery = 1),
                 (FROM
                    (ROW client_ip = "172.21.0.5"),
                    (ROW client_ip = "172.21.3.15")
                  | EVAL client_ip = client_ip::ip
                  | INLINE STATS cnt = COUNT(*) BY client_ip
                  | EVAL _subquery = 2)
            | WHERE _subquery == 1
            | DROP _subquery
            | LIMIT 10
            """, STAGES);
    }

    public void testConversionStatsInsideInnerUnions() {
        runGoldenTest("""
            FROM (FROM
                    (ROW client_ip = "172.21.0.5"),
                    (ROW client_ip = "172.21.3.15")
                  | EVAL client_ip = client_ip::ip
                  | STATS cnt = COUNT(*) BY client_ip
                  | EVAL _subquery = 1),
                 (FROM
                    (ROW client_ip = "172.21.0.5"),
                    (ROW client_ip = "172.21.3.15")
                  | EVAL client_ip = client_ip::ip
                  | STATS cnt = COUNT(*) BY client_ip
                  | EVAL _subquery = 2)
            | WHERE _subquery == 1
            | DROP _subquery
            | LIMIT 10
            """, STAGES);
    }

    public void testConversionInlineStatsInOneInnerUnion() {
        runGoldenTest("""
            FROM (FROM
                    (ROW client_ip = "172.21.0.5"),
                    (ROW client_ip = "172.21.3.15")
                  | EVAL client_ip = client_ip::ip
                  | INLINE STATS cnt = COUNT(*) BY client_ip),
                 (ROW client_ip = TO_IP("172.21.2.162"), cnt = 1::long)
            | LIMIT 10
            """, STAGES);
    }

    public void testConversionInlineStatsInInnerUnionsAndInMainQuery() {
        runGoldenTest("""
            FROM (FROM
                    (ROW client_ip = "172.21.0.5"),
                    (ROW client_ip = "172.21.3.15")
                  | EVAL client_ip = client_ip::ip
                  | INLINE STATS cnt = COUNT(*) BY client_ip),
                 (FROM
                    (ROW client_ip = "172.21.0.5"),
                    (ROW client_ip = "172.21.3.15")
                  | EVAL client_ip = client_ip::ip
                  | INLINE STATS cnt = COUNT(*) BY client_ip)
            | EVAL client_ip = client_ip::string
            | KEEP client_ip, cnt
            | LIMIT 10
            """, STAGES);
    }

    public void testConversionInMainQueryAfterNestedUnions() {
        runGoldenTest("""
            FROM (FROM
                    (FROM
                       (ROW client_ip = "172.21.0.5"),
                       (ROW client_ip = "172.21.3.15")
                     | EVAL label = 1),
                    (ROW client_ip = "172.21.2.162", label = 1)),
                 (ROW client_ip = "172.21.2.162", label = 1)
            | EVAL client_ip = client_ip::ip
            | KEEP client_ip
            | LIMIT 10
            """, STAGES);
    }

    public void testMultipleConversionsInMainQueryAfterNestedUnions() {
        runGoldenTest("""
            FROM (FROM (ROW value = "1"), (ROW value = "2")), (ROW value = "3")
            | KEEP value
            | EVAL l = value::long, d = value::double
            | KEEP l, d
            | LIMIT 10
            """, STAGES);
    }

    public void testSameConversionTwiceOverTwoLevelNestedSubquery() {
        runGoldenTest("""
            FROM (FROM (FROM sample_data), (FROM sample_data)),
                 (FROM sample_data)
            | WHERE TO_STRING(client_ip) IS NOT NULL AND NOT TO_STRING(client_ip) == "L2"
            """, STAGES);
    }

    public void testSameConversionTwiceOverOneAndTwoLevelNestedSubquery() {
        runGoldenTest("""
            FROM (FROM (FROM sample_data), (FROM sample_data)
                  | WHERE TO_STRING(client_ip) IS NOT NULL AND NOT TO_STRING(client_ip) == "L2"),
                 (FROM sample_data)
            | WHERE TO_STRING(client_ip) IS NOT NULL AND NOT TO_STRING(client_ip) == "L2"
            """, STAGES);
    }

    public void testSameConversionTwiceOverViewAndSubquery() {
        builder("""
            FROM (FROM sample_view), (FROM sample_data)
            | WHERE TO_STRING(client_ip) IS NOT NULL AND NOT TO_STRING(client_ip) == "L2"
            """).stages(STAGES).views(Map.of("sample_view", "FROM sample_data")).run();
    }

    public void testConversionInMainQueryWithRenameKeepInMainAndNestedSubquery() {
        runGoldenTest("""
            FROM (FROM (FROM (ROW client_ip = "172.21.0.5"), (ROW client_ip = "172.21.3.15")
                        | EVAL label = 1
                        | KEEP client_ip, label
                        | RENAME label AS renamed
                        | RENAME renamed AS label),
                       (ROW client_ip = "172.21.2.162", label = 1)
                  | KEEP client_ip, label
                  | RENAME label AS renamed
                  | RENAME renamed AS label),
                 (ROW client_ip = "172.21.2.162", label = 1)
            | KEEP client_ip, label
            | RENAME label AS renamed
            | RENAME renamed AS label
            | EVAL client_ip = client_ip::ip
            | KEEP client_ip
            | LIMIT 10
            """, STAGES);
    }

    public void testConversionInlineStatsForkAfterUnion() {
        runGoldenTest("""
            FROM (ROW client_ip = "172.21.0.5"), (ROW client_ip = "172.21.3.15")
            | EVAL client_ip = client_ip::ip
            | INLINE STATS cnt = COUNT(*) BY client_ip
            | FORK (WHERE cnt > 0) (WHERE cnt > 1)
            | KEEP client_ip, cnt
            | LIMIT 10
            """, STAGES);
    }

    public void testSameConversionTwiceBeforeFork() {
        runGoldenTest("""
            FROM (FROM sample_data), (FROM sample_data)
            | WHERE TO_STRING(client_ip) IS NOT NULL AND NOT TO_STRING(client_ip) == "L2"
            | FORK (WHERE client_ip == "172.21.0.5") (WHERE client_ip != "172.21.0.5")
            """, STAGES);
    }

    public void testSameConversionTwiceAfterFork() {
        runGoldenTest("""
            FROM (FROM sample_data), (FROM sample_data)
            | FORK (WHERE client_ip == "172.21.0.5") (WHERE client_ip != "172.21.0.5")
            | WHERE TO_STRING(client_ip) IS NOT NULL AND NOT TO_STRING(client_ip) == "L2"
            """, STAGES);
    }

    public void testSameConversionResolvedOnLaterPassOverOneLevelNestedSubquery() {
        builder("""
            SET unmapped_fields="nullify";
            FROM (FROM (FROM employees), (FROM employees, employees_incompatible)
                  | WHERE TO_STRING(emp_no) == "10001" OR does_not_exist IS NOT NULL
                  | EVAL e = TO_STRING(emp_no)
                  | KEEP e),
                 (FROM employees
                  | EVAL e = TO_STRING(emp_no)
                  | KEEP e)
            """).stages(STAGES).expectationChangesAt(COMPACT_MULTI_TYPE_ES_FIELD).run();
    }

    public void testSameConversionResolvedOnLaterPassOverTwoLevelNestedSubquery() {
        builder("""
            SET unmapped_fields="nullify";
            FROM (FROM (FROM employees), (FROM employees, employees_incompatible)
                  | EVAL emp_no = TO_STRING(emp_no)),
                 (FROM employees)
            | WHERE TO_STRING(emp_no) == "10001" OR does_not_exist IS NOT NULL
            | KEEP emp_no, does_not_exist
            | EVAL e = TO_STRING(emp_no)
            """).stages(STAGES).expectationChangesAt(COMPACT_MULTI_TYPE_ES_FIELD).run();
    }

    public void testSameConversionResolvedOnLaterPassOverViewAndMultiTypedBranch() {
        builder("""
            SET unmapped_fields="nullify";
            FROM (FROM emp_view), (FROM employees, employees_incompatible)
            | WHERE TO_STRING(emp_no) == "10001" OR does_not_exist IS NOT NULL
            | EVAL e = TO_STRING(emp_no)
            | KEEP e
            """).stages(STAGES).views(Map.of("emp_view", "FROM employees")).expectationChangesAt(COMPACT_MULTI_TYPE_ES_FIELD).run();
    }

    public void testSameConversionResolvedOnLaterPassBeforeFork() {
        assumeTrue("Requires FORK", EsqlCapabilities.Cap.FORK_V9.isEnabled());
        builder("""
            SET unmapped_fields="nullify";
            FROM (FROM employees), (FROM employees, employees_incompatible)
            | WHERE TO_STRING(emp_no) == "10001" OR does_not_exist IS NOT NULL
            | EVAL e = TO_STRING(emp_no)
            | FORK (WHERE e == "10001") (WHERE e != "10001" OR e IS NULL)
            | KEEP _fork, e
            """).stages(STAGES).expectationChangesAt(COMPACT_MULTI_TYPE_ES_FIELD).run();
    }

    public void testDifferentConversionWithSameNameResolvedOnLaterPassOverNestedSubquery() {
        builder("""
            SET unmapped_fields="nullify";
            FROM (FROM (FROM sample_data), (FROM sample_data)),
                 (FROM sample_data)
            | WHERE TO_DOUBLE(event_duration) > 0 OR does_not_exist IS NOT NULL
            | EVAL d = TO_DEGREES(event_duration)
            """).stages(STAGES).run();
    }

    // implicit casting for date and date_nanos with nested subquery, fork and view, validate alignMergeOutputTypes

    public void testOuterUnionRefreshesTimestampTypeAfterInnerDateNanosCast() {
        runGoldenTest("""
            FROM (
              FROM sample_data,
                   (FROM sample_data_ts_nanos, (FROM sample_data))
              | STATS c = COUNT(*) BY @timestamp
              | SORT @timestamp
              | LIMIT 2
              | EVAL _subquery = 1
            ), (
              FROM sample_data,
                   (FROM sample_data_ts_nanos, (FROM sample_data))
              | STATS c = COUNT(*) BY @timestamp
              | SORT @timestamp
              | LIMIT 2
              | EVAL _subquery = 2
            )
            | WHERE _subquery == 1
            | DROP _subquery
            """, STAGES);
    }

    public void testImplicitDateNanosCastThenLaterPassExplicitToDateNanos() {
        runGoldenTest("""
            SET unmapped_fields="nullify";
            FROM sample_data, (FROM sample_data_ts_nanos)
            | WHERE TO_DATE_NANOS(@timestamp) > "2023-10-23T13:00:00Z"::date_nanos OR does_not_exist IS NOT NULL
            | EVAL t = TO_DATE_NANOS(@timestamp)
            | KEEP t
            | SORT t
            """, STAGES);
    }

    public void testImplicitDateNanosCastWithRenameAndFork() {
        runGoldenTest("""
            FROM sample_data, (FROM sample_data_ts_nanos)
            | RENAME @timestamp AS ts
            | FORK (WHERE true) (WHERE true)
            | KEEP ts, _fork
            | SORT ts, _fork
            """, STAGES);
    }

    public void testImplicitDateNanosCastWithForkAndRename() {
        runGoldenTest("""
            FROM sample_data, (FROM sample_data_ts_nanos)
            | FORK (WHERE true) (WHERE true)
            | RENAME @timestamp AS ts
            | KEEP ts, _fork
            | SORT ts, _fork
            """, STAGES);
    }

    public void testImplicitDateNanosCastThenLaterPassWithRenameAndFork() {
        runGoldenTest("""
            SET unmapped_fields="nullify";
            FROM sample_data, (FROM sample_data_ts_nanos)
            | WHERE TO_DATE_NANOS(@timestamp) > "2023-10-23T13:00:00Z"::date_nanos OR does_not_exist IS NOT NULL
            | RENAME @timestamp AS ts
            | FORK (WHERE true) (WHERE true)
            | KEEP ts, _fork
            | SORT ts, _fork
            """, STAGES);
    }

    public void testNestedImplicitDateNanosCastThenLaterPassWithOuterUnionAndStats() {
        runGoldenTest("""
            SET unmapped_fields="nullify";
            FROM sample_data, (
              FROM sample_data, (FROM sample_data_ts_nanos)
              | WHERE TO_DATE_NANOS(@timestamp) > "2023-10-23T12:00:00Z"::date_nanos OR does_not_exist IS NOT NULL
            )
            | WHERE TO_DATE_NANOS(@timestamp) > "2023-10-23T13:00:00Z"::date_nanos OR does_not_exist IS NOT NULL
            | STATS c = COUNT(*) BY @timestamp
            | SORT @timestamp
            | LIMIT 10
            """, STAGES);
    }

    public void testNestedUnmappedBetweenLevelsImplicitCastThenOuterRenameAndFork() {
        runGoldenTest("""
            SET unmapped_fields="nullify";
            FROM sample_data, (
              FROM sample_data, (FROM sample_data_ts_nanos)
              | WHERE @timestamp IS NOT NULL OR missing_inner IS NOT NULL
            )
            | WHERE @timestamp IS NOT NULL OR missing_outer IS NOT NULL
            | RENAME @timestamp AS ts
            | FORK (WHERE ts IS NOT NULL) (WHERE ts IS NULL)
            | KEEP ts, _fork
            | SORT ts, _fork
            """, STAGES);
    }

    public void testNestedUnmappedBetweenLevelsInnerStringConvertThenOuterImplicitCast() {
        runGoldenTest("""
            SET unmapped_fields="nullify";
            FROM sample_data, (
              FROM sample_data, (FROM sample_data_ts_nanos)
              | WHERE TO_STRING(message) IS NOT NULL OR missing_inner IS NOT NULL
              | EVAL msg = TO_STRING(message)
              | KEEP @timestamp, msg
            )
            | WHERE @timestamp IS NOT NULL OR missing_outer IS NOT NULL
            | KEEP @timestamp, msg
            | SORT @timestamp
            """, STAGES);
    }

    public void testNestedUnmappedBetweenLevelsViewAndOuterStats() {
        builder("""
            SET unmapped_fields="nullify";
            FROM ts_view, (
              FROM sample_data, (FROM sample_data_ts_nanos)
              | WHERE @timestamp IS NOT NULL OR missing_inner IS NOT NULL
            )
            | WHERE @timestamp IS NOT NULL OR missing_outer IS NOT NULL
            | STATS c = COUNT(*) BY @timestamp
            | SORT @timestamp
            | LIMIT 10
            """).stages(STAGES).views(Map.of("ts_view", "FROM sample_data")).run();
    }

    public void testForkAfterImplicitDateNanosCastInNestedSubqueries() {
        runGoldenTest("""
            FROM sample_data, (FROM sample_data_ts_nanos, (FROM sample_data))
            | EVAL t = @timestamp
            | FORK (WHERE event_duration > 0) (WHERE event_duration <= 0)
            | RENAME t AS x, x AS y
            | SORT y
            | KEEP y, @timestamp, _fork
            """, STAGES);
    }

    public void testForkInsideNestedSubqueryWithImplicitDateNanosCast() {
        runGoldenTest("""
            FROM sample_data,
                 (FROM sample_data, (FROM sample_data_ts_nanos | FORK (WHERE true) (WHERE true)))
            | KEEP @timestamp
            """, STAGES);
    }

    public void testForkAfterUnionOfForkBranchesWithImplicitDateNanosCast() {
        runGoldenTest("""
            FROM (FROM sample_data | FORK (WHERE true) (WHERE true)),
                 (FROM sample_data_ts_nanos | FORK (WHERE true) (WHERE true))
            | FORK (WHERE true) (WHERE true)
            | KEEP @timestamp
            """, STAGES);
    }

    public void testForkStatsByImplicitDateNanosCastColumn() {
        runGoldenTest("""
            FROM sample_data, (FROM sample_data_ts_nanos, (FROM sample_data))
            | FORK (STATS c = COUNT(*) BY @timestamp) (STATS c = COUNT(*) BY @timestamp)
            """, STAGES);
    }

    public void testExplicitConversionsWithImplicitDateNanosCastAndFork() {
        runGoldenTest("""
            FROM sample_data, (FROM sample_data_ts_nanos, (FROM sample_data))
            | EVAL s = TO_STRING(@timestamp), l = TO_LONG(@timestamp)
            | FORK (WHERE l > 0) (WHERE l <= 0)
            | KEEP s, l, @timestamp
            """, STAGES);
    }

    public void testSameConversionResolvedOnLaterPassWithImplicitDateNanosCastAndFork() {
        runGoldenTest("""
            SET unmapped_fields="nullify";
            FROM sample_data, (FROM sample_data_ts_nanos, (FROM sample_data))
            | WHERE TO_STRING(@timestamp) IS NOT NULL OR does_not_exist IS NULL
            | EVAL s = TO_STRING(@timestamp)
            | FORK (WHERE true) (WHERE true)
            | KEEP s, @timestamp
            """, STAGES);
    }

    public void testImplicitDateNanosCastOverViewsInNestedSubqueries() {
        assumeTrue("Requires FORK", EsqlCapabilities.Cap.FORK_V9.isEnabled());
        runGoldenTest("""
            FROM sample_data, (FROM nanos_view, (FROM mixed_view))
            | FORK (WHERE true) (WHERE true)
            | KEEP @timestamp
            """, STAGES, Map.of("nanos_view", "FROM sample_data_ts_nanos", "mixed_view", "FROM sample_data, (FROM sample_data_ts_nanos)"));
    }

    // implicit casting field referenced in functions, test fix to collectAliasesNeedingTypeUpdate
    public void testStatsMaxKeepOverImplicitDateNanosCast() {
        runGoldenTest("""
            FROM sample_data, (FROM sample_data_ts_nanos)
            | STATS m = MAX(@timestamp)
            | KEEP m
            """, STAGES);
    }

    public void testStatsBucketOverImplicitDateNanosCast() {
        runGoldenTest("""
            FROM sample_data, (FROM sample_data_ts_nanos)
            | STATS c = COUNT(*) BY m = BUCKET(@timestamp, 1 hour)
            """, STAGES);
    }

    public void testEvalDateTruncKeepOverImplicitDateNanosCast() {
        runGoldenTest("""
            FROM sample_data, (FROM sample_data_ts_nanos)
            | EVAL x = DATE_TRUNC(1 hour, @timestamp)
            | KEEP x
            """, STAGES);
    }

    public void testStatsMaxRenameOverImplicitDateNanosCast() {
        runGoldenTest("""
            FROM sample_data, (FROM sample_data_ts_nanos)
            | STATS m = MAX(@timestamp)
            | RENAME m AS x
            """, STAGES);
    }

    // helpers

    private static final String COMPACT_MULTI_TYPE_ES_FIELD = "compact_multi_type_es_field";

    private static final String RESOURCE_A = "s3://bucket/heavy_a.parquet";
    private static final String RESOURCE_B = "s3://bucket/heavy_b.parquet";
    private static final String SALARIES_INT_RESOURCE = "s3://bucket/salaries_int.parquet";
    private static final String SALARIES_LONG_RESOURCE = "s3://bucket/salaries_long.parquet";

    private void runNestedHeavyGoldenTest(String query) {
        assumeTrue("Requires external data source FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());
        builder(query).stages(STAGES)
            .datasetMetadata(heavyDatasetMetadata())
            .externalSourceResolution(heavyExternalSourceResolution())
            .run();
    }

    private void runNestedHeavyGoldenTest(String query, Map<String, String> views) {
        assumeTrue("Requires external data source FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());
        builder(query).stages(STAGES)
            .datasetMetadata(heavyDatasetMetadata())
            .externalSourceResolution(heavyExternalSourceResolution())
            .views(views)
            .run();
    }

    /**
     * Golden builder for {@code salaries_int}/{@code salaries_long} dataset subqueries. Those datasets share
     * {@code emp_no}/{@code name} but type {@code salary} as integer vs long, so a union of the two produces an
     * {@code UNSUPPORTED} salary column.
     */
    private TestBuilder salariesExternalDatasetBuilder(String query) {
        assumeTrue("Requires external data source FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());
        return builder(query).stages(STAGES)
            .datasetMetadata(salariesDatasetMetadata())
            .externalSourceResolution(salariesExternalSourceResolution());
    }

    private static ProjectMetadata salariesDatasetMetadata() {
        DataSource dataSource = new DataSource("external_ds", "test", null, Map.of());
        Dataset intDataset = new Dataset("salaries_int", new DataSourceReference("external_ds"), SALARIES_INT_RESOURCE, null, Map.of());
        Dataset longDataset = new Dataset("salaries_long", new DataSourceReference("external_ds"), SALARIES_LONG_RESOURCE, null, Map.of());
        return ProjectMetadata.builder(ProjectId.DEFAULT)
            .putCustom(DataSourceMetadata.TYPE, new DataSourceMetadata(Map.of("external_ds", dataSource)))
            .datasets(Map.of("salaries_int", intDataset, "salaries_long", longDataset))
            .build();
    }

    private static ExternalSourceResolution salariesExternalSourceResolution() {
        return new ExternalSourceResolution(
            Map.of(
                SALARIES_INT_RESOURCE,
                salariesSource(SALARIES_INT_RESOURCE, DataType.INTEGER),
                SALARIES_LONG_RESOURCE,
                salariesSource(SALARIES_LONG_RESOURCE, DataType.LONG)
            )
        );
    }

    private static ExternalSourceResolution.ResolvedSource salariesSource(String path, DataType salaryType) {
        List<Attribute> schema = List.of(
            referenceAttribute("emp_no", DataType.INTEGER),
            referenceAttribute("name", DataType.KEYWORD),
            referenceAttribute("salary", salaryType)
        );
        ExternalSourceMetadata metadata = new ExternalSourceMetadata() {
            @Override
            public String location() {
                return path;
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
        return new ExternalSourceResolution.ResolvedSource(metadata, FileList.UNRESOLVED, Map.of());
    }

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
