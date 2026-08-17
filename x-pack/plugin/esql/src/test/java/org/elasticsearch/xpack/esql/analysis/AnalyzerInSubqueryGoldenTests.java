/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.cluster.metadata.DataSourceReference;
import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.CompactMultiTypeEsField;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolution;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.expression.function.aggregate.DimensionValues;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;

/**
 * Golden tests for the analyzed plans produced by IN / NOT IN subquery scenarios.
 */
public class AnalyzerInSubqueryGoldenTests extends GoldenTestCase {

    private static final String PACK_DIMS_AGG = "pack_dims_agg";

    @ParametersFactory(argumentFormatting = "%1$s")
    public static Iterable<Object[]> parameters() {
        return goldenModes();
    }

    public AnalyzerInSubqueryGoldenTests(@Name("mode") String mode) {
        super(mode);
    }

    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.ANALYSIS);

    private static final String SALARIES_INT_RESOURCE = "s3://bucket/salaries_int.parquet";
    private static final String SALARIES_LONG_RESOURCE = "s3://bucket/salaries_long.parquet";

    private static void requireExternalDatasetSupport() {
        assumeTrue("Requires external dataset in FROM command support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());
    }

    private static void requireMultiColumnInSubquerySupport() {
        assumeTrue("Requires multi-column IN subquery support", EsqlCapabilities.Cap.WHERE_IN_MULTI_COLUMN_SUBQUERY.isEnabled());
    }

    // -- basic IN subqueries --

    public void testInSubquery() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM employees | KEEP emp_no)
            """, STAGES);
    }

    public void testNotInSubquery() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no NOT IN (FROM employees | KEEP emp_no)
            """, STAGES);
    }

    public void testInSubqueryAndOneMorePredicate() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM employees | KEEP emp_no)
              AND salary > 50000
            """, STAGES);
    }

    public void testInSubqueryAndManyOtherPredicates() {
        runGoldenTest("""
            FROM employees
            | WHERE salary > 50000 AND emp_no IN (FROM employees | KEEP emp_no) AND salary < 100000
            """, STAGES);
    }

    public void testInSubqueryAndInPredicate() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM employees | KEEP emp_no) AND languages IN (1, 2, 3)
            """, STAGES);
    }

    public void testInSubqueryAfterEval() {
        runGoldenTest("""
            FROM employees
            | EVAL x = emp_no + 1
            | WHERE x IN (FROM employees | KEEP emp_no)
            """, STAGES);
    }

    public void testNotInSubqueryAfterEval() {
        runGoldenTest("""
            FROM employees
            | EVAL x = emp_no + 1
            | WHERE x NOT IN (FROM employees | KEEP emp_no)
            """, STAGES);
    }

    public void testInSubqueryAndOtherPredicateAfterEval() {
        runGoldenTest("""
            FROM employees
            | EVAL x = emp_no + 1
            | WHERE x IN (FROM employees | KEEP emp_no)
              AND salary > 50000
            """, STAGES);
    }

    public void testInAndNotInSubqueryAfterEval() {
        runGoldenTest("""
            FROM employees
            | EVAL x = emp_no + 1, y = salary * 2
            | WHERE x IN (FROM employees | KEEP emp_no)
              AND y NOT IN (FROM employees | KEEP salary)
            """, STAGES);
    }

    public void testStatsInsideInSubquery() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM employees | STATS max_emp = max(emp_no))
            """, STAGES);
    }

    public void testStatsInsideNotInSubquery() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no NOT IN (FROM employees | STATS min_emp = min(emp_no))
            """, STAGES);
    }

    public void testStatsByInsideInSubquery() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM employees
                              | STATS max_emp = max(emp_no) BY languages
                              | KEEP max_emp)
            """, STAGES);
    }

    public void testMultipleCommandsInsideInSubquery() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM employees
                              | STATS m = max(emp_no) BY y = date_trunc(1 year, hire_date)
                              | SORT y DESC
                              | LIMIT 5
                              | KEEP m)
            """, STAGES);
    }

    public void testMultipleCommandsAfterInSubquery() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM employees | KEEP emp_no)
            | EVAL doubled = salary * 2
            | WHERE doubled > 100000
            | SORT doubled DESC
            | LIMIT 10
            | KEEP emp_no, doubled
            """, STAGES);
    }

    public void testCommandsAfterNotInSubquery() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no NOT IN (FROM employees | KEEP emp_no)
            | EVAL doubled = salary * 2
            | SORT doubled
            | LIMIT 5
            """, STAGES);
    }

    public void testTwoWhereCommands() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM employees | KEEP emp_no)
            | WHERE salary > 50000
            """, STAGES);
    }

    public void testStatsAfterInSubquery() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM employees | KEEP emp_no)
            | STATS avg_salary = avg(salary) BY languages
            """, STAGES);
    }

    public void testExtraParenthesizedInSubquery() {
        runGoldenTest("""
            FROM employees
            | WHERE (emp_no IN (FROM employees | KEEP emp_no)) AND salary > 50000
            """, STAGES);
    }

    // -- constant left-hand side IN subquery tests --

    public void testConstantInSubquery() {
        runGoldenTest("""
            FROM employees
            | WHERE 10001 IN (FROM employees | KEEP emp_no)
            """, STAGES);
    }

    public void testConstantNotInSubquery() {
        runGoldenTest("""
            FROM employees
            | WHERE 10001 NOT IN (FROM employees | KEEP emp_no)
            """, STAGES);
    }

    public void testConstantInSubqueryWithRemainingFilter() {
        runGoldenTest("""
            FROM employees
            | WHERE 10001 IN (FROM employees | KEEP emp_no) AND salary > 50000
            """, STAGES);
    }

    public void testStringConstantInSubquery() {
        runGoldenTest("""
            FROM employees
            | WHERE "Georgi" IN (FROM employees | KEEP first_name)
            """, STAGES);
    }

    // -- date comparison inside IN subquery --

    public void testInSubqueryWithImplicitDateCast() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (
                FROM employees
                | WHERE hire_date >= "1989-01-01T00:00:00.000Z"
                | KEEP emp_no
              )
            | KEEP emp_no
            """, STAGES);
    }

    // -- tests with FROM subquery and IN subquery --

    public void testFromSubqueryInsideInSubquery() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM employees, (FROM employees | KEEP emp_no) | KEEP emp_no)
            """, STAGES);
    }

    public void testFromSubqueryInsideNotInSubquery() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no NOT IN (FROM employees, (FROM employees | KEEP emp_no) | KEEP emp_no)
            """, STAGES);
    }

    public void testInSubqueryInsideFromSubquery() {
        runGoldenTest("""
            FROM employees,
                 (FROM employees | WHERE emp_no IN (FROM employees | KEEP emp_no) | KEEP emp_no)
            """, STAGES);
    }

    public void testNotInSubqueryInsideFromSubquery() {
        runGoldenTest("""
            FROM employees,
                 (FROM employees | WHERE emp_no NOT IN (FROM employees | KEEP emp_no) | KEEP emp_no)
            """, STAGES);
    }

    // -- nested IN/NOT IN subquery tests --

    public void testNestedInSubquery() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (
                FROM employees
                | WHERE salary IN (FROM employees | KEEP salary)
                | KEEP emp_no
              )
            """, STAGES);
    }

    public void testNestedNotInInsideInSubquery() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (
                FROM employees
                | WHERE salary NOT IN (FROM employees | KEEP salary)
                | KEEP emp_no
              )
            """, STAGES);
    }

    public void testNestedInInsideNotInSubquery() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no NOT IN (
                FROM employees
                | WHERE salary IN (FROM employees | KEEP salary)
                | KEEP emp_no
              )
            """, STAGES);
    }

    public void testThreeNestedInSubquery() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (
                FROM employees
                | WHERE salary IN (
                    FROM employees
                    | WHERE languages IN (FROM employees | KEEP languages)
                    | KEEP salary
                  )
                | KEEP emp_no
              )
            """, STAGES);
    }

    public void testNestedInSubqueryAndOtherPredicate() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (
                FROM employees
                | WHERE salary IN (FROM employees | KEEP salary)
                  AND languages > 2
                | KEEP emp_no
              )
            """, STAGES);
    }

    public void testDoubleNotInSubquery() {
        runGoldenTest("""
            FROM employees
            | WHERE NOT (emp_no NOT IN (FROM employees | KEEP emp_no))
            """, STAGES);
    }

    public void testTripleNotInSubquery() {
        runGoldenTest("""
            FROM employees
            | WHERE NOT (NOT (emp_no NOT IN (FROM employees | KEEP emp_no)))
            """, STAGES);
    }

    public void testDoubleNotInSubqueryOrOneMorePredicate() {
        runGoldenTest("""
            FROM employees
            | WHERE NOT (emp_no NOT IN (FROM employees | KEEP emp_no))
               OR salary > 50000
            """, STAGES);
    }

    public void testDoubleNotInSubqueryOrInSubquery() {
        runGoldenTest("""
            FROM employees
            | WHERE NOT (emp_no NOT IN (FROM employees | KEEP emp_no))
               OR salary IN (FROM employees | KEEP salary)
            """, STAGES);
    }

    public void testDoubleNotInSubqueryAndOneMorePredicate() {
        runGoldenTest("""
            FROM employees
            | WHERE NOT (emp_no NOT IN (FROM employees | KEEP emp_no))
               AND salary > 50000
            """, STAGES);
    }

    public void testDoubleNotInSubqueryAndInSubquery() {
        runGoldenTest("""
            FROM employees
            | WHERE NOT (emp_no NOT IN (FROM employees | KEEP emp_no))
               AND salary IN (FROM employees | KEEP salary)
            """, STAGES);
    }

    // -- disjunctive IN/NOT IN subquery tests --

    public void testDisjunctiveInSubqueries() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM employees | KEEP emp_no)
               OR salary IN (FROM employees | KEEP salary)
            """, STAGES);
    }

    public void testDisjunctiveInAndNotInSubqueries() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no NOT IN (FROM employees | KEEP emp_no)
               OR emp_no IN (FROM employees | WHERE salary > 50000 | KEEP emp_no)
            """, STAGES);
    }

    public void testDisjunctiveInSubqueryWithOtherPredicate() {
        runGoldenTest("""
            FROM employees
            | WHERE salary > 50000
               OR emp_no IN (FROM employees | KEEP emp_no)
            """, STAGES);
    }

    public void testDisjunctiveOrChainWithNotInSubquery() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM employees | KEEP emp_no)
               OR (salary > 50000 OR (languages < 3 OR gender NOT IN (FROM employees | KEEP gender)))
            """, STAGES);
    }

    public void testDisjunctiveOrChainWithConjunctiveNotInSubquery() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM employees | KEEP emp_no)
               OR (salary > 50000 OR (languages < 3 AND gender NOT IN (FROM employees | KEEP gender)))
            """, STAGES);
    }

    public void testDisjunctiveOrChainWithNotInSubqueryInMiddle() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM employees | KEEP emp_no)
               OR (salary > 50000 OR (gender NOT IN (FROM employees | KEEP gender)) OR languages < 3)
            """, STAGES);
    }

    public void testNestedConjunctiveAndDisjunctiveInSubquery() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM employees | KEEP emp_no)
               OR (salary > 50000 AND (languages < 3 OR gender NOT IN (FROM employees | KEEP gender)))
            """, STAGES);
    }

    // -- data types on join keys related tests --

    public void testKeywordVsTextInSubquery() {
        runGoldenTest("""
            FROM all_types
            | WHERE keyword IN (FROM all_types | KEEP text)
            """, STAGES);
    }

    public void testTextVsKeywordInSubquery() {
        runGoldenTest("""
            FROM all_types
            | WHERE text IN (FROM all_types | KEEP keyword)
            """, STAGES);
    }

    public void testIpVsIpInSubquery() {
        runGoldenTest("""
            FROM all_types
            | WHERE ip IN (FROM all_types | KEEP ip)
            """, STAGES);
    }

    public void testVersionVsVersionInSubquery() {
        runGoldenTest("""
            FROM all_types
            | WHERE version IN (FROM all_types | KEEP version)
            """, STAGES);
    }

    // -- FROM subquery union-type resolved by an explicit cast --

    public void testFromSubqueryUnionTypeLeftFieldWithCast() {
        runGoldenTest("""
            FROM employees, (FROM employees_incompatible | KEEP emp_no, first_name, salary)
            | EVAL id = emp_no::long
            | WHERE id IN (FROM employees_incompatible | WHERE salary > 70000 | KEEP emp_no)
            | KEEP id
            """, STAGES);
    }

    public void testFromSubqueryUnionTypeRightFieldWithCast() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM employees, (FROM employees_incompatible | KEEP emp_no) | EVAL id = emp_no::integer | KEEP id)
            | KEEP emp_no
            """, STAGES);
    }

    // -- multi-index union-typed field (across employees/employees_incompatible) resolved by an explicit cast --
    // The comma-separated FROM pattern resolves to a single relation whose emp_no field is union-typed (integer vs the incompatible
    // mapping), so casting it (emp_no::keyword) is required before it can be used as an IN/NOT IN join key. The cast resolves to a
    // MultiTypeEsField whose representation (compact vs legacy) is transport-version gated, so lower-bound these tests at the compact
    // representation.

    public void testUnionTypeLeftFieldWithCastInSubquery() {
        builder("""
            FROM employees, employees_incompatible
            | EVAL id_kw = emp_no::keyword
            | WHERE id_kw IN (FROM employees | KEEP first_name)
            | KEEP id_kw
            """).stages(STAGES).since(CompactMultiTypeEsField.CompactMultiTypeEsField).run();
    }

    public void testUnionTypeRightFieldWithCastInSubquery() {
        builder("""
            FROM employees
            | WHERE first_name IN (FROM employees, employees_incompatible | EVAL id_kw = emp_no::keyword | KEEP id_kw)
            | KEEP first_name
            """).stages(STAGES).since(CompactMultiTypeEsField.CompactMultiTypeEsField).run();
    }

    public void testUnionTypeLeftFieldWithCastInAntiJoin() {
        builder("""
            FROM employees, employees_incompatible
            | EVAL id_kw = emp_no::keyword
            | WHERE id_kw NOT IN (FROM employees | KEEP first_name)
            | KEEP id_kw
            """).stages(STAGES).since(CompactMultiTypeEsField.CompactMultiTypeEsField).run();
    }

    // -- IN subquery with views --

    public void testViewContainingInSubquery() {
        runGoldenTest(
            """
                FROM employeesInEmployees
                | WHERE salary > 50000
                | KEEP emp_no
                """,
            STAGES,
            Map.of("employeesInEmployees", "FROM employees | WHERE emp_no IN (FROM employees | KEEP emp_no) | KEEP emp_no, salary")
        );
    }

    public void testInSubqueryReferencingView() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM high_earners)
            """, STAGES, Map.of("high_earners", "FROM employees | WHERE salary > 70000 | KEEP emp_no"));
    }

    public void testNotInSubqueryReferencingView() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no NOT IN (FROM high_earners)
            """, STAGES, Map.of("high_earners", "FROM employees | WHERE salary > 70000 | KEEP emp_no"));
    }

    public void testInSubqueryReferencingViewWithInSubquery() {
        runGoldenTest(
            """
                FROM employees
                | WHERE emp_no IN (FROM employeesInEmployees | WHERE salary > 50000 | KEEP emp_no)
                """,
            STAGES,
            Map.of("employeesInEmployees", "FROM employees | WHERE emp_no IN (FROM employees | KEEP emp_no) | KEEP emp_no, salary")
        );
    }

    public void testNotInSubqueryReferencingViewWithInSubquery() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no NOT IN (FROM employeesInEmployees)
            """, STAGES, Map.of("employeesInEmployees", "FROM employees | WHERE emp_no IN (FROM employees | KEEP emp_no) | KEEP emp_no"));
    }

    public void testInSubqueryReferencingViewWithInSubqueryAndPredicate() {
        runGoldenTest("""
            FROM employees
            | WHERE salary > 50000 AND emp_no IN (FROM in_sub_view)
            """, STAGES, Map.of("in_sub_view", "FROM employees | WHERE salary IN (FROM employees | KEEP salary) | KEEP emp_no"));
    }

    public void testMultipleInSubqueriesWithViewAndFromSubquery() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM high_earners)
                AND salary IN (FROM (FROM employees | KEEP salary) | KEEP salary)
            """, STAGES, Map.of("high_earners", "FROM employees | WHERE salary > 70000 | KEEP emp_no"));
    }

    public void testInViewAndNotInFromSubquery() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM high_earners)
                AND emp_no NOT IN (FROM (FROM employees | KEEP emp_no) | WHERE emp_no > 10050 | KEEP emp_no)
            """, STAGES, Map.of("high_earners", "FROM employees | WHERE salary > 70000 | KEEP emp_no"));
    }

    public void testMultipleInSubqueriesReferencingViewsWithInSubqueries() {
        runGoldenTest(
            """
                FROM employees
                | WHERE emp_no IN (FROM view_a) AND salary IN (FROM view_b)
                """,
            STAGES,
            Map.of(
                "view_a",
                "FROM employees | WHERE emp_no IN (FROM employees | KEEP emp_no) | KEEP emp_no",
                "view_b",
                "FROM employees | WHERE salary IN (FROM employees | KEEP salary) | KEEP salary"
            )
        );
    }

    public void testInSubqueryReferencingViewWithNestedInSubqueryInDefinition() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM nested_in_view | KEEP emp_no)
            """, STAGES, Map.of("nested_in_view", """
            FROM employees
            | WHERE emp_no IN (
                FROM employees
                | WHERE languages IN (1, 2)
                | SORT emp_no ASC
                | LIMIT 10
                | KEEP emp_no
              )
            | KEEP emp_no
            """));
    }

    public void testInSubqueryReferencingConjunctionViewWithTwoInSubqueriesInDefinition() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM conj_in_view | KEEP emp_no)
            """, STAGES, Map.of("conj_in_view", """
            FROM employees
            | WHERE emp_no IN (FROM employees | SORT emp_no ASC | LIMIT 3 | KEEP emp_no)
                AND languages IN (FROM employees | KEEP languages)
            | KEEP emp_no
            """));
    }

    public void testThreeInSubqueriesIntersectingViewsEachWithInnerInSubquery() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM v_nested | KEEP emp_no)
                AND emp_no IN (FROM v_conj | KEEP emp_no)
                AND emp_no IN (FROM v_disj | KEEP emp_no)
            """, STAGES, Map.of("v_nested", """
            FROM employees
            | WHERE emp_no IN (
                FROM employees
                | WHERE languages IN (1, 2)
                | SORT emp_no ASC
                | LIMIT 10
                | KEEP emp_no
              )
            | KEEP emp_no
            """, "v_conj", """
            FROM employees
            | WHERE emp_no IN (FROM employees | SORT emp_no ASC | LIMIT 3 | KEEP emp_no)
                AND languages IN (FROM employees | KEEP languages)
            | KEEP emp_no
            """, "v_disj", """
            FROM employees
            | WHERE emp_no IN (FROM employees | KEEP emp_no)
                OR languages IN (1, 2)
            | KEEP emp_no
            """));
    }

    public void testInSubqueryInSubqueryNotInSubqueryReferencingViewsWithInnerInSubqueries() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM v_nested | KEEP emp_no)
                AND emp_no IN (FROM v_disj | KEEP emp_no)
                AND emp_no NOT IN (FROM v_conj | KEEP emp_no)
            """, STAGES, Map.of("v_nested", """
            FROM employees
            | WHERE emp_no IN (
                FROM employees
                | WHERE languages IN (1, 2)
                | SORT emp_no ASC
                | LIMIT 10
                | KEEP emp_no
              )
            | KEEP emp_no
            """, "v_disj", """
            FROM employees
            | WHERE emp_no IN (FROM employees | KEEP emp_no)
                OR languages IN (1, 2)
            | KEEP emp_no
            """, "v_conj", "FROM employees | WHERE emp_no IN (FROM employees | KEEP emp_no) | KEEP emp_no"));
    }

    public void testNotInNestedInDisjunctionNotInConjunctionViews() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no NOT IN (FROM v_nested | KEEP emp_no)
                AND emp_no IN (FROM v_disj | KEEP emp_no)
                AND emp_no NOT IN (FROM v_conj | KEEP emp_no)
            """, STAGES, Map.of("v_nested", """
            FROM employees
            | WHERE emp_no IN (
                FROM employees
                | WHERE languages IN (1, 2)
                | SORT emp_no ASC
                | LIMIT 10
                | KEEP emp_no
              )
            | KEEP emp_no
            """, "v_disj", """
            FROM employees
            | WHERE emp_no IN (FROM employees | KEEP emp_no)
                OR languages IN (1, 2)
            | KEEP emp_no
            """, "v_conj", "FROM employees | WHERE emp_no IN (FROM employees | KEEP emp_no) | KEEP emp_no"));
    }

    // -- IN subquery with UnionAll (FROM view, (FROM subquery)) --

    public void testInSubqueryWithUnionAllOfViewAndFromSubquery() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM employees_view, (FROM employees | WHERE salary > 70000) | KEEP emp_no)
            """, STAGES, Map.of("employees_view", "FROM employees | WHERE salary > 60000 | KEEP emp_no"));
    }

    public void testNotInSubqueryWithUnionAllOfViewAndFromSubquery() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no NOT IN (FROM employees_view, (FROM employees | KEEP emp_no) | KEEP emp_no)
            """, STAGES, Map.of("employees_view", "FROM employees | WHERE salary > 60000 | KEEP emp_no"));
    }

    public void testMultipleInSubqueriesWithUnionAllViewAndFromSubquery() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM view_a, (FROM employees | KEEP emp_no) | KEEP emp_no)
                AND salary IN (FROM view_b, (FROM employees | KEEP salary) | KEEP salary)
            """, STAGES, Map.of("view_a", "FROM employees | KEEP emp_no", "view_b", "FROM employees | KEEP salary"));
    }

    public void testInSubqueryUnionAllAndNotInSubqueryView() {
        runGoldenTest(
            """
                FROM employees
                | WHERE emp_no IN (FROM view_a, (FROM employees | KEEP emp_no) | KEEP emp_no)
                    AND emp_no NOT IN (FROM high_earners)
                """,
            STAGES,
            Map.of("view_a", "FROM employees | KEEP emp_no", "high_earners", "FROM employees | WHERE salary > 70000 | KEEP emp_no")
        );
    }

    public void testMainFromAndInSubqueryEachReferenceMultipleViewSubqueries() {
        runGoldenTest(
            """
                FROM (FROM main_view_a | KEEP emp_no), (FROM main_view_b | KEEP emp_no)
                | WHERE emp_no IN (FROM (FROM in_view_a | KEEP emp_no), (FROM in_view_b | KEEP emp_no) | KEEP emp_no)
                """,
            STAGES,
            Map.of(
                "main_view_a",
                "FROM employees | KEEP emp_no",
                "main_view_b",
                "FROM employees | WHERE salary > 50000 | KEEP emp_no",
                "in_view_a",
                "FROM employees | KEEP emp_no",
                "in_view_b",
                "FROM employees | WHERE salary > 60000 | KEEP emp_no"
            )
        );
    }

    // -- tests with TS source inside IN subquery --
    //
    // The grouping key `cluster` is a time-series dimension, so TranslateTimeSeriesAggregate rewrites it to either DIMENSIONVALUES (when
    // the negotiated cluster version supports `dimension_values`) or VALUES (when it does not). These tests characterize the newer form, so
    // their builder chains declare the corresponding lower bound (or the newer SUM long-overflow fix when the query contains SUM).
    // At `pack_dims_agg` the PackDims node folds into the TimeSeriesAggregate as PACKDIMSAGG, so that older shape lives in
    // [before_pack_dims_agg].

    public void testTsRateInsideInSubquery() {
        builder("""
            TS k8s
            | WHERE cluster IN (TS k8s
                               | STATS m = max(rate(network.total_bytes_in)) BY cluster
                               | KEEP cluster)
            | STATS max_rate = max(rate(network.total_bytes_in)) BY cluster
            """).stages(STAGES).since(DimensionValues.DIMENSION_VALUES_VERSION).expectationChangesAt(PACK_DIMS_AGG).run();
    }

    public void testTsRateInsideNotInSubquery() {
        builder("""
            TS k8s
            | WHERE cluster NOT IN (TS k8s
                                   | STATS m = max(rate(network.total_bytes_in)) BY cluster
                                   | KEEP cluster)
            | STATS max_rate = max(rate(network.total_bytes_in)) BY cluster
            """).stages(STAGES).since(DimensionValues.DIMENSION_VALUES_VERSION).expectationChangesAt(PACK_DIMS_AGG).run();
    }

    public void testInSubqueryMainTimeSeriesSubqueryIndex() {
        builder("""
            TS k8s
            | WHERE cluster IN (FROM employees | KEEP first_name)
            | STATS max_rate = max(rate(network.total_bytes_in)) BY cluster
            """).stages(STAGES).since(DimensionValues.DIMENSION_VALUES_VERSION).expectationChangesAt(PACK_DIMS_AGG).run();
    }

    public void testNotInSubqueryMainTimeSeriesSubqueryIndex() {
        builder("""
            TS k8s
            | WHERE cluster NOT IN (FROM employees | KEEP first_name)
            | STATS max_rate = max(rate(network.total_bytes_in)) BY cluster
            """).stages(STAGES).since(DimensionValues.DIMENSION_VALUES_VERSION).expectationChangesAt(PACK_DIMS_AGG).run();
    }

    public void testInSubqueryMainIndexSubqueryTimeSeries() {
        builder("""
            FROM employees
            | WHERE first_name IN (TS k8s
                                  | STATS max_rate = max(rate(network.total_bytes_in)) BY cluster
                                  | KEEP cluster)
            """).stages(STAGES).since(DimensionValues.DIMENSION_VALUES_VERSION).expectationChangesAt(PACK_DIMS_AGG).run();
    }

    public void testNotInSubqueryMainIndexSubqueryTimeSeries() {
        builder("""
            FROM employees
            | WHERE first_name NOT IN (TS k8s
                                      | STATS max_rate = max(rate(network.total_bytes_in)) BY cluster
                                      | KEEP cluster)
            """).stages(STAGES).since(DimensionValues.DIMENSION_VALUES_VERSION).expectationChangesAt(PACK_DIMS_AGG).run();
    }

    public void testTsWithoutAndRateInsideInSubquery() {
        builder("""
            TS k8s
            | WHERE cluster IN (TS k8s
                               | STATS m = max(rate(network.total_bytes_in)) BY cluster
                               | KEEP cluster)
            | STATS total_cost = sum(network.cost) BY WITHOUT(pod, region)
            """).stages(STAGES).since(Sum.ESQL_SUM_LONG_OVERFLOW_FIX).expectationChangesAt(PACK_DIMS_AGG).run();
    }

    public void testTsWithoutAndRateInsideNotInSubquery() {
        builder("""
            TS k8s
            | WHERE cluster NOT IN (TS k8s
                                   | STATS m = max(rate(network.total_bytes_in)) BY cluster
                                   | KEEP cluster)
            | STATS total_cost = sum(network.cost) BY WITHOUT(pod, region)
            """).stages(STAGES).since(Sum.ESQL_SUM_LONG_OVERFLOW_FIX).expectationChangesAt(PACK_DIMS_AGG).run();
    }

    public void testMultipleTsSubqueriesInsideInSubquery() {
        builder("""
            TS k8s
            | WHERE cluster IN (FROM
                                   (TS k8s
                                    | STATS max_bytes = max(to_long(network.total_bytes_in)) BY cluster
                                    | WHERE max_bytes > 10500
                                    | KEEP cluster),
                                   (TS k8s
                                    | STATS max_bytes = max(to_long(network.total_bytes_in)) BY cluster
                                    | WHERE max_bytes < 8000
                                    | KEEP cluster)
                               )
            | STATS max_bytes = max(to_long(network.total_bytes_in)) BY cluster
            | SORT cluster
            """).stages(STAGES).since(DimensionValues.DIMENSION_VALUES_VERSION).expectationChangesAt(PACK_DIMS_AGG).run();
    }

    public void testMultipleTsSubqueriesInsideNotInSubquery() {
        builder("""
            TS k8s
            | WHERE cluster NOT IN (FROM
                                       (TS k8s
                                        | STATS max_bytes = max(to_long(network.total_bytes_in)) BY cluster
                                        | WHERE max_bytes > 10500
                                        | KEEP cluster),
                                       (TS k8s
                                        | STATS max_bytes = max(to_long(network.total_bytes_in)) BY cluster
                                        | WHERE max_bytes < 8000
                                        | KEEP cluster)
                                   )
            | STATS max_bytes = max(to_long(network.total_bytes_in)) BY cluster
            | SORT cluster
            """).stages(STAGES).since(DimensionValues.DIMENSION_VALUES_VERSION).expectationChangesAt(PACK_DIMS_AGG).run();
    }

    // -- IN / NOT IN (subquery) crossed with external datasets --

    public void testInSubqueryMainExternalDatasetSubqueryIndex() {
        requireExternalDatasetSupport();
        runExternalDatasetGoldenTest("""
            FROM salaries_int
            | WHERE emp_no IN (FROM employees | KEEP emp_no)
            """);
    }

    public void testNotInSubqueryMainExternalDatasetSubqueryIndex() {
        requireExternalDatasetSupport();
        runExternalDatasetGoldenTest("""
            FROM salaries_int
            | WHERE emp_no NOT IN (FROM employees | KEEP emp_no)
            """);
    }

    public void testInSubqueryMainIndexSubqueryExternalDataset() {
        requireExternalDatasetSupport();
        runExternalDatasetGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM salaries_int | KEEP emp_no)
            """);
    }

    public void testNotInSubqueryMainIndexSubqueryExternalDataset() {
        requireExternalDatasetSupport();
        runExternalDatasetGoldenTest("""
            FROM employees
            | WHERE emp_no NOT IN (FROM salaries_int | KEEP emp_no)
            """);
    }

    public void testInSubqueryMainAndSubqueryExternalDataset() {
        requireExternalDatasetSupport();
        runExternalDatasetGoldenTest("""
            FROM salaries_int
            | WHERE emp_no IN (FROM salaries_long | KEEP emp_no)
            """);
    }

    public void testNotInSubqueryMainAndSubqueryExternalDataset() {
        requireExternalDatasetSupport();
        runExternalDatasetGoldenTest("""
            FROM salaries_int
            | WHERE emp_no NOT IN (FROM salaries_long | KEEP emp_no)
            """);
    }

    // -- IN subquery inside CASE, COALESCE, IS [NOT] NULL in WHERE --

    public void testInSubqueryInCaseThenArm() {
        runGoldenTest("""
            FROM employees
            | WHERE CASE(salary > 50000, emp_no IN (FROM employees | KEEP emp_no), false)
            """, STAGES);
    }

    public void testInSubqueryInCaseElseArm() {
        runGoldenTest("""
            FROM employees
            | WHERE CASE(salary > 50000, false, emp_no IN (FROM employees | KEEP emp_no))
            """, STAGES);
    }

    public void testNotInSubqueryInCoalesce() {
        runGoldenTest("""
            FROM employees
            | WHERE COALESCE(emp_no NOT IN (FROM employees | KEEP emp_no), false)
            """, STAGES);
    }

    // -- multi-column IN subquery: mixed with single-column IN subquery connected by AND/OR/NOT --

    public void testMultiColumnInSubqueryAndSingleColumnInSubquery() {
        requireMultiColumnInSubquerySupport();
        runGoldenTest("""
            FROM employees
            | WHERE (emp_no, salary) IN (FROM employees | KEEP emp_no, salary)
              AND languages IN (FROM employees | KEEP languages)
            """, STAGES);
    }

    public void testMultiColumnInSubqueryOrSingleColumnInSubquery() {
        requireMultiColumnInSubquerySupport();
        runGoldenTest("""
            FROM employees
            | WHERE (emp_no, salary) IN (FROM employees | KEEP emp_no, salary)
               OR languages IN (FROM employees | KEEP languages)
            """, STAGES);
    }

    public void testMultiColumnNotInSubqueryAndSingleColumnInSubquery() {
        requireMultiColumnInSubquerySupport();
        runGoldenTest("""
            FROM employees
            | WHERE (emp_no, salary) NOT IN (FROM employees | KEEP emp_no, salary)
              AND languages IN (FROM employees | KEEP languages)
            """, STAGES);
    }

    public void testMultiColumnInSubqueryAndSingleColumnNotInSubquery() {
        requireMultiColumnInSubquerySupport();
        runGoldenTest("""
            FROM employees
            | WHERE (emp_no, salary) IN (FROM employees | KEEP emp_no, salary)
              AND languages NOT IN (FROM employees | KEEP languages)
            """, STAGES);
    }

    // -- multi-column IN subquery: constant left-hand side --

    public void testConstantsInMultiColumnInSubquery() {
        requireMultiColumnInSubquerySupport();
        runGoldenTest("""
            FROM employees
            | WHERE (10001, 60000) IN (FROM employees | KEEP emp_no, salary)
            """, STAGES);
    }

    public void testMixedConstantAndFieldInMultiColumnInSubquery() {
        requireMultiColumnInSubquerySupport();
        runGoldenTest("""
            FROM employees
            | WHERE (emp_no, 60000) IN (FROM employees | KEEP emp_no, salary)
            """, STAGES);
    }

    /**
     * Repeated equal constants in a multi-column tuple hash identically, so both synthetic constant aliases land in the same Eval;
     * their names must stay distinct (via the per-rewrite ordinal in {@code InSubqueryResolver#syntheticConstName}) or the Eval's
     * output merging would silently drop the first field and orphan the join key referencing it. The golden plan pins down the two
     * distinctly-named Eval fields and the SemiJoin keys bound to them. Mirrors
     * {@code InSubqueryResolverTests#testRepeatedConstantsInMultiColumnInSubqueryGetDistinctNames}.
     */
    public void testRepeatedConstantsInMultiColumnInSubquery() {
        requireMultiColumnInSubquerySupport();
        runGoldenTest("""
            FROM employees
            | WHERE (1, 1) IN (FROM employees | KEEP emp_no, languages)
            """, STAGES);
    }

    // -- multi-column IN subquery: implicit date cast --

    public void testMultiColumnInSubqueryWithImplicitDateCast() {
        requireMultiColumnInSubquerySupport();
        runGoldenTest("""
            FROM employees
            | WHERE (emp_no, hire_date) IN (
                FROM employees
                | WHERE hire_date >= "1989-01-01T00:00:00.000Z"
                | KEEP emp_no, hire_date
              )
            | KEEP emp_no, hire_date
            """, STAGES);
    }

    // -- multi-column IN subquery: FROM subquery combinations --

    public void testFromSubqueryInsideMultiColumnInSubquery() {
        requireMultiColumnInSubquerySupport();
        runGoldenTest("""
            FROM employees
            | WHERE (emp_no, salary) IN (
                FROM employees, (FROM employees | KEEP emp_no, salary)
                | KEEP emp_no, salary
              )
            """, STAGES);
    }

    public void testFromSubqueryBeforeMultiColumnInSubquery() {
        requireMultiColumnInSubquerySupport();
        runGoldenTest("""
            FROM employees,
                 (FROM employees | WHERE salary > 50000 | KEEP emp_no, salary)
            | WHERE (emp_no, salary) IN (FROM employees | KEEP emp_no, salary)
            """, STAGES);
    }

    public void testMultiColumnInSubqueryInsideFromSubquery() {
        requireMultiColumnInSubquerySupport();
        runGoldenTest("""
            FROM employees,
                 (FROM employees
                  | WHERE (emp_no, salary) IN (FROM employees | KEEP emp_no, salary)
                  | KEEP emp_no, salary)
            """, STAGES);
    }

    // -- nested multi-column IN subquery --

    public void testNestedMultiColumnInSubquery() {
        requireMultiColumnInSubquerySupport();
        runGoldenTest("""
            FROM employees
            | WHERE (emp_no, salary) IN (
                FROM employees
                | WHERE (languages, salary) IN (FROM employees | KEEP languages, salary)
                | KEEP emp_no, salary
              )
            """, STAGES);
    }

    public void testNestedNotInMultiColumnInSubquery() {
        requireMultiColumnInSubquerySupport();
        runGoldenTest("""
            FROM employees
            | WHERE (emp_no, salary) NOT IN (
                FROM employees
                | WHERE (languages, salary) NOT IN (FROM employees | KEEP languages, salary)
                | KEEP emp_no, salary
              )
            """, STAGES);
    }

    public void testNestedSingleColumnInSubqueryInsideMultiColumnInSubquery() {
        requireMultiColumnInSubquerySupport();
        runGoldenTest("""
            FROM employees
            | WHERE (emp_no, salary) IN (
                FROM employees
                | WHERE languages IN (FROM employees | KEEP languages)
                | KEEP emp_no, salary
              )
            """, STAGES);
    }

    // -- multi-column IN subquery: union-typed field resolved by an explicit cast --

    public void testFromSubqueryUnionTypeLeftFieldWithCastInMultiColumnInSubquery() {
        requireMultiColumnInSubquerySupport();
        runGoldenTest("""
            FROM employees, (FROM employees_incompatible | KEEP emp_no, first_name, salary)
            | EVAL id = emp_no::long, sal = salary::long
            | WHERE (id, sal) IN (FROM employees_incompatible | KEEP emp_no, salary)
            | KEEP id, sal
            """, STAGES);
    }

    public void testUnionTypeFieldWithCastInMultiColumnInSubquery() {
        requireMultiColumnInSubquerySupport();
        runGoldenTest("""
            FROM employees, employees_incompatible
            | EVAL id_kw = emp_no::keyword, sal_kw = salary::keyword
            | WHERE (id_kw, sal_kw) IN (FROM employees | EVAL e = emp_no::keyword, s = salary::keyword | KEEP e, s)
            | KEEP id_kw, sal_kw
            """, STAGES, CompactMultiTypeEsField.CompactMultiTypeEsField);
    }

    public void testUnionTypeRightFieldWithCastInMultiColumnInSubquery() {
        requireMultiColumnInSubquerySupport();
        runGoldenTest("""
            FROM employees
            | WHERE (first_name, last_name) IN (
                FROM employees, employees_incompatible
                | EVAL id_kw = emp_no::keyword, sal_kw = salary::keyword
                | KEEP id_kw, sal_kw
              )
            | KEEP first_name, last_name
            """, STAGES, CompactMultiTypeEsField.CompactMultiTypeEsField);
    }

    // -- multi-column IN subquery: ROW as main source or subquery source --

    public void testMultiColumnInSubqueryWithRowSource() {
        requireMultiColumnInSubquerySupport();
        runGoldenTest("""
            FROM employees
            | WHERE (emp_no, salary) IN (ROW emp_no = 10001, salary = 60000)
            """, STAGES);
    }

    public void testRowMainMultiColumnInSubqueryWithIndexSource() {
        requireMultiColumnInSubquerySupport();
        runGoldenTest("""
            ROW emp_no = 10001, salary = 60000
            | WHERE (emp_no, salary) IN (FROM employees | KEEP emp_no, salary)
            """, STAGES);
    }

    public void testRowMainMultiColumnInSubqueryWithRowSource() {
        requireMultiColumnInSubquerySupport();
        runGoldenTest("""
            ROW emp_no = 10001, salary = 60000
            | WHERE (emp_no, salary) IN (ROW emp_no = 10001, salary = 60000)
            """, STAGES);
    }

    // -- multi-column IN subquery: TS as main source or subquery source --

    public void testMultiColumnInSubqueryWithTsSource() {
        requireMultiColumnInSubquerySupport();
        builder("""
            FROM employees
            | WHERE (first_name, last_name) IN (
                TS k8s
                | STATS max_bytes = max(to_long(network.total_bytes_in)) BY cluster, pod
                | KEEP cluster, pod
              )
            | KEEP first_name, last_name
            """).stages(STAGES).since(DimensionValues.DIMENSION_VALUES_VERSION).expectationChangesAt(PACK_DIMS_AGG).run();
    }

    public void testTsMainMultiColumnInSubqueryWithIndexSource() {
        requireMultiColumnInSubquerySupport();
        builder("""
            TS k8s
            | WHERE (cluster, pod) IN (FROM employees | KEEP first_name, last_name)
            | STATS max_rate = max(rate(network.total_bytes_in)) BY cluster
            """).stages(STAGES).since(DimensionValues.DIMENSION_VALUES_VERSION).expectationChangesAt(PACK_DIMS_AGG).run();
    }

    public void testTsMainMultiColumnInSubqueryWithRowSource() {
        requireMultiColumnInSubquerySupport();
        builder("""
            TS k8s
            | WHERE (cluster, pod) IN (ROW cluster = "my-cluster", pod = "my-pod")
            | STATS max_rate = max(rate(network.total_bytes_in)) BY cluster
            """).stages(STAGES).since(DimensionValues.DIMENSION_VALUES_VERSION).expectationChangesAt(PACK_DIMS_AGG).run();
    }

    public void testTsMainMultiColumnInSubqueryWithTsSource() {
        requireMultiColumnInSubquerySupport();
        builder("""
            TS k8s
            | WHERE (cluster, pod) IN (
                TS k8s
                | STATS max_bytes = max(to_long(network.total_bytes_in)) BY cluster, pod
                | KEEP cluster, pod
              )
            | STATS total_bytes = sum(to_long(network.total_bytes_in)) BY cluster
            """).stages(STAGES).since(DimensionValues.DIMENSION_VALUES_VERSION).expectationChangesAt(PACK_DIMS_AGG).run();
    }

    public void testRowMainMultiColumnInSubqueryWithTsSource() {
        requireMultiColumnInSubquerySupport();
        builder("""
            ROW cluster = "my-cluster", pod = "my-pod"
            | WHERE (cluster, pod) IN (
                TS k8s
                | STATS max_bytes = max(to_long(network.total_bytes_in)) BY cluster, pod
                | KEEP cluster, pod
              )
            """).stages(STAGES).since(DimensionValues.DIMENSION_VALUES_VERSION).expectationChangesAt(PACK_DIMS_AGG).run();
    }

    /**
     * The same constant IN predicate repeated across conjuncts of one WHERE materializes two synthetic constant aliases in the same
     * Eval; their names must stay distinct (via the per-rewrite ordinal in {@code InSubqueryResolver#syntheticConstName}) or the
     * Eval's output merging would silently drop the first field and orphan the join key referencing it. Mirrors
     * {@code InSubqueryResolverTests#testRepeatedConstantInSubqueriesGetDistinctNames}.
     */
    public void testRepeatedConstantInSubqueriesInOneWhere() {
        runGoldenTest("""
            FROM employees
            | WHERE 42 IN (FROM employees | KEEP emp_no) AND 42 IN (FROM employees | KEEP emp_no)
            """, STAGES);
    }

    /**
     * The same constant IN predicate in two separate WHERE commands. Each Filter is rewritten independently, allocating its synthetic
     * constant alias in its own Eval, so the two aliases can share a name (see {@code InSubqueryResolver#syntheticConstName}). That
     * collision is benign — ordinary cross-level name shadowing (as in {@code EVAL x = .. | EVAL x = ..}): each SemiJoin consumes its
     * key from its own Eval below the shadowing point, bound by NameId. The golden plan pins this down: two stacked SemiJoin/Eval
     * pairs, each join key referencing its own Eval's field.
     */
    public void testSameConstantInSubqueryInTwoWhereCommands() {
        runGoldenTest("""
            FROM employees
            | WHERE 42 IN (FROM employees | KEEP emp_no)
            | WHERE 42 IN (FROM employees | KEEP emp_no)
            """, STAGES);
    }

    // -- helpers --

    /**
     * Runs a golden test for a query that mixes the IN-subquery feature with external datasets, registering the
     * {@code salaries_int}/{@code salaries_long} datasets and their resolved schemas. The golden framework replays the production
     * pipeline order from {@code EsqlSession} (resolve IN subqueries into SemiJoin/AntiJoin, then rewrite FROM dataset targets into
     * external relations via {@code DatasetRewriter}), so combinations referencing an index, a dataset, or both resolve on the shared
     * {@code emp_no} key.
     */
    private void runExternalDatasetGoldenTest(String query) {
        DataSource dataSource = new DataSource("external_ds", "test", null, Map.of());
        Dataset intDataset = new Dataset("salaries_int", new DataSourceReference("external_ds"), SALARIES_INT_RESOURCE, null, Map.of());
        Dataset longDataset = new Dataset("salaries_long", new DataSourceReference("external_ds"), SALARIES_LONG_RESOURCE, null, Map.of());
        ProjectMetadata projectMetadata = ProjectMetadata.builder(ProjectId.DEFAULT)
            .putCustom(DataSourceMetadata.TYPE, new DataSourceMetadata(Map.of("external_ds", dataSource)))
            .datasets(Map.of("salaries_int", intDataset, "salaries_long", longDataset))
            .build();
        ExternalSourceResolution resolution = new ExternalSourceResolution(
            Map.of(
                SALARIES_INT_RESOURCE,
                externalSource(SALARIES_INT_RESOURCE, DataType.INTEGER),
                SALARIES_LONG_RESOURCE,
                externalSource(SALARIES_LONG_RESOURCE, DataType.LONG)
            )
        );
        builder(query).stages(STAGES).datasetMetadata(projectMetadata).externalSourceResolution(resolution).run();
    }

    /** A resolved external source named {@code emp_no}/{@code name}/{@code salary} with the given salary type. */
    private static ExternalSourceResolution.ResolvedSource externalSource(String path, DataType salaryType) {
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
}
