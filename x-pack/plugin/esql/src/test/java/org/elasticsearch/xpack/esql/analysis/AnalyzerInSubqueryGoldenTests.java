/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.DataSourceReference;
import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.test.TransportVersionUtils;
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
import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;
import org.junit.Before;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;

/**
 * Golden tests for the analyzed plans produced by IN / NOT IN subquery scenarios.
 */
public class AnalyzerInSubqueryGoldenTests extends GoldenTestCase {

    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.ANALYSIS);

    private static final String SALARIES_INT_RESOURCE = "s3://bucket/salaries_int.parquet";
    private static final String SALARIES_LONG_RESOURCE = "s3://bucket/salaries_long.parquet";

    @Before
    public void checkInSubquerySupport() {
        assumeTrue("Requires IN subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITHOUT_VIEW.isEnabled());
    }

    private static void requireInSubqueryViewSupport() {
        assumeTrue("Requires IN subquery with view support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITH_VIEW.isEnabled());
    }

    private static void requireExternalDatasetSupport() {
        assumeTrue("Requires external dataset in FROM command support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());
    }

    private static void requireInSubqueryWithTSSupport() {
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
        assumeTrue("Requires where in subquery with TS source support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITH_TS.isEnabled());
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
    // MultiTypeEsField whose representation (compact vs legacy) is transport-version gated, so pin a compact-supporting version to keep the
    // snapshot deterministic.

    public void testUnionTypeLeftFieldWithCastInSubquery() {
        runGoldenTest("""
            FROM employees, employees_incompatible
            | EVAL id_kw = emp_no::keyword
            | WHERE id_kw IN (FROM employees | KEEP first_name)
            | KEEP id_kw
            """, STAGES, CompactMultiTypeEsField.CompactMultiTypeEsField);
    }

    public void testUnionTypeRightFieldWithCastInSubquery() {
        runGoldenTest("""
            FROM employees
            | WHERE first_name IN (FROM employees, employees_incompatible | EVAL id_kw = emp_no::keyword | KEEP id_kw)
            | KEEP first_name
            """, STAGES, CompactMultiTypeEsField.CompactMultiTypeEsField);
    }

    public void testUnionTypeLeftFieldWithCastInAntiJoin() {
        runGoldenTest("""
            FROM employees, employees_incompatible
            | EVAL id_kw = emp_no::keyword
            | WHERE id_kw NOT IN (FROM employees | KEEP first_name)
            | KEEP id_kw
            """, STAGES, CompactMultiTypeEsField.CompactMultiTypeEsField);
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
        requireInSubqueryViewSupport();
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM high_earners)
            """, STAGES, Map.of("high_earners", "FROM employees | WHERE salary > 70000 | KEEP emp_no"));
    }

    public void testNotInSubqueryReferencingView() {
        requireInSubqueryViewSupport();
        runGoldenTest("""
            FROM employees
            | WHERE emp_no NOT IN (FROM high_earners)
            """, STAGES, Map.of("high_earners", "FROM employees | WHERE salary > 70000 | KEEP emp_no"));
    }

    public void testInSubqueryReferencingViewWithInSubquery() {
        requireInSubqueryViewSupport();
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
        requireInSubqueryViewSupport();
        runGoldenTest("""
            FROM employees
            | WHERE emp_no NOT IN (FROM employeesInEmployees)
            """, STAGES, Map.of("employeesInEmployees", "FROM employees | WHERE emp_no IN (FROM employees | KEEP emp_no) | KEEP emp_no"));
    }

    public void testInSubqueryReferencingViewWithInSubqueryAndPredicate() {
        requireInSubqueryViewSupport();
        runGoldenTest("""
            FROM employees
            | WHERE salary > 50000 AND emp_no IN (FROM in_sub_view)
            """, STAGES, Map.of("in_sub_view", "FROM employees | WHERE salary IN (FROM employees | KEEP salary) | KEEP emp_no"));
    }

    public void testMultipleInSubqueriesWithViewAndFromSubquery() {
        requireInSubqueryViewSupport();
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM high_earners)
                AND salary IN (FROM (FROM employees | KEEP salary) | KEEP salary)
            """, STAGES, Map.of("high_earners", "FROM employees | WHERE salary > 70000 | KEEP emp_no"));
    }

    public void testInViewAndNotInFromSubquery() {
        requireInSubqueryViewSupport();
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM high_earners)
                AND emp_no NOT IN (FROM (FROM employees | KEEP emp_no) | WHERE emp_no > 10050 | KEEP emp_no)
            """, STAGES, Map.of("high_earners", "FROM employees | WHERE salary > 70000 | KEEP emp_no"));
    }

    public void testMultipleInSubqueriesReferencingViewsWithInSubqueries() {
        requireInSubqueryViewSupport();
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
        requireInSubqueryViewSupport();
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
        requireInSubqueryViewSupport();
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
        requireInSubqueryViewSupport();
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
        requireInSubqueryViewSupport();
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
        requireInSubqueryViewSupport();
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
        requireInSubqueryViewSupport();
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM employees_view, (FROM employees | WHERE salary > 70000) | KEEP emp_no)
            """, STAGES, Map.of("employees_view", "FROM employees | WHERE salary > 60000 | KEEP emp_no"));
    }

    public void testNotInSubqueryWithUnionAllOfViewAndFromSubquery() {
        requireInSubqueryViewSupport();
        runGoldenTest("""
            FROM employees
            | WHERE emp_no NOT IN (FROM employees_view, (FROM employees | KEEP emp_no) | KEEP emp_no)
            """, STAGES, Map.of("employees_view", "FROM employees | WHERE salary > 60000 | KEEP emp_no"));
    }

    public void testMultipleInSubqueriesWithUnionAllViewAndFromSubquery() {
        requireInSubqueryViewSupport();
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM view_a, (FROM employees | KEEP emp_no) | KEEP emp_no)
                AND salary IN (FROM view_b, (FROM employees | KEEP salary) | KEEP salary)
            """, STAGES, Map.of("view_a", "FROM employees | KEEP emp_no", "view_b", "FROM employees | KEEP salary"));
    }

    public void testInSubqueryUnionAllAndNotInSubqueryView() {
        requireInSubqueryViewSupport();
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
        requireInSubqueryViewSupport();
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
    // the negotiated cluster version supports `dimension_values`) or VALUES (when it does not). The default golden-test builder randomizes
    // the minimum transport version, which would make the captured plan flap between the two forms; pin a version that supports
    // `dimension_values` (or TransportVersion.current() for the version-gated SUM long-overflow mode) so the snapshot stays deterministic.

    public void testTsRateInsideInSubquery() {
        requireInSubqueryWithTSSupport();
        runGoldenTest("""
            TS k8s
            | WHERE cluster IN (TS k8s
                               | STATS m = max(rate(network.total_bytes_in)) BY cluster
                               | KEEP cluster)
            | STATS max_rate = max(rate(network.total_bytes_in)) BY cluster
            """, STAGES, TransportVersionUtils.randomVersionSupporting(DimensionValues.DIMENSION_VALUES_VERSION));
    }

    public void testTsRateInsideNotInSubquery() {
        requireInSubqueryWithTSSupport();
        runGoldenTest("""
            TS k8s
            | WHERE cluster NOT IN (TS k8s
                                   | STATS m = max(rate(network.total_bytes_in)) BY cluster
                                   | KEEP cluster)
            | STATS max_rate = max(rate(network.total_bytes_in)) BY cluster
            """, STAGES, TransportVersionUtils.randomVersionSupporting(DimensionValues.DIMENSION_VALUES_VERSION));
    }

    public void testInSubqueryMainTimeSeriesSubqueryIndex() {
        requireInSubqueryWithTSSupport();
        runGoldenTest("""
            TS k8s
            | WHERE cluster IN (FROM employees | KEEP first_name)
            | STATS max_rate = max(rate(network.total_bytes_in)) BY cluster
            """, STAGES, TransportVersionUtils.randomVersionSupporting(DimensionValues.DIMENSION_VALUES_VERSION));
    }

    public void testNotInSubqueryMainTimeSeriesSubqueryIndex() {
        requireInSubqueryWithTSSupport();
        runGoldenTest("""
            TS k8s
            | WHERE cluster NOT IN (FROM employees | KEEP first_name)
            | STATS max_rate = max(rate(network.total_bytes_in)) BY cluster
            """, STAGES, TransportVersionUtils.randomVersionSupporting(DimensionValues.DIMENSION_VALUES_VERSION));
    }

    public void testInSubqueryMainIndexSubqueryTimeSeries() {
        requireInSubqueryWithTSSupport();
        runGoldenTest("""
            FROM employees
            | WHERE first_name IN (TS k8s
                                  | STATS max_rate = max(rate(network.total_bytes_in)) BY cluster
                                  | KEEP cluster)
            """, STAGES, TransportVersionUtils.randomVersionSupporting(DimensionValues.DIMENSION_VALUES_VERSION));
    }

    public void testNotInSubqueryMainIndexSubqueryTimeSeries() {
        requireInSubqueryWithTSSupport();
        runGoldenTest("""
            FROM employees
            | WHERE first_name NOT IN (TS k8s
                                      | STATS max_rate = max(rate(network.total_bytes_in)) BY cluster
                                      | KEEP cluster)
            """, STAGES, TransportVersionUtils.randomVersionSupporting(DimensionValues.DIMENSION_VALUES_VERSION));
    }

    public void testTsWithoutAndRateInsideInSubquery() {
        requireInSubqueryWithTSSupport();
        runGoldenTest("""
            TS k8s
            | WHERE cluster IN (TS k8s
                               | STATS m = max(rate(network.total_bytes_in)) BY cluster
                               | KEEP cluster)
            | STATS total_cost = sum(network.cost) BY WITHOUT(pod, region)
            """, STAGES, TransportVersion.current());
    }

    public void testTsWithoutAndRateInsideNotInSubquery() {
        requireInSubqueryWithTSSupport();
        runGoldenTest("""
            TS k8s
            | WHERE cluster NOT IN (TS k8s
                                   | STATS m = max(rate(network.total_bytes_in)) BY cluster
                                   | KEEP cluster)
            | STATS total_cost = sum(network.cost) BY WITHOUT(pod, region)
            """, STAGES, TransportVersion.current());
    }

    public void testMultipleTsSubqueriesInsideInSubquery() {
        requireInSubqueryWithTSSupport();
        runGoldenTest("""
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
            """, STAGES, TransportVersion.current());
    }

    public void testMultipleTsSubqueriesInsideNotInSubquery() {
        requireInSubqueryWithTSSupport();
        runGoldenTest("""
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
            """, STAGES, TransportVersion.current());
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
