/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.approximation.ApproximationVerifier;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedField;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.join.AntiJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.join.SemiJoin;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Unit tests for IN/NOT IN subquery analysis that don't fit the golden-test model: the negative (rejection / error) cases.
 */
public class AnalyzerInSubqueryTests extends ESTestCase {

    @Before
    public void checkInSubquerySupport() {
        assumeTrue("Requires IN subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITHOUT_VIEW.isEnabled());
    }

    // basic IN and NOT IN subquery, validate JoinConfig

    public void testInSubquery() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM employees
            | WHERE emp_no IN (FROM employees | KEEP emp_no)
            """);

        Limit limit = as(plan, Limit.class);
        SemiJoin semiJoin = as(limit.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().size(), equalTo(1));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(semiJoin.config().rightFields().size(), equalTo(1));
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));
        assertThat(semiJoin.output(), equalTo(semiJoin.left().output()));

        EsRelation leftRelation = as(semiJoin.left(), EsRelation.class);
        assertEquals("employees", leftRelation.indexPattern());

        Project rightProject = as(semiJoin.right(), Project.class);
        EsRelation rightRelation = as(rightProject.child(), EsRelation.class);
        assertEquals("employees", rightRelation.indexPattern());
    }

    public void testNotInSubquery() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM employees
            | WHERE emp_no NOT IN (FROM employees | KEEP emp_no)
            """);

        Limit limit = as(plan, Limit.class);
        AntiJoin antiJoin = as(limit.child(), AntiJoin.class);
        assertThat(antiJoin.config().type(), equalTo(JoinTypes.ANTI));
        assertThat(antiJoin.config().leftFields().size(), equalTo(1));
        assertThat(antiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(antiJoin.config().rightFields().size(), equalTo(1));
        assertThat(antiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));
        assertThat(antiJoin.output(), equalTo(antiJoin.left().output()));

        EsRelation leftRelation = as(antiJoin.left(), EsRelation.class);
        assertEquals("employees", leftRelation.indexPattern());

        Project rightProject = as(antiJoin.right(), Project.class);
        EsRelation rightRelation = as(rightProject.child(), EsRelation.class);
        assertEquals("employees", rightRelation.indexPattern());
    }

    /**
     * Verifies that an IN subquery in STATS WHERE filter is rejected.
     */
    public void testRejectsInSubqueryInStatsWhereFilter() {
        errorInSubquery("""
            FROM employees
            | STATS cnt = COUNT(*) WHERE emp_no IN (FROM employees | KEEP emp_no)
            """, containsString("IN subquery is not supported in [STATS cnt = COUNT(*) WHERE emp_no IN (FROM employees | KEEP emp_no)]"));
    }

    /**
     * Verifies that a NOT IN subquery in STATS WHERE filter is rejected.
     */
    public void testRejectsNotInSubqueryInStatsWhereFilter() {
        errorInSubquery(
            """
                FROM employees
                | STATS cnt = COUNT(*) WHERE emp_no NOT IN (FROM employees | KEEP emp_no)
                """,
            containsString("IN subquery is not supported in [STATS cnt = COUNT(*) WHERE emp_no NOT IN (FROM employees | KEEP emp_no)]")
        );
    }

    /**
     * Verifies that IN subquery in STATS WHERE with BY grouping is rejected.
     */
    public void testRejectsInSubqueryInStatsWhereFilterWithGrouping() {
        errorInSubquery(
            """
                FROM employees
                | STATS cnt = COUNT(*) WHERE emp_no IN (FROM employees | KEEP emp_no) BY languages
                """,
            containsString(
                "IN subquery is not supported in [STATS cnt = COUNT(*) WHERE emp_no IN (FROM employees | KEEP emp_no) BY languages]"
            )
        );
    }

    // -- negative: IN subquery in INLINESTATS --

    /**
     * Verifies that an IN subquery in INLINESTATS WHERE filter is rejected.
     */
    public void testRejectsInSubqueryInInlineStatsWhereFilter() {
        errorInSubquery(
            """
                FROM employees
                | INLINESTATS cnt = COUNT(*) WHERE emp_no IN (FROM employees | KEEP emp_no)
                """,
            containsString("IN subquery is not supported in [INLINESTATS cnt = COUNT(*) WHERE emp_no IN (FROM employees | KEEP emp_no)]")
        );
    }

    /**
     * Verifies that a NOT IN subquery in INLINESTATS WHERE filter is rejected.
     */
    public void testRejectsNotInSubqueryInInlineStatsWhereFilter() {
        errorInSubquery(
            """
                FROM employees
                | INLINESTATS cnt = COUNT(*) WHERE emp_no NOT IN (FROM employees | KEEP emp_no)
                """,
            containsString(
                "IN subquery is not supported in [INLINESTATS cnt = COUNT(*) WHERE emp_no NOT IN (FROM employees | KEEP emp_no)]"
            )
        );
    }

    /**
     * Verifies that IN subquery in INLINESTATS WHERE with BY grouping is rejected.
     */
    public void testRejectsInSubqueryInInlineStatsWhereFilterWithGrouping() {
        errorInSubquery(
            """
                FROM employees
                | INLINESTATS cnt = COUNT(*) WHERE emp_no IN (FROM employees | KEEP emp_no) BY languages
                """,
            containsString(
                "IN subquery is not supported in [INLINESTATS cnt = COUNT(*) WHERE emp_no IN (FROM employees | KEEP emp_no) BY languages]"
            )
        );
    }

    // -- negative: IN subquery in EVAL --

    /**
     * Verifies that an IN subquery inside EVAL is rejected.
     */
    public void testRejectsInSubqueryInEval() {
        errorInSubquery("""
            FROM employees
            | EVAL x = emp_no IN (FROM employees | KEEP emp_no)
            """, containsString("IN subquery is not supported in [EVAL x = emp_no IN (FROM employees | KEEP emp_no)]"));
    }

    /**
     * Verifies that a NOT IN subquery inside EVAL is rejected.
     */
    public void testRejectsNotInSubqueryInEval() {
        errorInSubquery("""
            FROM employees
            | EVAL x = emp_no NOT IN (FROM employees | KEEP emp_no)
            """, containsString("IN subquery is not supported in [EVAL x = emp_no NOT IN (FROM employees | KEEP emp_no)]"));
    }

    // -- approximation incompatibility tests --

    /**
     * Verifies that IN subquery before STATS is incompatible with approximation.
     */
    public void testApproximationRejectsInSubqueryBeforeStats() {
        assertApproximationRejects("FROM employees | WHERE emp_no IN (FROM employees | KEEP emp_no) | STATS COUNT()");
    }

    /**
     * Verifies that NOT IN subquery before STATS is incompatible with approximation.
     */
    public void testApproximationRejectsNotInSubqueryBeforeStats() {
        assertApproximationRejects("FROM employees | WHERE emp_no NOT IN (FROM employees | KEEP emp_no) | STATS COUNT()");
    }

    /**
     * Verifies that IN subquery after STATS is incompatible with approximation.
     */
    public void testApproximationRejectsInSubqueryAfterStats() {
        assertApproximationRejects("FROM employees | STATS cnt = COUNT() BY emp_no | WHERE emp_no IN (FROM employees | KEEP emp_no)");
    }

    // -- negative analyzer/verifier tests --

    /**
     * Verifies that an IN subquery returning two columns (KEEP emp_no, salary) is rejected.
     */
    public void testRejectsInSubqueryWithMultipleColumns() {
        errorInSubquery("""
            FROM employees
            | WHERE emp_no IN (FROM employees | KEEP emp_no, salary)
            """, containsString("IN subquery must return exactly one column, found [emp_no, salary]"));
    }

    /**
     * Verifies that a NOT IN subquery returning two columns is rejected.
     */
    public void testRejectsNotInSubqueryWithMultipleColumns() {
        errorInSubquery("""
            FROM employees
            | WHERE emp_no NOT IN (FROM employees | KEEP emp_no, salary)
            """, containsString("IN subquery must return exactly one column, found [emp_no, salary]"));
    }

    /**
     * Verifies that an IN subquery returning all columns (no KEEP) is rejected.
     */
    public void testRejectsInSubqueryWithAllColumns() {
        errorInSubquery("""
            FROM employees
            | WHERE emp_no IN (FROM employees)
            """, containsString("IN subquery must return exactly one column"));
    }

    /**
     * Verifies that an IN subquery with STATS ... BY returning two columns is rejected.
     */
    public void testRejectsInSubqueryWithStatsByReturningMultipleColumns() {
        errorInSubquery("""
            FROM employees
            | WHERE emp_no IN (FROM employees | STATS max(emp_no) BY languages)
            """, containsString("IN subquery must return exactly one column, found [max(emp_no), languages]"));
    }

    /**
     * Verifies that an IN subquery returning no column is rejected.
     */
    public void testRejectsInSubqueryReturningNoColumn() {
        errorInSubquery("""
            FROM employees
            | WHERE emp_no IN (FROM employees | STATS m = max(emp_no) BY languages | DROP m ,languages)
            """, containsString("IN subquery must return exactly one column, found []"));
    }

    /**
     * An IN subquery against an index with empty mapping (only the {@code <no-fields>} placeholder)
     * has no real column to compare against. The analyzer should surface a clear error rather than
     * letting the placeholder leak into type-compatibility checking.
     */
    public void testRejectsInSubqueryAgainstIndexWithEmptyMapping() {
        analyzer().addEmployees().addEmptyIndex().error("""
            FROM employees
            | WHERE emp_no IN (FROM empty_index)
            """, containsString("IN subquery cannot reference an index with empty mapping"));
    }

    /**
     * Same as {@link #testRejectsInSubqueryAgainstIndexWithEmptyMapping}, but for an index whose
     * concrete indices entry exists yet the mapping is still empty (no_fields_index).
     */
    public void testRejectsInSubqueryAgainstNoFieldsIndex() {
        analyzer().addEmployees().addNoFieldsIndex().error("""
            FROM employees
            | WHERE emp_no IN (FROM no_fields_index)
            """, containsString("IN subquery cannot reference an index with empty mapping"));
    }

    /**
     * Verifies that an IN subquery with integer left side and keyword right side is rejected.
     */
    public void testRejectsTypeMismatchIntegerVsKeyword() {
        errorInSubquery("""
            FROM employees
            | WHERE emp_no IN (FROM employees | KEEP first_name)
            """, containsString("left field [emp_no] of type [INTEGER] is incompatible with right field [first_name] of type [KEYWORD]"));
    }

    /**
     * Verifies that a NOT IN subquery with keyword left side and integer right side is rejected.
     */
    public void testRejectsTypeMismatchKeywordVsInteger() {
        errorInSubquery("""
            FROM employees
            | WHERE first_name NOT IN (FROM employees | KEEP emp_no)
            """, containsString("left field [first_name] of type [KEYWORD] is incompatible with right field [emp_no] of type [INTEGER]"));
    }

    /**
     * Verifies that an IN subquery with integer left side and date right side is rejected.
     */
    public void testRejectsTypeMismatchIntegerVsDate() {
        errorInSubquery("""
            FROM employees
            | WHERE emp_no IN (FROM employees | KEEP hire_date)
            """, containsString("left field [emp_no] of type [INTEGER] is incompatible with right field [hire_date] of type [DATETIME]"));
    }

    // -- non-comparable types in IN subquery --

    /**
     * Verifies that counter types (COUNTER_LONG) are rejected as IN subquery join keys.
     */
    public void testRejectsCounterLongInSubquery() {
        errorWithK8s("""
            FROM k8s
            | WHERE network.total_bytes_in IN (FROM k8s | KEEP network.total_bytes_in)
            """, containsString("IN subquery with right field [network.total_bytes_in] of type [COUNTER_LONG] is not supported"));
    }

    /**
     * Verifies that counter types (COUNTER_DOUBLE) are rejected as IN subquery join keys.
     */
    public void testRejectsCounterDoubleInSubquery() {
        errorWithK8s("""
            FROM k8s
            | WHERE network.total_cost IN (FROM k8s | KEEP network.total_cost)
            """, containsString("IN subquery with right field [network.total_cost] of type [COUNTER_DOUBLE] is not supported"));
    }

    /**
     * Verifies that aggregate_metric_double is rejected as IN subquery join key.
     */
    public void testRejectsAggregateMetricDoubleInSubquery() {
        errorWithK8sDownsampled("""
            FROM k8s
            | WHERE network.eth0.tx IN (FROM k8s | KEEP network.eth0.tx)
            """, containsString("IN subquery with right field [network.eth0.tx] of type [AGGREGATE_METRIC_DOUBLE] is not supported"));
    }

    /**
     * Verifies that numeric type mismatch (INTEGER vs LONG) is rejected — SemiJoin requires exact type match.
     */
    public void testRejectsNumericTypeMismatchIntegerVsLong() {
        errorInSubquery("""
            FROM employees
            | WHERE emp_no IN (FROM employees | EVAL x = languages::long | KEEP x)
            """, containsString("left field [emp_no] of type [INTEGER] is incompatible with right field [x] of type [LONG]"));
    }

    /**
     * Verifies that KEYWORD left vs IP right is compatible in IN subquery.
     */
    public void testRejectsKeywordVsIpInSubquery() {
        errorWithAllTypes("""
            FROM all_types
            | WHERE keyword IN (FROM all_types | KEEP ip)
            """, containsString("left field [keyword] of type [KEYWORD] is incompatible with right field [ip] of type [IP]"));
    }

    /**
     * Verifies that IP left vs VERSION right is incompatible in IN subquery.
     */
    public void testRejectsIpVsVersionInSubquery() {
        errorWithAllTypes("""
            FROM all_types
            | WHERE ip IN (FROM all_types | KEEP version)
            """, containsString("left field [ip] of type [IP] is incompatible with right field [version] of type [VERSION]"));
    }

    /**
     * Verifies that VERSION left vs TEXT right is incompatible in IN subquery.
     */
    public void testRejectsVersionVsTextInSubquery() {
        errorWithAllTypes("""
            FROM all_types
            | WHERE version IN (FROM all_types | KEEP text)
            """, containsString("left field [version] of type [VERSION] is incompatible with right field [text] of type [TEXT]"));
    }

    /**
     * Verifies that IP left vs KEYWORD right is incompatible in IN subquery.
     */
    public void testRejectsIpVsKeywordInSubquery() {
        errorWithAllTypes("""
            FROM all_types
            | WHERE ip IN (FROM all_types | KEEP keyword)
            """, containsString("left field [ip] of type [IP] is incompatible with right field [keyword] of type [KEYWORD]"));
    }

    /**
     * Verifies that VERSION left vs KEYWORD right is incompatible in IN subquery.
     */
    public void testRejectsVersionVsKeywordInSubquery() {
        errorWithAllTypes("""
            FROM all_types
            | WHERE version IN (FROM all_types | KEEP keyword)
            """, containsString("left field [version] of type [VERSION] is incompatible with right field [keyword] of type [KEYWORD]"));
    }

    // -- date vs date_nanos incompatibility --

    /**
     * Verifies that DATETIME left vs DATE_NANOS right is incompatible in IN subquery.
     * employees has hire_date:date (DATETIME), employees_incompatible has hire_date:date_nanos (DATE_NANOS).
     */
    public void testRejectsDateVsDateNanosInSubquery() {
        errorWithIncompatible(
            """
                FROM employees
                | WHERE hire_date IN (FROM employees_incompatible | KEEP hire_date)
                """,
            containsString("left field [hire_date] of type [DATETIME] is incompatible with right field [hire_date] of type [DATE_NANOS]")
        );
    }

    /**
     * Verifies that DATE_NANOS left vs DATETIME right is incompatible in IN subquery.
     */
    public void testRejectsDateNanosVsDateInSubquery() {
        errorWithIncompatible(
            """
                FROM employees_incompatible
                | WHERE hire_date IN (FROM employees | KEEP hire_date)
                """,
            containsString("left field [hire_date] of type [DATE_NANOS] is incompatible with right field [hire_date] of type [DATETIME]")
        );
    }

    // -- union type tests --

    /**
     * Verifies that a union type field (id: keyword + integer) as the left join key of IN subquery
     * fails without explicit casting.
     */
    public void testRejectsUnionTypeLeftFieldInSubquery() {
        errorWithUnionIndex(
            """
                FROM union_index*
                | WHERE id IN (FROM employees | KEEP emp_no)
                | KEEP id
                """,
            containsString(
                "Cannot use field [id] due to ambiguities being mapped as [2] incompatible types:"
                    + " [keyword] in [union_index_1], [integer] in [union_index_2]"
            )
        );
    }

    /**
     * Verifies that a union type field as the right join key of IN subquery fails without explicit casting.
     */
    public void testRejectsUnionTypeRightFieldInSubquery() {
        errorWithUnionIndex(
            """
                FROM employees
                | WHERE first_name IN (FROM union_index* | KEEP id)
                | KEEP first_name
                """,
            containsString(
                "Cannot use field [id] due to ambiguities being mapped as [2] incompatible types:"
                    + " [keyword] in [union_index_1], [integer] in [union_index_2]"
            )
        );
    }

    /**
     * Verifies that NOT IN with a union type field on the left fails without casting.
     */
    public void testRejectsUnionTypeLeftFieldInAntiJoin() {
        errorWithUnionIndex(
            """
                FROM union_index*
                | WHERE id NOT IN (FROM employees | KEEP emp_no)
                | KEEP id
                """,
            containsString(
                "Cannot use field [id] due to ambiguities being mapped as [2] incompatible types:"
                    + " [keyword] in [union_index_1], [integer] in [union_index_2]"
            )
        );
    }

    // -- union type tests with FROM subqueries --

    /**
     * Verifies that FROM subqueries with conflicting types for emp_no (integer + long) fail without casting.
     */
    public void testRejectsFromSubqueryUnionTypeLeftField() {
        errorWithIncompatible("""
            FROM employees, (FROM employees_incompatible | KEEP emp_no, first_name, salary)
            | WHERE emp_no IN (FROM employees | WHERE salary > 70000 | KEEP emp_no)
            | KEEP emp_no
            """, containsString("Column [emp_no] has conflicting data types in subqueries: [integer, long]"));
    }

    /**
     * Verifies that FROM subqueries with conflicting types on the right side fail without casting.
     */
    public void testRejectsFromSubqueryUnionTypeRightField() {
        errorWithIncompatible("""
            FROM employees
            | WHERE emp_no IN (FROM employees, (FROM employees_incompatible | KEEP emp_no) | KEEP emp_no)
            | KEEP emp_no
            """, containsString("Column [emp_no] has conflicting data types in subqueries: [integer, long]"));
    }

    // -- IN subquery in processing commands (rejected by analyzer) --

    /**
     * Verifies that an IN subquery inside SORT is rejected.
     */
    public void testRejectsInSubqueryInSort() {
        errorInSubquery("""
            FROM employees
            | SORT emp_no IN (FROM employees | KEEP emp_no)
            """, containsString("IN subquery is not supported in [SORT emp_no IN (FROM employees | KEEP emp_no)]"));
    }

    /**
     * Verifies that a NOT IN subquery inside SORT is rejected.
     */
    public void testRejectsNotInSubqueryInSort() {
        errorInSubquery("""
            FROM employees
            | SORT emp_no NOT IN (FROM employees | KEEP emp_no)
            """, containsString("IN subquery is not supported in [SORT emp_no NOT IN (FROM employees | KEEP emp_no)]"));
    }

    /**
     * Verifies that an IN subquery in STATS BY clause is rejected.
     */
    public void testRejectsInSubqueryInStatsBy() {
        errorInSubquery("""
            FROM employees
            | STATS cnt = COUNT(*) BY emp_no IN (FROM employees | KEEP emp_no)
            """, containsString("IN subquery is not supported in [STATS cnt = COUNT(*) BY emp_no IN (FROM employees | KEEP emp_no)]"));
    }

    /**
     * Verifies that a NOT IN subquery in STATS BY clause is rejected.
     */
    public void testRejectsNotInSubqueryInStatsBy() {
        errorInSubquery("""
            FROM employees
            | STATS cnt = COUNT(*) BY emp_no NOT IN (FROM employees | KEEP emp_no)
            """, containsString("IN subquery is not supported in [STATS cnt = COUNT(*) BY emp_no NOT IN (FROM employees | KEEP emp_no)]"));
    }

    /**
     * Verifies that an IN subquery in LIMIT BY clause is rejected.
     */
    public void testRejectsInSubqueryInLimitBy() {
        errorInSubquery("""
            FROM employees
            | SORT emp_no
            | LIMIT 10 BY emp_no IN (FROM employees | KEEP emp_no)
            """, containsString("IN subquery is not supported in [LIMIT 10 BY emp_no IN (FROM employees | KEEP emp_no)]"));
    }

    /**
     * Verifies that a NOT IN subquery in LIMIT BY clause is rejected.
     */
    public void testRejectsNotInSubqueryInLimitBy() {
        errorInSubquery("""
            FROM employees
            | SORT emp_no
            | LIMIT 10 BY emp_no NOT IN (FROM employees | KEEP emp_no)
            """, containsString("IN subquery is not supported in [LIMIT 10 BY emp_no NOT IN (FROM employees | KEEP emp_no)]"));
    }

    /**
     * Verifies that an IN subquery inside EVAL with multiple fields (one being the IN subquery) is rejected.
     */
    public void testRejectsInSubqueryInEvalAmongMultipleFields() {
        errorInSubquery(
            """
                FROM employees
                | EVAL a = 1, is_match = emp_no IN (FROM employees | KEEP emp_no), b = salary
                """,
            containsString("IN subquery is not supported in [EVAL a = 1, is_match = emp_no IN (FROM employees | KEEP emp_no), b = salary]")
        );
    }

    /**
     * Verifies that an IN subquery as a function argument inside EVAL is rejected.
     * The InSubquery inside COALESCE is unresolved, and the verifier reports
     * that IN/NOT IN subquery is not supported in Eval.
     */
    public void testRejectsInSubqueryAsFunctionArgInEval() {
        errorInSubquery(
            """
                FROM employees
                | EVAL result = COALESCE(emp_no IN (FROM employees | KEEP emp_no), false)
                """,
            containsString("IN subquery is not supported in [EVAL result = COALESCE(emp_no IN (FROM employees | KEEP emp_no), false)]")
        );
    }

    // -- IN subquery nested in WHERE expressions --

    /**
     * Verifies that an IN subquery nested inside a CASE function in WHERE is rejected.
     * The analyzer cannot extract InSubquery from inside a function call.
     */
    public void testRejectsInSubqueryInCaseFunctionInWhere() {
        errorInSubquery(
            """
                FROM employees
                | WHERE CASE(emp_no IN (FROM employees | KEEP emp_no), true, false)
                """,
            containsString(
                "IN subquery is not supported within other expressions [CASE(emp_no IN (FROM employees | KEEP emp_no), true, false)]"
            )
        );
    }

    /**
     * Verifies that an IN subquery wrapped in IS NOT NULL in WHERE is rejected.
     * The analyzer cannot extract InSubquery from inside IS NULL expressions.
     */
    public void testRejectsInSubqueryInIsNullInWhere() {
        errorInSubquery(
            """
                FROM employees
                | WHERE (emp_no IN (FROM employees | KEEP emp_no)) IS NOT NULL
                """,
            containsString("IN subquery is not supported within other expressions [(emp_no IN (FROM employees | KEEP emp_no)) IS NOT NULL]")
        );
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    @Override
    protected boolean enableWarningsCheck() {
        // Some tests call Approximation.verifyPlan which adds header warnings that can't be consumed in unit tests
        return false;
    }

    // -- helpers --

    private static LogicalPlan analyzeInSubquery(String query) {
        return analyzer().addEmployees().query(query);
    }

    private static void errorInSubquery(String query, Matcher<String> messageMatcher) {
        analyzer().addEmployees().error(query, messageMatcher);
    }

    private static void errorWithK8s(String query, Matcher<String> messageMatcher) {
        analyzer().addK8s().error(query, messageMatcher);
    }

    private static void errorWithK8sDownsampled(String query, Matcher<String> messageMatcher) {
        analyzer().addK8sDownsampled().error(query, messageMatcher);
    }

    private static void errorWithAllTypes(String query, Matcher<String> messageMatcher) {
        analyzer().addIndex("all_types", "mapping-all-types.json").error(query, messageMatcher);
    }

    private static void errorWithIncompatible(String query, Matcher<String> messageMatcher) {
        analyzer().addEmployees().addIndex("employees_incompatible", "mapping-default-incompatible.json").error(query, messageMatcher);
    }

    private static IndexResolution unionIndexResolution() {
        LinkedHashMap<String, Set<String>> typesToIndices = new LinkedHashMap<>();
        typesToIndices.put("keyword", Set.of("union_index_1"));
        typesToIndices.put("integer", Set.of("union_index_2"));
        EsField idField = new InvalidMappedField("id", typesToIndices);
        EsField nameField = new EsField("name", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE);
        EsIndex index = new EsIndex(
            "union_index*",
            Map.of("id", idField, "name", nameField),
            Map.of("union_index_1", IndexMode.STANDARD, "union_index_2", IndexMode.STANDARD),
            Map.of(),
            Map.of()
        );
        return IndexResolution.valid(index);
    }

    private static void errorWithUnionIndex(String query, Matcher<String> messageMatcher) {
        analyzer().addEmployees().addIndex(unionIndexResolution()).error(query, messageMatcher);
    }

    private void assertApproximationRejects(String query) {
        LogicalPlan plan = analyzeInSubquery(query);
        // verifyPlan returns null when the plan is incompatible with approximation (and adds a warning)
        assertThat(
            "Approximation should reject this query",
            ApproximationVerifier.verifyPlan(plan, TransportVersion.current()),
            nullValue()
        );
    }
}
