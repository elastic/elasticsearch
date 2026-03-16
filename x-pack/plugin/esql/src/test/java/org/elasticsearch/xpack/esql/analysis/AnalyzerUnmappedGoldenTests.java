/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.optimizer.UnmappedGoldenTestCase;

import java.util.EnumSet;
import java.util.List;

/**
 * Golden tests for analyzer behavior with unmapped fields using SET unmapped_fields="nullify" and "load".
 * These tests verify that unmapped fields are properly handled.
 */
public class AnalyzerUnmappedGoldenTests extends UnmappedGoldenTestCase {
    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.ANALYSIS);

    public void testKeep() throws Exception {
        runTests("""
            FROM employees
            | keep does_not_exist_field
            """);
    }

    public void testKeepRepeated() throws Exception {
        runTests("""
            FROM employees
            | KEEP does_not_exist_field, does_not_exist_field
            """);
    }

    public void testKeepAndMatchingStar() throws Exception {
        runTests("""
            FROM employees
            | KEEP emp_*, does_not_exist_field
            """);
    }

    public void testEvalAndKeep() throws Exception {
        runTests("""
            FROM employees
            | EVAL x = does_not_exist_field1::INTEGER + 42
            | KEEP does_not_exist_field1, does_not_exist_field2
            """);
    }

    public void testEvalAfterKeepStar() throws Exception {
        runTests("""
            FROM employees
            | KEEP *
            | EVAL x = emp_no + 1
            | EVAL y = does_not_exist_field::DOUBLE + 2
            """);
    }

    public void testEvalAfterMatchingKeepWithWildcard() throws Exception {
        runTests("""
            FROM employees
            | KEEP emp_no, *
            | EVAL x = emp_no + 1
            | EVAL y = emp_does_not_exist_field::DOUBLE + 2
            """);
    }

    public void testDrop() throws Exception {
        for (Tuple<String, String> drop : List.of(
            Tuple.tuple("empty", ""),
            Tuple.tuple("exists", ", emp_no"),
            Tuple.tuple("same_field", ", does_not_exist_field"),
            Tuple.tuple("another_field", ", does_not_exist_field2")
        )) {
            runTests("FROM employees | DROP does_not_exist_field" + drop.v2(), drop.v1());
        }
    }

    public void testDropWithMatchingStar() throws Exception {
        runTests("""
            FROM employees
            | DROP emp_*, does_not_exist_field
            """);
    }

    public void testRename() throws Exception {
        runTests("""
            FROM employees
            | RENAME does_not_exist_field AS now_it_does, emp_no AS employee_number
            """);
    }

    public void testRenameShadowed() throws Exception {
        runTests("""
            FROM employees
            | RENAME does_not_exist_field AS now_it_does, neither_does_this AS now_it_does, emp_no AS employee_number
            """);
    }

    public void testEvalAfterRename() throws Exception {
        runTests("""
            FROM employees
            | RENAME emp_no AS employee_number
            | EVAL x = does_not_exist::DOUBLE + 1
            """);
    }

    public void testEval() throws Exception {
        runTests("""
            FROM employees
            | EVAL x = does_not_exist_field::DOUBLE + 1
            """);
    }

    public void testMultipleEval() throws Exception {
        runTests("""
            FROM employees
            | EVAL a = 1
            | EVAL x = a + b::DOUBLE
            | EVAL y = b::DOUBLE + c::DOUBLE
            """);
    }

    public void testCasting() throws Exception {
        runTests("""
            FROM employees
            | EVAL x = does_not_exist_field::LONG
            """);
    }

    public void testCastingNoAliasing() throws Exception {
        runTests("""
            FROM employees
            | EVAL does_not_exist_field::LONG
            """);
    }

    public void testShadowingAfterEval() throws Exception {
        runTests("""
            FROM employees
            | EVAL x = does_not_exist_field::DOUBLE + 1
            | EVAL does_not_exist_field = 42
            """);
    }

    public void testShadowingAfterKeep() throws Exception {
        runTests("""
            FROM employees
            | KEEP does_not_exist_field
            | EVAL does_not_exist_field = 42
            """);
    }

    public void testStatsAgg() throws Exception {
        runTests("""
            FROM employees
            | STATS cnt = COUNT(does_not_exist_field)
            """);
    }

    public void testStatsGroup() throws Exception {
        runTests("""
            FROM employees
            | STATS BY does_not_exist_field
            """);
    }

    public void testDoesNotExistAfterInlineStats() throws Exception {
        runTests("""
            FROM employees
            | INLINE STATS COUNT(*) BY emp_no
            | EVAL x = does_not_exist_field
            """);
    }

    public void testStatsAggAndGroup() throws Exception {
        runTests("""
            FROM employees
            | STATS s = SUM(does_not_exist1::DOUBLE) BY does_not_exist2
            """);
    }

    public void testStatsAggAndAliasedGroup() throws Exception {
        runTests("""
            FROM employees
            | STATS s = SUM(does_not_exist1::DOUBLE) + d2 BY d2 = does_not_exist2::DOUBLE, emp_no
            """);
    }

    public void testStatsAggAndAliasedGroupWithExpression() throws Exception {
        runTests("""
            FROM employees
            | STATS sum = SUM(does_not_exist1::DOUBLE) + s0 + s1 BY s0 = does_not_exist2::DOUBLE + does_not_exist3::DOUBLE, s1 = emp_no
            """);
    }

    public void testStatsMixed() throws Exception {
        runTests("""
            FROM employees
            | STATS s = SUM(does_not_exist1::DOUBLE), c = COUNT(*) BY does_not_exist2, emp_no
            """);
    }

    public void testStatsMixedAndExpressions() throws Exception {
        runTestsNullifyOnly("""
            FROM employees
            | STATS s = SUM(does_not_exist1) + does_not_exist2, c = COUNT(*) BY does_not_exist3, emp_no, does_not_exist2
            """, STAGES);
    }

    public void testInlineStats() throws Exception {
        runTests("""
            FROM employees
            | STATS s = SUM(does_not_exist1::DOUBLE) BY does_not_exist2
            """);
    }

    public void testInlineStatsMixed() throws Exception {
        runTests("""
            FROM employees
            | INLINE STATS s = SUM(does_not_exist1::DOUBLE), c = COUNT(*) BY does_not_exist2, emp_no
            """);
    }

    public void testWhere() throws Exception {
        runTests("""
            FROM employees
            | WHERE does_not_exist::LONG > 0
            """);
    }

    public void testWhereOr() throws Exception {
        runTests("""
            FROM employees
            | WHERE does_not_exist::LONG > 0 OR emp_no > 0
            """);
    }

    public void testWhereComplex() throws Exception {
        runTests("""
            FROM employees
            | WHERE does_not_exist1::LONG > 0 OR emp_no > 0 AND does_not_exist2::LONG < 100
            """);
    }

    public void testAggsFiltering() throws Exception {
        runTests("""
            FROM employees
            | STATS c = COUNT(*) WHERE does_not_exist1::LONG > 0
            """);
    }

    public void testAggsFilteringMultipleFields() throws Exception {
        runTests("""
            FROM employees
            | STATS c1 = COUNT(*) WHERE does_not_exist1::LONG > 0 OR emp_no > 0 OR does_not_exist2::LONG < 100,
                    c2 = COUNT(*) WHERE does_not_exist3 IS NULL
            """);
    }

    public void testStatsAggAndAliasedShadowingGroupOverExpression() throws Exception {
        runTests("""
            FROM languages
            | WHERE language_code == 1
            | STATS c = COUNT(*) + language_code
                    BY language_code = does_not_exist1::INTEGER + does_not_exist2::INTEGER + language_code, language_name
            """);
    }

    public void testStatsAggAndAliasedShadowingGroup() throws Exception {
        runTests("""
            FROM languages
            | WHERE language_code == 1
            | STATS c = COUNT(*) BY language_code = does_not_exist, language_name
            """);
    }

    public void testSort() throws Exception {
        runTests("""
            FROM employees
            | SORT does_not_exist ASC
            """);
    }

    public void testSortExpression() throws Exception {
        runTests("""
            FROM employees
            | SORT does_not_exist::LONG + 1
            """);
    }

    public void testSortExpressionMultipleFields() throws Exception {
        runTests("""
            FROM employees
            | SORT does_not_exist1::LONG + 1, does_not_exist2 DESC, emp_no ASC
            """);
    }

    public void testMvExpand() throws Exception {
        runTests("""
            FROM employees
            | MV_EXPAND does_not_exist
            """);
    }

    public void testLookupJoin() throws Exception {
        runTestsNullifyOnly("""
            FROM employees
            | EVAL language_code = does_not_exist :: INTEGER
            | LOOKUP JOIN languages_lookup ON language_code
            """, STAGES);
    }

    public void testLookupJoinWithFilter() throws Exception {
        runTestsNullifyOnly("""
            FROM employees
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | WHERE does_not_exist::LONG > 0
            """, STAGES);
    }

    public void testSubqueryKeepUnmapped() throws Exception {
        runTestsNullifyOnly("""
            FROM employees, (FROM languages | KEEP language_code)
            | KEEP emp_no, language_code, does_not_exist
            """, STAGES);
    }

    public void testSubqueryWithStats() throws Exception {
        runTestsNullifyOnly("""
            FROM employees, (FROM sample_data | STATS max_ts = MAX(@timestamp) BY does_not_exist)
            | KEEP emp_no, max_ts, does_not_exist
            """, STAGES);
    }

    public void testSubqueryKeepMultipleUnmapped() throws Exception {
        runTestsNullifyOnly("""
            FROM employees,
                (FROM languages | KEEP language_code, unmapped1, unmapped2)
            | KEEP emp_no, language_code, unmapped1, unmapped2
            """, STAGES);
    }

    public void testFork() throws Exception {
        runTestsNullifyOnly("""
            FROM employees
            | FORK (WHERE does_not_exist::LONG > 0)
                   (WHERE emp_no > 0)
            """, STAGES);
    }

    public void testForkWithEval() throws Exception {
        runTestsNullifyOnly("""
            FROM employees
            | FORK (EVAL x = does_not_exist::DOUBLE + 1)
                   (EVAL y = emp_no + 1)
            """, STAGES);
    }

    public void testForkWithStats() throws Exception {
        runTestsNullifyOnly("""
            FROM employees
            | FORK (STATS c = COUNT(*) BY does_not_exist)
                   (STATS d = AVG(salary::DOUBLE))
            | SORT does_not_exist
            """, STAGES);
    }

    public void testForkBranchesWithDifferentSchemas() throws Exception {
        runTestsNullifyOnly("""
            FROM employees
            | WHERE first_name == "Chris" AND does_not_exist1::LONG > 5
            | EVAL does_not_exist2 IS NULL
            | FORK (WHERE emp_no > 3 | SORT does_not_exist3 | LIMIT 7 )
                   (WHERE emp_no > 2 | EVAL xyz = does_not_exist4::KEYWORD )
                   (DISSECT first_name "%{d} %{e} %{f}"
                    | STATS x = MIN(d::DOUBLE), y = MAX(e::DOUBLE) WHERE d::DOUBLE > 1000 + does_not_exist5::DOUBLE
                    | EVAL xyz = "abc")
            """, STAGES);
    }

    public void testForkBranchesAfterStats2ndBranch() throws Exception {
        runTestsNullifyOnly("""
            FROM employees
            | WHERE does_not_exist1 IS NULL
            | FORK (STATS c = COUNT(*))
                   (STATS d = AVG(salary) BY does_not_exist2)
            | SORT does_not_exist2
            """, STAGES);
    }

    public void testFuse() throws Exception {
        runTestsNullifyOnly("""
            FROM employees METADATA _score, _index, _id
            | FORK (WHERE does_not_exist::LONG > 0)
                   (WHERE emp_no > 0)
            | FUSE
            """, STAGES);
    }

    public void testFuseWithEval() throws Exception {
        runTestsNullifyOnly("""
            FROM employees METADATA _score, _index, _id
            | FORK (EVAL x = does_not_exist::DOUBLE + 1)
                   (EVAL y = emp_no + 1)
            | FUSE RRF
            """, STAGES);
    }

    public void testFuseLinear() throws Exception {
        runTestsNullifyOnly("""
            FROM employees METADATA _score, _index, _id
            | FORK (WHERE does_not_exist::LONG > 0 | EVAL x = 1)
                   (WHERE emp_no > 0 | EVAL y = 2)
            | FUSE LINEAR
            """, STAGES);
    }

    public void testCoalesce() throws Exception {
        runTests("""
            FROM employees
            | EVAL x = COALESCE(does_not_exist::LONG, emp_no, 0)
            | KEEP emp_no, x
            """);
    }

    public void testEnrich() throws Exception {
        runTestsNullifyOnly("""
            FROM employees
            | EVAL x = does_not_exist::KEYWORD
            | ENRICH languages ON x
            """, STAGES);
    }

    public void testSemanticText() throws Exception {
        runTestsNullifyOnly("""
            FROM employees
            | WHERE KNN(does_not_exist, [0, 1, 2])
            """, STAGES);
    }

    public void testTBucketGroupByUnmapped() throws Exception {
        runTests("""
            FROM sample_data
            | STATS c = COUNT(*) BY tbucket(1 hour), does_not_exist
            """);
    }

    public void testTBucketAggregateUnmapped() throws Exception {
        runTests("""
            FROM sample_data
            | STATS s = SUM(does_not_exist::DOUBLE), c = COUNT(*) BY tbucket(1 day)
            """);
    }

    public void testTimeSeriesRateUnmapped() throws Exception {
        runTestsNullifyOnly("""
            TS k8s
            | STATS r = RATE(does_not_exist) BY tbucket(1 hour)
            """, STAGES);
    }

    public void testRow() throws Exception {
        runTestsNullifyOnly("""
            ROW x = 1
            | EVAL y = does_not_exist_field1::INTEGER + x
            | KEEP *, does_not_exist_field2
            """, STAGES);
    }

    public void testTimeSeriesFirstOverTimeUnmapped() throws Exception {
        runTests("""
            TS k8s
            | STATS f = FIRST_OVER_TIME(does_not_exist::DOUBLE) BY tbucket(1 hour)
            """);
    }

    public void testSubqueryOnly() throws Exception {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        runTestsNullifyOnly("""
            FROM
                (FROM languages
                 | WHERE does_not_exist::LONG > 1)
            """, STAGES);
    }

    public void testDoubleSubqueryOnly() throws Exception {
        assumeTrue(
            "Requires subquery in FROM command support",
            EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_WITHOUT_IMPLICIT_LIMIT.isEnabled()
        );
        runTestsNullifyOnly("""
            FROM
                (FROM languages
                 | WHERE does_not_exist1::LONG > 1),
                (FROM sample_data
                 | WHERE does_not_exist1::DOUBLE > 10.)
            """, STAGES);
    }

    public void testDoubleSubqueryOnlyWithTopFilterAndNoMain() throws Exception {
        assumeTrue(
            "Requires subquery in FROM command support",
            EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_WITHOUT_IMPLICIT_LIMIT.isEnabled()
        );
        runTestsNullifyOnly("""
            FROM
                (FROM languages
                 | WHERE does_not_exist1::LONG > 1),
                (FROM sample_data
                 | WHERE does_not_exist1::DOUBLE > 10.)
            | WHERE does_not_exist2::LONG < 100
            """, STAGES);
    }

    public void testSubqueryAndMainQuery() throws Exception {
        assumeTrue(
            "Requires subquery in FROM command support",
            EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_WITHOUT_IMPLICIT_LIMIT.isEnabled()
        );
        runTestsNullifyOnly("""
            FROM employees,
                (FROM languages
                 | WHERE does_not_exist1::LONG > 1)
            | WHERE does_not_exist2::LONG < 10 AND emp_no > 0
            """, STAGES);
    }

    public void testSubqueryMix() throws Exception {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        runTestsNullifyOnly("""
            FROM
                (FROM employees
                 | EVAL emp_no_plus = emp_no_foo::LONG + 1
                 | WHERE emp_no < 10003)
            | KEEP emp_no*
            | SORT emp_no, emp_no_plus
            """, STAGES);
    }

    public void testSubqueryMixWithDropPattern() throws Exception {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        runTestsNullifyOnly("""
            FROM
                (FROM employees
                 | EVAL emp_no_plus = emp_no_foo::LONG + 1
                 | WHERE emp_no < 10003)
            | DROP *_name
            | SORT emp_no, emp_no_plus
            """, STAGES);
    }

    public void testSubqueryAfterUnionAllOfStats() throws Exception {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        runTestsNullifyOnly("""
            FROM
                (FROM employees
                 | STATS c = COUNT(*) BY does_not_exist)
            | SORT does_not_exist
            """, STAGES);
    }

    public void testSubqueryAfterUnionAllOfStatsAndMain() throws Exception {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        runTestsNullifyOnly("""
            FROM employees,
                (FROM employees | STATS c = count(*))
            | SORT does_not_exist
            """, STAGES);
    }

    public void testSubquerysWithMainAndSameOptional() throws Exception {
        assumeTrue(
            "Requires subquery in FROM command support",
            EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_WITHOUT_IMPLICIT_LIMIT.isEnabled()
        );
        runTestsNullifyOnly("""
            FROM employees,
                (FROM languages
                 | WHERE does_not_exist1::LONG > 1),
                (FROM languages
                 | WHERE does_not_exist1::LONG > 2)
            | WHERE does_not_exist2::LONG < 10 AND emp_no > 0 OR does_not_exist1::LONG < 11
            """, STAGES);
    }

    public void testSubquerysMixAndLookupJoinNullify() throws Exception {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        runTestsNullifyOnly("""
            FROM employees,
                (FROM languages
                 | WHERE language_code > 10
                 | RENAME language_name as languageName),
                (FROM sample_data
                | STATS max(@timestamp)),
                (FROM employees
                | EVAL language_code = languages
                | LOOKUP JOIN languages_lookup ON language_code)
            | WHERE emp_no > 10000 OR does_not_exist1::LONG < 10
            | STATS count(*) BY emp_no, language_code, does_not_exist2
            | RENAME emp_no AS empNo, language_code AS languageCode
            | MV_EXPAND languageCode
            """, STAGES);
    }

    public void testSubquerysMixAndLookupJoinLoad() throws Exception {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        runTestsNullifyOnly("""
            FROM employees,
                (FROM languages
                 | WHERE language_code > 10
                 | RENAME language_name as languageName),
                (FROM sample_data
                | STATS max(@timestamp)),
                (FROM employees
                | EVAL language_code = languages
                | LOOKUP JOIN languages_lookup ON language_code)
            | WHERE emp_no > 10000 OR does_not_exist1::LONG < 10
            | STATS count(*) BY emp_no, language_code, does_not_exist2
            | RENAME emp_no AS empNo, language_code AS languageCode
            | MV_EXPAND languageCode
            """, STAGES);
    }

    public void testSubquerysWithMainAndStatsOnly() throws Exception {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        runTestsNullifyOnly("""
            FROM employees, // adding a "main" index/pattern makes does_not_exist2 & 3 resolved (compared to the same query above, w/o it)
                (FROM languages
                 | STATS c = COUNT(*) BY emp_no, does_not_exist1),
                (FROM languages
                 | STATS a = AVG(salary))
            | WHERE does_not_exist2::LONG < 10
            | EVAL x = does_not_exist3
            """, STAGES);
    }

    public void testPartiallyMappedField() throws Exception {
        runTests("""
            FROM sample_data, partial_mapping_sample_data
            | KEEP @timestamp, message, unmapped_message
            """);
    }

    public void testMappedInOneIndexOnly() throws Exception {
        runTests("""
            FROM sample_data, no_mapping_sample_data
            | KEEP message
            """);
    }

    public void testMappedInOneIndexOnlyCast() throws Exception {
        runTests("""
            FROM sample_data, no_mapping_sample_data
            | EVAL x = message :: LONG
            """);
    }

    public void testMappedToNonKeywordInOneIndexOnly() throws Exception {
        runTests("""
            FROM sample_data, no_mapping_sample_data
            | KEEP event_duration
            """);
    }

    public void testForkBranchesAfterStats1stBranch() throws Exception {
        runTestsNullifyOnly("""
            FROM employees
            | WHERE does_not_exist1 IS NULL
            | FORK (STATS c = COUNT(*) BY does_not_exist2)
                   (STATS d = AVG(salary))
            | SORT does_not_exist2
            """, STAGES);
    }

    /**
     * Reproducer for https://github.com/elastic/elasticsearch/issues/142968
     * KQL (and QSTR) functions should be allowed in WHERE immediately after FROM,
     * even when an unmapped field is referenced later in the query.
     */
    public void testKqlWithUnmappedFieldInEval() throws Exception {
        // This should NOT throw a verification exception.
        // The KQL function is correctly placed in a WHERE directly after FROM.
        runTestsNullifyOnly("""
            FROM employees
            | WHERE kql("first_name: test")
            | EVAL x = does_not_exist_field + 1
            """, STAGES);
    }

    /**
     * Reproducer for https://github.com/elastic/elasticsearch/issues/142959
     * QSTR functions should be allowed after SORT, even when an unmapped field is used later.
     */
    public void testQstrAfterSortWithUnmappedField() throws Exception {
        runTestsNullifyOnly("""
            FROM employees
            | SORT first_name
            | WHERE qstr("first_name: test")
            | EVAL x = does_not_exist_field + 1
            """, STAGES);
    }

    public void testForkWithRow() throws Exception {
        runTestsNullifyOnly("""
            ROW a = 1
            | FORK (where true)
            | WHERE a == 1
            | KEEP bar
            """, STAGES);
    }

    public void testForkWithFrom() throws Exception {
        runTestsNullifyOnly("""
            FROM employees
            | FORK (where foo != 84) (where true)
            | WHERE _fork == "fork1"
            | DROP _fork
            | eval y = coalesce(bar, baz)
            """, STAGES);
    }

    public void testForkWithUnmappedStatsEvalKeep() throws Exception {
        runTestsNullifyOnly("""
            FROM employees
            | keep emp_no
            | FORK (where true | mv_expand emp_no)
            | stats emp_no = count(*)
            | eval x = least(emp_no, 52, 60)
            | keep emp_no
            """, STAGES);
    }

    public void testForkWithUnmappedStatsEvalKeepTwoBranches() throws Exception {
        assumeTrue("sample must be enabled", EsqlCapabilities.Cap.SAMPLE_V3.isEnabled());
        runTestsNullifyOnly("""
            FROM employees
            | keep emp_no
            | FORK (where true | mv_expand emp_no) (where true | SAMPLE 0.5)
            | stats emp_no = count(*)
            | eval x = least(emp_no, 52, 60)
            | keep emp_no
            """, STAGES);
    }

    public void testForkWithRowCoalesceAndDrop() throws Exception {
        runTestsNullifyOnly("""
            ROW a = 12::long
            | fork (where true)
            | eval x = Coalesce(a, 5)
            | drop a
            """, STAGES);
    }

    /**
     * Reproducer for https://github.com/elastic/elasticsearch/issues/141870
     * ResolveRefs processes the EVAL only after ImplicitCasting processes the implicit cast in the WHERE.
     * This means that ResolveUnmapped will see the EVAL with a yet-to-be-resolved reference to nanos.
     * It should not treat it as unmapped, because there is clearly a nanos attribute in the EVAL's input.
     */
    public void testDoNotResolveUnmappedFieldPresentInChildren() throws Exception {
        runTests("""
            ROW millis = "1970-01-01T00:00:00Z"::date, nanos = "1970-01-01T00:00:00Z"::date_nanos
            | SORT millis ASC
            | WHERE millis < "2000-01-01"
            | EVAL nanos = MV_MIN(nanos)
            """);
    }

    /**
     * Reproducer for https://github.com/elastic/elasticsearch/issues/143991
     * Unmapped fields with dotted names (e.g. host.entity.id) should be nullified in STATS WHERE, even when an EVAL before the STATS
     * creates a field whose name is a suffix of the unmapped field name (e.g. entity.id).
     */
    public void testStatsFilteredAggAfterEvalWithDottedUnmappedField() throws Exception {
        runTestsNullifyOnly("""
            ROW x = 1
            | EVAL entity.id = "foo"
            | STATS host.entity.id = VALUES(host.entity.id) WHERE host.entity.id IS NOT NULL BY entity.id
            """, STAGES);
    }

    /**
     * Reproducer for https://github.com/elastic/elasticsearch/issues/143991
     * Same as {@link #testStatsFilteredAggAfterEvalWithDottedUnmappedField()} but with FROM instead of ROW.
     * Tests both nullify and load modes: the plan shape is the same, only the field type in the EsRelation differs.
     */
    public void testStatsFilteredAggAfterEvalWithDottedUnmappedFieldFromIndex() throws Exception {
        runTests("""
            FROM employees
            | EVAL entity.id = "foo"
            | STATS host.entity.id = VALUES(host.entity.id) WHERE host.entity.id IS NOT NULL BY entity.id
            """);
    }

    public void testSingleSubquery() throws Exception {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        // A single subquery without a main index is merged into the main query during analysis,
        // so there is no Subquery node in the plan and no branching — this is allowed in load.
        runTests("FROM (FROM languages | WHERE language_code > 1)");
    }

    private void runTests(String query) {
        runTestsNullifyAndLoad(query, STAGES);
    }

    private void runTests(String query, String... nestedPaths) {
        runTestsNullifyAndLoad(query, STAGES, nestedPaths);
    }
}
