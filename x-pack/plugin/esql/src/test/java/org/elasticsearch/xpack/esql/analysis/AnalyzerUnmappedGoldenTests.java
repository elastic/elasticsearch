/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.type.CompactMultiTypeEsField;
import org.elasticsearch.xpack.esql.expression.function.aggregate.DimensionValues;
import org.elasticsearch.xpack.esql.optimizer.UnmappedGoldenTestCase;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;

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

    // A pattern DROP that doesn't match the missing field still lets nullify inject it for a later KEEP.
    public void testDropPatternThenKeepMissing() throws Exception {
        runTestsNullifyOnly("""
            FROM employees
            | DROP emp_*
            | KEEP does_not_exist_field
            """, STAGES);
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

    public void testEvalReplacesUnmappedFieldFromEmptyMapping() throws Exception {
        runTestsLoadOnly("""
            FROM no_mapping_date_extract_fields
            | EVAL date_string = date_string::date
            """, STAGES);
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

    public void testInlineStatsAggAndGroup() throws Exception {
        runTests("""
            FROM employees
            | INLINE STATS s = SUM(does_not_exist1::DOUBLE) BY does_not_exist2
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

    // does_not_exist is referenced only in the outer KEEP and is unmapped in every branch source: it is loaded from _source in all
    // branches (#142033, "referenced after subqueries"), exactly as "FROM idx1, idx2 | KEEP missing" loads it from every index.
    public void testSubqueryKeepUnmapped() throws Exception {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        runTests("""
            FROM employees, (FROM languages | KEEP language_code)
            | KEEP emp_no, language_code, does_not_exist
            """);
    }

    // does_not_exist is referenced inside the sample_data subquery (STATS grouping): under load it is loaded into that branch's
    // source and null-filled in the employees branch (Decision A).
    public void testSubqueryWithStats() throws Exception {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        runTests("""
            FROM employees, (FROM sample_data | STATS max_ts = MAX(@timestamp) BY does_not_exist)
            | KEEP emp_no, max_ts, does_not_exist
            """);
    }

    // unmapped1/unmapped2 are referenced inside the languages subquery (KEEP): under load they are loaded into that branch's source
    // and null-filled in the employees branch (Decision A).
    public void testSubqueryKeepMultipleUnmapped() throws Exception {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        runTests("""
            FROM employees,
                (FROM languages | KEEP language_code, unmapped1, unmapped2)
            | KEEP emp_no, language_code, unmapped1, unmapped2
            """);
    }

    public void testFork() throws Exception {
        runTests("""
            FROM employees
            | FORK (WHERE does_not_exist::LONG > 0)
                   (WHERE emp_no > 0)
            """);
    }

    public void testForkLoadsUnmappedFieldReferencedInOneBranch() throws Exception {
        runTests("""
            FROM partial_mapping_sample_data
            | FORK (WHERE unmapped_message == "Disconnection error")
                   (WHERE message == "42")
            | KEEP _fork, message, unmapped_message, unmapped_event_duration
            | SORT _fork, unmapped_event_duration
            """);
    }

    public void testForkLoadsUnmappedFieldWhenSiblingBranchAlignsAnotherColumn() throws Exception {
        runTests("""
            FROM partial_mapping_sample_data
            | FORK (WHERE unmapped_message == "Disconnection error")
                   (EVAL branch_tag = "two")
            | KEEP _fork, message, unmapped_message, branch_tag
            | SORT _fork, message
            """);
    }

    public void testForkLoadsUnmappedFieldKeptInOneBranchOnly() throws Exception {
        runTests("""
            FROM partial_mapping_sample_data
            | FORK (KEEP message, unmapped_message)
                   (WHERE message == "42")
            | KEEP _fork, message, unmapped_message
            | SORT _fork, message
            """);
    }

    // MV_EXPAND makes unmapped_message a ReferenceAttribute in branch 1's output; the sibling WHERE branch must still load it
    // (matched by name, not the transformed type) since all FORK branches share one source. #142033
    public void testForkLoadsUnmappedFieldExpandedInOneBranchOnly() throws Exception {
        runTests("""
            FROM partial_mapping_sample_data
            | FORK (MV_EXPAND unmapped_message)
                   (WHERE message == "42")
            | KEEP _fork, message, unmapped_message
            | SORT _fork, message, unmapped_message
            """);
    }

    public void testForkRenamesUnmappedFieldInOneBranch() throws Exception {
        runTests("""
            FROM partial_mapping_sample_data
            | FORK (WHERE unmapped_message == "Disconnection error")
                   (RENAME unmapped_message AS msg)
            | KEEP _fork, message, unmapped_message, msg
            | SORT _fork, message
            """);
    }

    // does_not_exist is referenced only in the WHERE branch; the LEFT LOOKUP JOIN branch still loads it into its left source and
    // flows it through the join rather than null-filling. #142033
    public void testForkLoadsUnmappedFieldAcrossLookupJoinBranch() throws Exception {
        runTests("""
            FROM employees
            | EVAL language_code = languages
            | FORK (LOOKUP JOIN languages_lookup ON language_code)
                   (WHERE does_not_exist::KEYWORD == "x")
            | KEEP _fork, emp_no, language_name, does_not_exist
            | SORT _fork, emp_no
            """);
    }

    // The LOOKUP JOIN branch loads does_not_exist across (into its left source), the WHERE branch loads it directly, and the STATS branch
    // null-fills it because an aggregation drops non-grouped fields - exercising load-through-join, load-direct and null-fill in one FORK.
    public void testForkLoadsUnmappedFieldAcrossLookupJoinAndStatsBranches() throws Exception {
        runTests("""
            FROM employees
            | EVAL language_code = languages
            | FORK (LOOKUP JOIN languages_lookup ON language_code)
                   (WHERE does_not_exist::KEYWORD == "x")
                   (STATS c = COUNT(*) BY emp_no)
            | KEEP _fork, emp_no, language_name, does_not_exist, c
            | SORT _fork, emp_no
            """);
    }

    // gender is a two-legged PUNK (TEXT in employees_gender_text, unmapped in employees_no_gender); a FORK output must preserve its
    // TEXT type, not flag it UNSUPPORTED.
    public void testForkKeepsSingleTypePartiallyUnmappedTextField() throws Exception {
        runTests("""
            FROM employees_gender_text, employees_no_gender
            | KEEP gender
            | FORK (WHERE true)
                   (WHERE true)
            | KEEP _fork, gender
            | SORT _fork
            """);
    }

    // id is short in apps_short and unmapped in partial_mapping_sample_data, so it is a single-type partially-unmapped (two-legged PUNK)
    // small numeric
    public void testForkWidensSingleTypePartiallyUnmappedShortField() throws Exception {
        runTests("""
            FROM apps_short, partial_mapping_sample_data
            | KEEP id
            | FORK (WHERE true)
                   (WHERE true)
            | KEEP _fork, id
            | SORT _fork
            """, CompactMultiTypeEsField.CompactMultiTypeEsField);
    }

    // UnionAll counterpart of testForkWidensSingleTypePartiallyUnmappedShortField: id (two-legged short PUNK) must surface as INTEGER
    // on the UnionAll output, so the widening fix applies to UnionAll/views too, not just Fork.
    public void testSubqueryWidensSingleTypePartiallyUnmappedShortField() throws Exception {
        // Both branches make id a two-legged short PUNK and only KEEP it; branches and UnionAll output must agree on the widened INTEGER
        // type, else checkUnionAll reports [INTEGER] vs [SHORT]. (Plain short avoided: subqueries don't auto-widen numerics.)
        runTests("""
            FROM (FROM apps_short, partial_mapping_sample_data | KEEP id),
                 (FROM apps_short, partial_mapping_sample_data | KEEP id)
            | KEEP id
            | SORT id NULLS LAST
            | LIMIT 5
            """, CompactMultiTypeEsField.CompactMultiTypeEsField);
    }

    // A genuine multi-type conflict (short/long/unmapped) is not a two-legged PUNK (types > 1), so it stays UNSUPPORTED through the
    // FORK output; KEEP-only is tolerated (checkFork skips it).
    public void testForkThreeWayTypeConflictShortLongUnmappedStaysUnsupported() throws Exception {
        runTests("""
            FROM all_types, all_types_short_as_long, all_types_no_short
            | KEEP short
            | FORK (WHERE true)
                   (WHERE true)
            | KEEP _fork, short
            | SORT _fork
            """);
    }

    public void testForkWithEval() throws Exception {
        runTests("""
            FROM employees
            | FORK (EVAL x = does_not_exist::DOUBLE + 1)
                   (EVAL y = emp_no + 1)
            """);
    }

    public void testForkWithStats() throws Exception {
        runTests("""
            FROM employees
            | FORK (STATS c = COUNT(*) BY does_not_exist)
                   (STATS d = AVG(salary::DOUBLE))
            | SORT does_not_exist
            """);
    }

    public void testForkBranchesWithDifferentSchemas() throws Exception {
        runTests("""
            FROM employees
            | WHERE first_name == "Chris" AND does_not_exist1::LONG > 5
            | EVAL does_not_exist2 IS NULL
            | FORK (WHERE emp_no > 3 | SORT does_not_exist3 | LIMIT 7 )
                   (WHERE emp_no > 2 | EVAL xyz = does_not_exist4::KEYWORD )
                   (DISSECT first_name "%{d} %{e} %{f}"
                    | STATS x = MIN(d::DOUBLE), y = MAX(e::DOUBLE) WHERE d::DOUBLE > 1000 + does_not_exist5::DOUBLE
                    | EVAL xyz = "abc")
            """);
    }

    public void testForkBranchesAfterStats2ndBranch() throws Exception {
        runTests("""
            FROM employees
            | WHERE does_not_exist1 IS NULL
            | FORK (STATS c = COUNT(*))
                   (STATS d = AVG(salary) BY does_not_exist2)
            | SORT does_not_exist2
            """);
    }

    public void testFuse() throws Exception {
        runTestsNullifyOnly("""
            FROM employees METADATA _score, _index, _id
            | FORK (WHERE does_not_exist::LONG > 0)
                   (WHERE emp_no > 0)
            | LIMIT 100
            | FUSE
            """, STAGES);
    }

    public void testFuseWithEval() throws Exception {
        runTestsNullifyOnly("""
            FROM employees METADATA _score, _index, _id
            | FORK (EVAL x = does_not_exist::DOUBLE + 1)
                   (EVAL y = emp_no + 1)
            | LIMIT 100
            | FUSE RRF
            """, STAGES);
    }

    public void testFuseLinear() throws Exception {
        runTestsNullifyOnly("""
            FROM employees METADATA _score, _index, _id
            | FORK (WHERE does_not_exist::LONG > 0 | EVAL x = 1)
                   (WHERE emp_no > 0 | EVAL y = 2)
            | LIMIT 100
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
            """, STAGES, DimensionValues.DIMENSION_VALUES_VERSION);
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
            """, DimensionValues.DIMENSION_VALUES_VERSION);
    }

    // Single subquery without a main index is merged during analysis (no UnionAll), so does_not_exist is loaded into the merged
    // source - the linear/FORK path, unchanged by Step 2.
    public void testSubqueryOnly() throws Exception {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        runTests("""
            FROM
                (FROM languages
                 | WHERE does_not_exist::LONG > 1)
            """);
    }

    // does_not_exist1 is referenced inside both branches: under load it is loaded into each branch's own source (Decision A); the
    // differing casts apply to the WHERE predicate only, so both branches still surface a keyword and there is no type conflict.
    public void testDoubleSubqueryOnly() throws Exception {
        assumeTrue(
            "Requires subquery in FROM command support",
            EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_WITHOUT_IMPLICIT_LIMIT.isEnabled()
        );
        runTests("""
            FROM
                (FROM languages
                 | WHERE does_not_exist1::LONG > 1),
                (FROM sample_data
                 | WHERE does_not_exist1::DOUBLE > 10.)
            """);
    }

    // does_not_exist1 is referenced inside each branch, so it is loaded into each branch's own source (in-branch scope); does_not_exist2
    // is referenced only in the outer WHERE and is unmapped in every branch, so it is loaded from _source in all branches (#142033).
    public void testDoubleSubqueryOnlyWithTopFilterAndNoMain() throws Exception {
        assumeTrue(
            "Requires subquery in FROM command support",
            EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_WITHOUT_IMPLICIT_LIMIT.isEnabled()
        );
        runTests("""
            FROM
                (FROM languages
                 | WHERE does_not_exist1::LONG > 1),
                (FROM sample_data
                 | WHERE does_not_exist1::DOUBLE > 10.)
            | WHERE does_not_exist2::LONG < 100
            """);
    }

    // does_not_exist1 is in-branch (loaded only in the languages branch, null-filled in employees); does_not_exist2 is outer-only and
    // unmapped everywhere, so loaded from _source in all branches. #142033
    public void testSubqueryAndMainQuery() throws Exception {
        assumeTrue(
            "Requires subquery in FROM command support",
            EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_WITHOUT_IMPLICIT_LIMIT.isEnabled()
        );
        runTests("""
            FROM employees,
                (FROM languages
                 | WHERE does_not_exist1::LONG > 1)
            | WHERE does_not_exist2::LONG < 10 AND emp_no > 0
            """);
    }

    // Outer-only reference over a union of an index branch and a ROW branch: does_not_exist loads from _source into the employees
    // EsRelation, while the ROW branch (can't load) is null-filled by resolveFork alignment. #142033
    public void testSubqueryWithRowBranchOuterReference() throws Exception {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        assumeTrue("Requires ROW source subqueries", EsqlCapabilities.Cap.SUBQUERY_WITH_ROW.isEnabled());
        runTests("""
            FROM employees, (ROW synthetic = 1)
            | KEEP emp_no, synthetic, does_not_exist
            """);
    }

    // Single subquery merged during analysis (no UnionAll): emp_no_foo is loaded into the merged source (linear path).
    public void testSubqueryMix() throws Exception {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        runTests("""
            FROM
                (FROM employees
                 | EVAL emp_no_plus = emp_no_foo::LONG + 1
                 | WHERE emp_no < 10003)
            | KEEP emp_no*
            | SORT emp_no, emp_no_plus
            """);
    }

    // Single subquery merged during analysis (no UnionAll): emp_no_foo is loaded into the merged source (linear path).
    public void testSubqueryMixWithDropPattern() throws Exception {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        runTests("""
            FROM
                (FROM employees
                 | EVAL emp_no_plus = emp_no_foo::LONG + 1
                 | WHERE emp_no < 10003)
            | DROP *_name
            | SORT emp_no, emp_no_plus
            """);
    }

    // Single subquery merged during analysis (no UnionAll): does_not_exist is loaded into the merged source (linear path).
    public void testSubqueryAfterUnionAllOfStats() throws Exception {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        runTests("""
            FROM
                (FROM employees
                 | STATS c = COUNT(*) BY does_not_exist)
            | SORT does_not_exist
            """);
    }

    // does_not_exist is outer-only and unmapped everywhere, so loaded in all branches (#142033): the main branch surfaces it; the STATS
    // branch loads it but STATS drops it, so it null-fills at the union.
    public void testSubqueryAfterUnionAllOfStatsAndMain() throws Exception {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        runTests("""
            FROM employees,
                (FROM employees | STATS c = count(*))
            | SORT does_not_exist
            """);
    }

    // does_not_exist1 is referenced inside both language branches (loaded there, in-branch scope) and again in the outer WHERE (resolves
    // via the union output); does_not_exist2 is outer-only and unmapped everywhere, so it is loaded from _source in all branches (#142033).
    public void testSubquerysWithMainAndSameOptional() throws Exception {
        assumeTrue(
            "Requires subquery in FROM command support",
            EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_WITHOUT_IMPLICIT_LIMIT.isEnabled()
        );
        runTests("""
            FROM employees,
                (FROM languages
                 | WHERE does_not_exist1::LONG > 1),
                (FROM languages
                 | WHERE does_not_exist1::LONG > 2)
            | WHERE does_not_exist2::LONG < 10 AND emp_no > 0 OR does_not_exist1::LONG < 11
            """);
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

    // Nullify-only: under load, salary loads as KEYWORD inside AVG(salary), which AVG rejects (numeric required) - a legitimate
    // load-mode semantic unrelated to the subquery scoping under test.
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

    public void testTypeConflictTimeseriesLongUnmappedWithCast() throws Exception {
        runTests("""
            FROM k8s, k8s_unmapped
            | EVAL bytes = network.bytes_in::long
            | KEEP bytes
            """, CompactMultiTypeEsField.CompactMultiTypeEsField);
    }

    public void testTSTypeConflictTimeseriesLongUnmappedWithCast() throws Exception {
        runTests("""
            TS k8s, k8s_unmapped
            | EVAL bytes = network.bytes_in::long
            | KEEP bytes
            """, CompactMultiTypeEsField.CompactMultiTypeEsField);
    }

    public void testTypeConflictTimeseriesDoubleUnmappedWithCast() throws Exception {
        runTests("""
            FROM k8s, k8s_unmapped
            | EVAL cost = network.cost::double
            | KEEP cost
            """, CompactMultiTypeEsField.CompactMultiTypeEsField);
    }

    public void testTypeConflictTimeseriesStatsWithCast() throws Exception {
        runTests("""
            FROM k8s, k8s_unmapped
            | STATS s = SUM(network.bytes_in::long) BY cluster
            """, CompactMultiTypeEsField.CompactMultiTypeEsField);
    }

    public void testTSTypeConflictTimeseriesStatsWithCast() throws Exception {
        runTests("""
            TS k8s, k8s_unmapped
            | STATS s = SUM(network.bytes_in::long) BY cluster
            """, CompactMultiTypeEsField.CompactMultiTypeEsField);
    }

    public void testTypeConflictTimeseriesWhereWithCast() throws Exception {
        runTests("""
            FROM k8s, k8s_unmapped
            | WHERE network.cost::double > 10.0
            | KEEP cluster, network.cost
            """, CompactMultiTypeEsField.CompactMultiTypeEsField);
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
            """, CompactMultiTypeEsField.CompactMultiTypeEsField);
    }

    public void testMappedInOneIndexOnlyCast() throws Exception {
        runTests("""
            FROM sample_data, no_mapping_sample_data
            | EVAL x = message :: LONG
            """, CompactMultiTypeEsField.CompactMultiTypeEsField);
    }

    public void testMappedToNonKeywordInOneIndexOnly() throws Exception {
        runTests("""
            FROM sample_data, no_mapping_sample_data
            | KEEP event_duration
            """, CompactMultiTypeEsField.CompactMultiTypeEsField);
    }

    public void testTypeConflictMappedAndUnmappedWithCast() throws Exception {
        runTests("""
            FROM sample_data, no_mapping_sample_data
            | EVAL event_duration = event_duration::long
            | KEEP event_duration
            """, CompactMultiTypeEsField.CompactMultiTypeEsField);
    }

    public void testTypeConflictMappedTimesTwoAndUnmapped() throws Exception {
        runTests("""
            FROM sample_data_ts_long, sample_data, no_mapping_sample_data
            | EVAL ts = @timestamp::date
            | KEEP ts
            """, CompactMultiTypeEsField.CompactMultiTypeEsField);
    }

    public void testNoTypeConflictKeywordAndUnmappedWhere() throws Exception {
        runTests("""
            FROM sample_data, no_mapping_sample_data
            | WHERE message::keyword LIKE "Connected*"
            | KEEP message
            """, CompactMultiTypeEsField.CompactMultiTypeEsField);
    }

    // All fields are partially unmapped (no_mapping_sample_data has no mapped fields).
    // Keyword fields should become PotentiallyUnmappedKeywordEsField; non-keyword fields should become InvalidMappedField.
    // No explicit field reference — all fields come from the implicit output of FROM.
    public void testPartiallyMappedFieldsAutomaticallyFound() throws Exception {
        runTests("""
            FROM sample_data, no_mapping_sample_data
            """, CompactMultiTypeEsField.CompactMultiTypeEsField);
    }

    // Same as testPartiallyMappedFieldsAutomaticallyFound, but with an explicit KEEP * to verify wildcard expansion
    // handles partially-mapped fields correctly.
    public void testPartiallyMappedFieldsAutomaticallyFoundKeepStar() throws Exception {
        runTests("""
            FROM sample_data, no_mapping_sample_data
            | KEEP *
            """, CompactMultiTypeEsField.CompactMultiTypeEsField);
    }

    public void testPartiallyMappedNonKeywordFieldMarkedAsPotentiallyUnmapped() throws Exception {
        runTests("""
            FROM sample_data, no_mapping_sample_data
            | KEEP @timestamp, event_duration
            """, CompactMultiTypeEsField.CompactMultiTypeEsField);
    }

    public void testSingleTypeTextUnmappedNoCastLoadOnly() throws Exception {
        runTestsLoadOnly("""
            FROM text_state_mapped, text_state_unmapped
            | KEEP txt
            """, STAGES);
    }

    public void testSingleTypeDenseVectorUnmappedNoCastLoadOnly() throws Exception {
        runTestsLoadOnly("""
            FROM dense_vector, dense_vector_unmapped
            | KEEP float_vector
            """, STAGES);
    }

    public void testSingleTypeAggregateMetricDoubleUnmappedNoCastLoadOnly() throws Exception {
        runTestsLoadOnly("""
            FROM k8s-downsampled, k8s_unmapped
            | KEEP network.eth0.tx
            """, STAGES, CompactMultiTypeEsField.CompactMultiTypeEsField);
    }

    public void testSingleTypeTextUnmappedWithMatchOperatorLoadOnly() throws Exception {
        assumeTrue("Requires match operator", EsqlCapabilities.Cap.MATCH_OPERATOR_COLON.isEnabled());
        runTestsLoadOnly("""
            FROM text_state_mapped, text_state_unmapped
            | WHERE txt:"Faulkner"
            | KEEP txt
            """, STAGES);
    }

    public void testSingleTypeTextUnmappedWithMatchFunctionLoadOnly() throws Exception {
        assumeTrue("Requires MATCH_FUNCTION", EsqlCapabilities.Cap.MATCH_FUNCTION.isEnabled());
        runTestsLoadOnly("""
            FROM text_state_mapped, text_state_unmapped
            | WHERE match(txt, "Faulkner")
            | KEEP txt
            """, STAGES);
    }

    public void testSingleTypeTextMappedUnmappedAndNonExistentWithMatchFunctionLoadOnly() throws Exception {
        assumeTrue("Requires MATCH_FUNCTION", EsqlCapabilities.Cap.MATCH_FUNCTION.isEnabled());
        runTestsLoadOnly("""
            FROM text_state_mapped, text_state_unmapped, text_state_nonexistent
            | WHERE match(txt, "Faulkner") OR txt IS NULL
            | KEEP txt
            """, STAGES);
    }

    public void testSingleTypeTextMappedUnmappedAndNonExistentWithMatchFunctionAndMetadataKeepLoadOnly() throws Exception {
        assumeTrue("Requires MATCH_FUNCTION", EsqlCapabilities.Cap.MATCH_FUNCTION.isEnabled());
        runTestsLoadOnly("""
            FROM text_state_mapped, text_state_unmapped, text_state_nonexistent METADATA _index
            | WHERE match(txt, "Faulkner") OR txt IS NULL
            | KEEP _index, doc_id, txt
            | SORT _index
            """, STAGES);
    }

    public void testSingleTypeTextUnmappedWithMatchPhraseFunctionLoadOnly() throws Exception {
        assumeTrue("Requires MATCH_PHRASE_FUNCTION", EsqlCapabilities.Cap.MATCH_PHRASE_FUNCTION.isEnabled());
        runTestsLoadOnly("""
            FROM text_state_mapped, text_state_unmapped
            | WHERE match_phrase(txt, "William Faulkner")
            | KEEP txt
            """, STAGES);
    }

    // first_name and last_name are keyword, partially unmapped (missing in employees_no_names).
    // They should appear as PotentiallyUnmappedKeywordEsField in the EsRelation without being explicitly referenced.
    public void testPartiallyMappedKeywordFieldLoadedWithoutExplicitReference() throws Exception {
        runTests("""
            FROM employees, employees_no_names
            | SORT emp_no
            | LIMIT 1
            """);
    }

    // first_name (keyword, partially unmapped) should become PotentiallyUnmappedKeywordEsField.
    // gender (keyword, fully mapped in both indices) should remain a regular KeywordEsField.
    public void testNonPartiallyMappedKeywordFieldNotLoadedFromSource() throws Exception {
        runTests("""
            FROM employees, employees_no_names
            | KEEP first_name, gender
            """);
    }

    // gender is text in employees_gender_text but missing in employees_no_gender.
    // It should appear as InvalidMappedField (unsupported) in the EsRelation.
    public void testPartiallyMappedTextFieldMarkedAsPotentiallyUnmapped() throws Exception {
        runTests("""
            FROM employees_gender_text, employees_no_gender
            | KEEP gender
            """);
    }

    // DROP a single partially-mapped keyword field (message), leaving only non-keyword fields.
    public void testPartiallyMappedFieldsDropOnePartiallyMapped() throws Exception {
        runTests("""
            FROM sample_data, no_mapping_sample_data
            | DROP message
            """, CompactMultiTypeEsField.CompactMultiTypeEsField);
    }

    // DROP a single partially-mapped non-keyword field (event_duration), leaving message and the other non-keyword fields.
    public void testPartiallyMappedFieldsDropOnePartiallyMappedNonKeyword() throws Exception {
        runTests("""
            FROM sample_data, no_mapping_sample_data
            | DROP event_duration
            """, CompactMultiTypeEsField.CompactMultiTypeEsField);
    }

    // DROP with wildcards on partially-mapped non-keyword fields, leaving only the keyword field (message).
    public void testPartiallyMappedFieldsDropNonKeywordWithWildcards() throws Exception {
        runTests("""
            FROM sample_data, no_mapping_sample_data
            | DROP *_ip, *_duration, @timestamp
            """, CompactMultiTypeEsField.CompactMultiTypeEsField);
    }

    // DROP with wildcards on partially-mapped keyword fields, leaving only a few non-keyword fields.
    public void testPartiallyMappedFieldsDropKeywordWithWildcards() throws Exception {
        runTests("""
            FROM employees, employees_no_names
            | DROP *date*, gender, height*, languages*, *_hired, *_seconds, *_positions, salary_change*
            """);
    }

    public void testForkBranchesAfterStats1stBranch() throws Exception {
        runTests("""
            FROM employees
            | WHERE does_not_exist1 IS NULL
            | FORK (STATS c = COUNT(*) BY does_not_exist2)
                   (STATS d = AVG(salary))
            | SORT does_not_exist2
            """);
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

    // does_not_exist is referenced inside the languages subquery (WHERE + KEEP): under load it is loaded into that branch's source
    // and null-filled in the employees branch (Decision A in #142033).
    public void testSubqueryLoadsUnmappedFieldReferencedInOneBranch() throws Exception {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        runTests("""
            FROM employees,
                (FROM languages | WHERE does_not_exist::LONG > 1 | KEEP language_code, does_not_exist)
            | KEEP emp_no, language_code, does_not_exist
            """);
    }

    // Outer reference: the languages branch DROPs does_not_exist so it doesn't surface there (null-filled), while employees materializes
    // it from _source - the in-branch DROP no longer suppresses the broadcast to the sibling. #142033
    public void testSubqueryDropInBranchMaterializesSibling() throws Exception {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        runTests("""
            FROM employees,
                (FROM languages | DROP does_not_exist)
            | KEEP emp_no, language_code, does_not_exist
            """);
    }

    // The languages branch RENAMEs does_not_exist away; an outer reference to the original name still materializes it in the employees
    // branch (#142033), while the languages branch surfaces the value under the new name and null-fills the original name at the union.
    public void testSubqueryRenameInBranchOuterReferencesOriginalName() throws Exception {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        runTests("""
            FROM employees,
                (FROM languages | RENAME does_not_exist AS renamed)
            | KEEP emp_no, language_code, does_not_exist, renamed
            """);
    }

    // Branching view (expands to ViewUnionAll, a UnionAll subclass): does_not_exist is referenced only in the outer KEEP and is
    // unmapped in every branch, so it is loaded from _source in all branches (#142033). Exercises the ViewUnionAll scope boundary.
    public void testViewBranchingLoadsUnmappedField() throws Exception {
        assumeTrue("Requires branching views", EsqlCapabilities.Cap.VIEWS_WITH_BRANCHING.isEnabled());
        runTests("""
            FROM emp_lang_view
            | KEEP emp_no, language_code, does_not_exist
            """, Map.of("emp_lang_view", "FROM employees, (FROM languages | KEEP language_code)"));
    }

    // Branching view (ViewUnionAll): does_not_exist is referenced inside the languages branch (via the view's KEEP), so under load it is
    // loaded into that branch's source and null-filled in the employees branch (Decision A), mirroring the subquery case.
    public void testViewBranchingLoadsUnmappedFieldReferencedInOneBranch() throws Exception {
        assumeTrue("Requires branching views", EsqlCapabilities.Cap.VIEWS_WITH_BRANCHING.isEnabled());
        runTests("""
            FROM emp_lang_view
            | KEEP emp_no, language_code, does_not_exist
            """, Map.of("emp_lang_view", "FROM employees, (FROM languages | KEEP language_code, does_not_exist)"));
    }

    // does_not_exist is in-branch (loaded in the languages branch, null-filled in employees); emp_no/language_code each exist in one
    // branch and null-fill in the other through the union output. Decision A, #142033.
    public void testSubquery() throws Exception {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        runTests("""
            FROM employees, (FROM languages | WHERE does_not_exist::LONG > 0)
            | KEEP emp_no, language_code
            """);
    }

    // does_not_exist is outer-only and unmapped everywhere, so loaded in all branches (#142033); the ::LONG cast applies per branch via
    // union-type conversion, and the lookup right-side relation is left untouched. Confirms load handles a mixed branching subquery.
    public void testSubqueryWithLookupJoin() throws Exception {
        assumeTrue(
            "Requires subquery in FROM command support",
            EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_WITHOUT_IMPLICIT_LIMIT.isEnabled()
        );
        runTests("""
            FROM employees,
                (FROM languages | WHERE language_code > 0),
                (FROM employees | EVAL language_code = languages | LOOKUP JOIN languages_lookup ON language_code)
            | WHERE does_not_exist::LONG > 0
            | KEEP emp_no, language_code
            """);
    }

    public void testForkWithSort() throws Exception {
        runTests("""
            FROM employees
            | WHERE does_not_exist1::LONG > 5
            | FORK (WHERE emp_no > 3 | SORT does_not_exist2 | LIMIT 7)
                   (WHERE emp_no > 2 | EVAL xyz = does_not_exist3::KEYWORD)
            """);
    }

    private void runTests(String query) {
        runTestsNullifyAndLoad(query, STAGES, null);
    }

    private void runTests(String query, String... nestedPaths) {
        runTestsNullifyAndLoad(query, STAGES, null, nestedPaths);
    }

    private void runTests(String query, Map<String, String> views) {
        runTestsNullifyAndLoad(query, STAGES, null, views);
    }

    private void runTests(String query, TransportVersion minimumSupportedVersion, String... nestedPath) {
        runTestsNullifyAndLoad(query, STAGES, minimumSupportedVersion, nestedPath);
    }
}
