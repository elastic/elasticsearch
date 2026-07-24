/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.type.CompactMultiTypeEsField;

/**
 * Analyzer behavior for FORK and FUSE pipelines with unmapped fields.
 */
public class AnalyzerUnmappedForkGoldenTests extends AnalyzerUnmappedGoldenTestCase {

    private static void requireSample() {
        assumeTrue("sample must be enabled", EsqlCapabilities.Cap.SAMPLE_V3.isEnabled());
    }

    @ParametersFactory(argumentFormatting = "%1$s")
    public static Iterable<Object[]> parameters() {
        return goldenModes();
    }

    public AnalyzerUnmappedForkGoldenTests(@Name("mode") String mode) {
        super(mode);
    }

    public void testForkNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | FORK (WHERE does_not_exist::LONG > 0)
                   (WHERE emp_no > 0)
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testFork").nestedPath("nullify").run();
    }

    public void testForkLoad() throws Exception {
        builder(load("""
            FROM employees
            | FORK (WHERE does_not_exist::LONG > 0)
                   (WHERE emp_no > 0)
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testFork").nestedPath("load").run();
    }

    public void testForkLoadsUnmappedFieldReferencedInOneBranchNullify() throws Exception {
        builder(nullify("""
            FROM partial_mapping_sample_data
            | FORK (WHERE unmapped_message == "Disconnection error")
                   (WHERE message == "42")
            | KEEP _fork, message, unmapped_message, unmapped_event_duration
            | SORT _fork, unmapped_event_duration
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testForkLoadsUnmappedFieldReferencedInOneBranch")
            .nestedPath("nullify")
            .run();
    }

    public void testForkLoadsUnmappedFieldReferencedInOneBranchLoad() throws Exception {
        builder(load("""
            FROM partial_mapping_sample_data
            | FORK (WHERE unmapped_message == "Disconnection error")
                   (WHERE message == "42")
            | KEEP _fork, message, unmapped_message, unmapped_event_duration
            | SORT _fork, unmapped_event_duration
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testForkLoadsUnmappedFieldReferencedInOneBranch")
            .nestedPath("load")
            .run();
    }

    public void testForkLoadsUnmappedFieldWhenSiblingBranchAlignsAnotherColumnNullify() throws Exception {
        builder(nullify("""
            FROM partial_mapping_sample_data
            | FORK (WHERE unmapped_message == "Disconnection error")
                   (EVAL branch_tag = "two")
            | KEEP _fork, message, unmapped_message, branch_tag
            | SORT _fork, message
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testForkLoadsUnmappedFieldWhenSiblingBranchAlignsAnotherColumn")
            .nestedPath("nullify")
            .run();
    }

    public void testForkLoadsUnmappedFieldWhenSiblingBranchAlignsAnotherColumnLoad() throws Exception {
        builder(load("""
            FROM partial_mapping_sample_data
            | FORK (WHERE unmapped_message == "Disconnection error")
                   (EVAL branch_tag = "two")
            | KEEP _fork, message, unmapped_message, branch_tag
            | SORT _fork, message
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testForkLoadsUnmappedFieldWhenSiblingBranchAlignsAnotherColumn")
            .nestedPath("load")
            .run();
    }

    public void testForkLoadsUnmappedFieldKeptInOneBranchOnlyNullify() throws Exception {
        builder(nullify("""
            FROM partial_mapping_sample_data
            | FORK (KEEP message, unmapped_message)
                   (WHERE message == "42")
            | KEEP _fork, message, unmapped_message
            | SORT _fork, message
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testForkLoadsUnmappedFieldKeptInOneBranchOnly")
            .nestedPath("nullify")
            .run();
    }

    public void testForkLoadsUnmappedFieldKeptInOneBranchOnlyLoad() throws Exception {
        builder(load("""
            FROM partial_mapping_sample_data
            | FORK (KEEP message, unmapped_message)
                   (WHERE message == "42")
            | KEEP _fork, message, unmapped_message
            | SORT _fork, message
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testForkLoadsUnmappedFieldKeptInOneBranchOnly")
            .nestedPath("load")
            .run();
    }

    // DROP of an unmapped field is a mention, so the sibling branch materializes it while the DROP branch null-fills it. #152843
    public void testForkDropsUnmappedFieldInOneBranchMaterializesSiblingNullify() throws Exception {
        builder(nullify("""
            FROM partial_mapping_sample_data
            | FORK (DROP unmapped_message)
                   (WHERE true)
            | SORT @timestamp, _fork
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testForkDropsUnmappedFieldInOneBranchMaterializesSibling")
            .nestedPath("nullify")
            .run();
    }

    public void testForkDropsUnmappedFieldInOneBranchMaterializesSiblingLoad() throws Exception {
        builder(load("""
            FROM partial_mapping_sample_data
            | FORK (DROP unmapped_message)
                   (WHERE true)
            | SORT @timestamp, _fork
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testForkDropsUnmappedFieldInOneBranchMaterializesSibling")
            .nestedPath("load")
            .run();
    }

    // WHERE then DROP of an unmapped field is still a mention, so the sibling branch materializes it. #152843
    public void testForkWhereThenDropsUnmappedFieldInOneBranchNullify() throws Exception {
        builder(nullify("""
            FROM partial_mapping_sample_data
            | FORK (WHERE unmapped_message == "Disconnection error" | DROP unmapped_message)
                   (WHERE true)
            | SORT @timestamp, _fork
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testForkWhereThenDropsUnmappedFieldInOneBranch")
            .nestedPath("nullify")
            .run();
    }

    public void testForkWhereThenDropsUnmappedFieldInOneBranchLoad() throws Exception {
        builder(load("""
            FROM partial_mapping_sample_data
            | FORK (WHERE unmapped_message == "Disconnection error" | DROP unmapped_message)
                   (WHERE true)
            | SORT @timestamp, _fork
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testForkWhereThenDropsUnmappedFieldInOneBranch")
            .nestedPath("load")
            .run();
    }

    // MV_EXPAND turns unmapped_message into a ReferenceAttribute in branch 1; the sibling branch still loads it by name. #142033
    public void testForkLoadsUnmappedFieldExpandedInOneBranchOnlyNullify() throws Exception {
        builder(nullify("""
            FROM partial_mapping_sample_data
            | FORK (MV_EXPAND unmapped_message)
                   (WHERE message == "42")
            | KEEP _fork, message, unmapped_message
            | SORT _fork, message, unmapped_message
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testForkLoadsUnmappedFieldExpandedInOneBranchOnly")
            .nestedPath("nullify")
            .run();
    }

    public void testForkLoadsUnmappedFieldExpandedInOneBranchOnlyLoad() throws Exception {
        builder(load("""
            FROM partial_mapping_sample_data
            | FORK (MV_EXPAND unmapped_message)
                   (WHERE message == "42")
            | KEEP _fork, message, unmapped_message
            | SORT _fork, message, unmapped_message
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testForkLoadsUnmappedFieldExpandedInOneBranchOnly")
            .nestedPath("load")
            .run();
    }

    public void testForkRenamesUnmappedFieldInOneBranchNullify() throws Exception {
        builder(nullify("""
            FROM partial_mapping_sample_data
            | FORK (WHERE unmapped_message == "Disconnection error")
                   (RENAME unmapped_message AS msg)
            | KEEP _fork, message, unmapped_message, msg
            | SORT _fork, message
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testForkRenamesUnmappedFieldInOneBranch").nestedPath("nullify").run();
    }

    public void testForkRenamesUnmappedFieldInOneBranchLoad() throws Exception {
        builder(load("""
            FROM partial_mapping_sample_data
            | FORK (WHERE unmapped_message == "Disconnection error")
                   (RENAME unmapped_message AS msg)
            | KEEP _fork, message, unmapped_message, msg
            | SORT _fork, message
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testForkRenamesUnmappedFieldInOneBranch").nestedPath("load").run();
    }

    // does_not_exist is referenced only in the WHERE branch; the LEFT LOOKUP JOIN branch still loads it into its left source and
    // flows it through the join rather than null-filling. #142033
    public void testForkLoadsUnmappedFieldAcrossLookupJoinBranchNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | EVAL language_code = languages
            | FORK (LOOKUP JOIN languages_lookup ON language_code)
                   (WHERE does_not_exist::KEYWORD == "x")
            | KEEP _fork, emp_no, language_name, does_not_exist
            | SORT _fork, emp_no
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testForkLoadsUnmappedFieldAcrossLookupJoinBranch")
            .nestedPath("nullify")
            .run();
    }

    public void testForkLoadsUnmappedFieldAcrossLookupJoinBranchLoad() throws Exception {
        builder(load("""
            FROM employees
            | EVAL language_code = languages
            | FORK (LOOKUP JOIN languages_lookup ON language_code)
                   (WHERE does_not_exist::KEYWORD == "x")
            | KEEP _fork, emp_no, language_name, does_not_exist
            | SORT _fork, emp_no
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testForkLoadsUnmappedFieldAcrossLookupJoinBranch")
            .nestedPath("load")
            .run();
    }

    // The LOOKUP JOIN branch loads does_not_exist across (into its left source), the WHERE branch loads it directly, and the STATS branch
    // null-fills it because an aggregation drops non-grouped fields - exercising load-through-join, load-direct and null-fill in one FORK.
    public void testForkLoadsUnmappedFieldAcrossLookupJoinAndStatsBranchesNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | EVAL language_code = languages
            | FORK (LOOKUP JOIN languages_lookup ON language_code)
                   (WHERE does_not_exist::KEYWORD == "x")
                   (STATS c = COUNT(*) BY emp_no)
            | KEEP _fork, emp_no, language_name, does_not_exist, c
            | SORT _fork, emp_no
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testForkLoadsUnmappedFieldAcrossLookupJoinAndStatsBranches")
            .nestedPath("nullify")
            .run();
    }

    public void testForkLoadsUnmappedFieldAcrossLookupJoinAndStatsBranchesLoad() throws Exception {
        builder(load("""
            FROM employees
            | EVAL language_code = languages
            | FORK (LOOKUP JOIN languages_lookup ON language_code)
                   (WHERE does_not_exist::KEYWORD == "x")
                   (STATS c = COUNT(*) BY emp_no)
            | KEEP _fork, emp_no, language_name, does_not_exist, c
            | SORT _fork, emp_no
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testForkLoadsUnmappedFieldAcrossLookupJoinAndStatsBranches")
            .nestedPath("load")
            .run();
    }

    // gender is a two-legged PUNK (TEXT in employees_gender_text, unmapped in employees_no_gender); a FORK output must preserve its
    // TEXT type, not flag it UNSUPPORTED.
    public void testForkKeepsSingleTypePartiallyUnmappedTextFieldNullify() throws Exception {
        builder(nullify("""
            FROM employees_gender_text, employees_no_gender
            | KEEP gender
            | FORK (WHERE true)
                   (WHERE true)
            | KEEP _fork, gender
            | SORT _fork
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testForkKeepsSingleTypePartiallyUnmappedTextField")
            .nestedPath("nullify")
            .run();
    }

    public void testForkKeepsSingleTypePartiallyUnmappedTextFieldLoad() throws Exception {
        builder(load("""
            FROM employees_gender_text, employees_no_gender
            | KEEP gender
            | FORK (WHERE true)
                   (WHERE true)
            | KEEP _fork, gender
            | SORT _fork
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testForkKeepsSingleTypePartiallyUnmappedTextField")
            .nestedPath("load")
            .run();
    }

    // id is short in apps_short and unmapped in partial_mapping_sample_data, so it is a single-type partially-unmapped (two-legged PUNK)
    // small numeric
    public void testForkWidensSingleTypePartiallyUnmappedShortFieldNullify() throws Exception {
        builder(nullify("""
            FROM apps_short, partial_mapping_sample_data
            | KEEP id
            | FORK (WHERE true)
                   (WHERE true)
            | KEEP _fork, id
            | SORT _fork
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testForkWidensSingleTypePartiallyUnmappedShortField")
            .nestedPath("nullify")
            .since(CompactMultiTypeEsField.CompactMultiTypeEsField)
            .run();
    }

    public void testForkWidensSingleTypePartiallyUnmappedShortFieldLoad() throws Exception {
        builder(load("""
            FROM apps_short, partial_mapping_sample_data
            | KEEP id
            | FORK (WHERE true)
                   (WHERE true)
            | KEEP _fork, id
            | SORT _fork
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testForkWidensSingleTypePartiallyUnmappedShortField")
            .nestedPath("load")
            .since(CompactMultiTypeEsField.CompactMultiTypeEsField)
            .run();
    }

    // A genuine multi-type conflict (short/long/unmapped) is not a two-legged PUNK (types > 1), so it stays UNSUPPORTED through the
    // FORK output; KEEP-only is tolerated (checkFork skips it).
    public void testForkThreeWayTypeConflictShortLongUnmappedStaysUnsupportedNullify() throws Exception {
        builder(nullify("""
            FROM all_types, all_types_short_as_long, all_types_no_short
            | KEEP short
            | FORK (WHERE true)
                   (WHERE true)
            | KEEP _fork, short
            | SORT _fork
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testForkThreeWayTypeConflictShortLongUnmappedStaysUnsupported")
            .nestedPath("nullify")
            .run();
    }

    public void testForkThreeWayTypeConflictShortLongUnmappedStaysUnsupportedLoad() throws Exception {
        builder(load("""
            FROM all_types, all_types_short_as_long, all_types_no_short
            | KEEP short
            | FORK (WHERE true)
                   (WHERE true)
            | KEEP _fork, short
            | SORT _fork
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testForkThreeWayTypeConflictShortLongUnmappedStaysUnsupported")
            .nestedPath("load")
            .run();
    }

    public void testForkWithEvalNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | FORK (EVAL x = does_not_exist::DOUBLE + 1)
                   (EVAL y = emp_no + 1)
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testForkWithEval").nestedPath("nullify").run();
    }

    public void testForkWithEvalLoad() throws Exception {
        builder(load("""
            FROM employees
            | FORK (EVAL x = does_not_exist::DOUBLE + 1)
                   (EVAL y = emp_no + 1)
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testForkWithEval").nestedPath("load").run();
    }

    public void testForkWithStatsNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | FORK (STATS c = COUNT(*) BY does_not_exist)
                   (STATS d = AVG(salary::DOUBLE))
            | SORT does_not_exist
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testForkWithStats").nestedPath("nullify").run();
    }

    public void testForkWithStatsLoad() throws Exception {
        builder(load("""
            FROM employees
            | FORK (STATS c = COUNT(*) BY does_not_exist)
                   (STATS d = AVG(salary::DOUBLE))
            | SORT does_not_exist
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testForkWithStats").nestedPath("load").run();
    }

    public void testForkBranchesWithDifferentSchemasNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | WHERE first_name == "Chris" AND does_not_exist1::LONG > 5
            | EVAL does_not_exist2 IS NULL
            | FORK (WHERE emp_no > 3 | SORT does_not_exist3 | LIMIT 7 )
                   (WHERE emp_no > 2 | EVAL xyz = does_not_exist4::KEYWORD )
                   (DISSECT first_name "%{d} %{e} %{f}"
                    | STATS x = MIN(d::DOUBLE), y = MAX(e::DOUBLE) WHERE d::DOUBLE > 1000 + does_not_exist5::DOUBLE
                    | EVAL xyz = "abc")
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testForkBranchesWithDifferentSchemas").nestedPath("nullify").run();
    }

    public void testForkBranchesWithDifferentSchemasLoad() throws Exception {
        builder(load("""
            FROM employees
            | WHERE first_name == "Chris" AND does_not_exist1::LONG > 5
            | EVAL does_not_exist2 IS NULL
            | FORK (WHERE emp_no > 3 | SORT does_not_exist3 | LIMIT 7 )
                   (WHERE emp_no > 2 | EVAL xyz = does_not_exist4::KEYWORD )
                   (DISSECT first_name "%{d} %{e} %{f}"
                    | STATS x = MIN(d::DOUBLE), y = MAX(e::DOUBLE) WHERE d::DOUBLE > 1000 + does_not_exist5::DOUBLE
                    | EVAL xyz = "abc")
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testForkBranchesWithDifferentSchemas").nestedPath("load").run();
    }

    public void testForkBranchesAfterStats2ndBranchNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | WHERE does_not_exist1 IS NULL
            | FORK (STATS c = COUNT(*))
                   (STATS d = AVG(salary) BY does_not_exist2)
            | SORT does_not_exist2
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testForkBranchesAfterStats2ndBranch").nestedPath("nullify").run();
    }

    public void testForkBranchesAfterStats2ndBranchLoad() throws Exception {
        builder(load("""
            FROM employees
            | WHERE does_not_exist1 IS NULL
            | FORK (STATS c = COUNT(*))
                   (STATS d = AVG(salary) BY does_not_exist2)
            | SORT does_not_exist2
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testForkBranchesAfterStats2ndBranch").nestedPath("load").run();
    }

    public void testFuseNullify() throws Exception {
        builder(nullify("""
            FROM employees METADATA _score, _index, _id
            | FORK (WHERE does_not_exist::LONG > 0)
                   (WHERE emp_no > 0)
            | LIMIT 100
            | FUSE
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testFuse").nestedPath("nullify").run();
    }

    public void testFuseWithEvalNullify() throws Exception {
        builder(nullify("""
            FROM employees METADATA _score, _index, _id
            | FORK (EVAL x = does_not_exist::DOUBLE + 1)
                   (EVAL y = emp_no + 1)
            | LIMIT 100
            | FUSE RRF
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testFuseWithEval").nestedPath("nullify").run();
    }

    public void testFuseLinearNullify() throws Exception {
        builder(nullify("""
            FROM employees METADATA _score, _index, _id
            | FORK (WHERE does_not_exist::LONG > 0 | EVAL x = 1)
                   (WHERE emp_no > 0 | EVAL y = 2)
            | LIMIT 100
            | FUSE LINEAR
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testFuseLinear").nestedPath("nullify").run();
    }

    public void testForkBranchesAfterStats1stBranchNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | WHERE does_not_exist1 IS NULL
            | FORK (STATS c = COUNT(*) BY does_not_exist2)
                   (STATS d = AVG(salary))
            | SORT does_not_exist2
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testForkBranchesAfterStats1stBranch").nestedPath("nullify").run();
    }

    public void testForkBranchesAfterStats1stBranchLoad() throws Exception {
        builder(load("""
            FROM employees
            | WHERE does_not_exist1 IS NULL
            | FORK (STATS c = COUNT(*) BY does_not_exist2)
                   (STATS d = AVG(salary))
            | SORT does_not_exist2
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testForkBranchesAfterStats1stBranch").nestedPath("load").run();
    }

    public void testForkWithRowNullify() throws Exception {
        builder(nullify("""
            ROW a = 1
            | FORK (where true)
            | WHERE a == 1
            | KEEP bar
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testForkWithRow").nestedPath("nullify").run();
    }

    public void testForkWithFromNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | FORK (where foo != 84) (where true)
            | WHERE _fork == "fork1"
            | DROP _fork
            | eval y = coalesce(bar, baz)
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testForkWithFrom").nestedPath("nullify").run();
    }

    public void testForkWithUnmappedStatsEvalKeepNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | keep emp_no
            | FORK (where true | mv_expand emp_no)
            | stats emp_no = count(*)
            | eval x = least(emp_no, 52, 60)
            | keep emp_no
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testForkWithUnmappedStatsEvalKeep").nestedPath("nullify").run();
    }

    public void testForkWithUnmappedStatsEvalKeepTwoBranchesNullify() throws Exception {
        requireSample();
        builder(nullify("""
            FROM employees
            | keep emp_no
            | FORK (where true | mv_expand emp_no) (where true | SAMPLE 0.5)
            | stats emp_no = count(*)
            | eval x = least(emp_no, 52, 60)
            | keep emp_no
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testForkWithUnmappedStatsEvalKeepTwoBranches")
            .nestedPath("nullify")
            .run();
    }

    public void testForkWithRowCoalesceAndDropNullify() throws Exception {
        builder(nullify("""
            ROW a = 12::long
            | fork (where true)
            | eval x = Coalesce(a, 5)
            | drop a
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testForkWithRowCoalesceAndDrop").nestedPath("nullify").run();
    }

    public void testForkWithSortNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | WHERE does_not_exist1::LONG > 5
            | FORK (WHERE emp_no > 3 | SORT does_not_exist2 | LIMIT 7)
                   (WHERE emp_no > 2 | EVAL xyz = does_not_exist3::KEYWORD)
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testForkWithSort").nestedPath("nullify").run();
    }

    public void testForkWithSortLoad() throws Exception {
        builder(load("""
            FROM employees
            | WHERE does_not_exist1::LONG > 5
            | FORK (WHERE emp_no > 3 | SORT does_not_exist2 | LIMIT 7)
                   (WHERE emp_no > 2 | EVAL xyz = does_not_exist3::KEYWORD)
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testForkWithSort").nestedPath("load").run();
    }
}
