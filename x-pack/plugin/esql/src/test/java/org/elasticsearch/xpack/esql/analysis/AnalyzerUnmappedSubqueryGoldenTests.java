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

import java.util.Map;

/**
 * Analyzer behavior for subqueries, {@code UnionAll}, and branching views with unmapped fields.
 */
public class AnalyzerUnmappedSubqueryGoldenTests extends AnalyzerUnmappedGoldenTestCase {

    private static void requireSubqueryInFrom() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
    }

    private static void requireSubqueryWithoutImplicitLimit() {
        assumeTrue(
            "Requires subquery in FROM without an implicit limit",
            EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_WITHOUT_IMPLICIT_LIMIT.isEnabled()
        );
    }

    private static void requireRowSubquery() {
        assumeTrue("Requires ROW source subqueries", EsqlCapabilities.Cap.SUBQUERY_WITH_ROW.isEnabled());
    }

    private static void requireBranchingViews() {
        assumeTrue("Requires branching views", EsqlCapabilities.Cap.VIEWS_WITH_BRANCHING.isEnabled());
    }

    @ParametersFactory(argumentFormatting = "%1$s")
    public static Iterable<Object[]> parameters() {
        return goldenModes();
    }

    public AnalyzerUnmappedSubqueryGoldenTests(@Name("mode") String mode) {
        super(mode);
    }

    // does_not_exist is referenced only in the outer KEEP and is unmapped in every branch source: it is loaded from _source in all
    // branches (#142033, "referenced after subqueries"), exactly as "FROM idx1, idx2 | KEEP missing" loads it from every index.
    public void testSubqueryKeepUnmappedNullify() throws Exception {
        requireSubqueryInFrom();
        builder(nullify("""
            FROM employees, (FROM languages | KEEP language_code)
            | KEEP emp_no, language_code, does_not_exist
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testSubqueryKeepUnmapped").nestedPath("nullify").run();
    }

    public void testSubqueryKeepUnmappedLoad() throws Exception {
        requireSubqueryInFrom();
        builder(load("""
            FROM employees, (FROM languages | KEEP language_code)
            | KEEP emp_no, language_code, does_not_exist
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testSubqueryKeepUnmapped").nestedPath("load").run();
    }

    // does_not_exist is referenced inside the sample_data subquery (STATS grouping): under load it is loaded into that branch's
    // source and null-filled in the employees branch (Decision A).
    public void testSubqueryWithStatsNullify() throws Exception {
        requireSubqueryInFrom();
        builder(nullify("""
            FROM employees, (FROM sample_data | STATS max_ts = MAX(@timestamp) BY does_not_exist)
            | KEEP emp_no, max_ts, does_not_exist
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testSubqueryWithStats").nestedPath("nullify").run();
    }

    public void testSubqueryWithStatsLoad() throws Exception {
        requireSubqueryInFrom();
        builder(load("""
            FROM employees, (FROM sample_data | STATS max_ts = MAX(@timestamp) BY does_not_exist)
            | KEEP emp_no, max_ts, does_not_exist
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testSubqueryWithStats").nestedPath("load").run();
    }

    // unmapped1/unmapped2 are referenced inside the languages subquery (KEEP): under load they are loaded into that branch's source
    // and null-filled in the employees branch (Decision A).
    public void testSubqueryKeepMultipleUnmappedNullify() throws Exception {
        requireSubqueryInFrom();
        builder(nullify("""
            FROM employees,
                (FROM languages | KEEP language_code, unmapped1, unmapped2)
            | KEEP emp_no, language_code, unmapped1, unmapped2
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testSubqueryKeepMultipleUnmapped").nestedPath("nullify").run();
    }

    public void testSubqueryKeepMultipleUnmappedLoad() throws Exception {
        requireSubqueryInFrom();
        builder(load("""
            FROM employees,
                (FROM languages | KEEP language_code, unmapped1, unmapped2)
            | KEEP emp_no, language_code, unmapped1, unmapped2
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testSubqueryKeepMultipleUnmapped").nestedPath("load").run();
    }

    // UnionAll counterpart of testForkWidensSingleTypePartiallyUnmappedShortField: id (two-legged short PUNK) must surface as INTEGER
    // on the UnionAll output, so the widening fix applies to UnionAll/views too, not just Fork.
    public void testSubqueryWidensSingleTypePartiallyUnmappedShortFieldNullify() throws Exception {
        // Both branches make id a two-legged short PUNK and only KEEP it; branches and UnionAll output must agree on the widened INTEGER
        // type, else checkUnionAll reports [INTEGER] vs [SHORT]. (Plain short avoided: subqueries don't auto-widen numerics.)
        builder(nullify("""
            FROM (FROM apps_short, partial_mapping_sample_data | KEEP id),
                 (FROM apps_short, partial_mapping_sample_data | KEEP id)
            | KEEP id
            | SORT id NULLS LAST
            | LIMIT 5
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testSubqueryWidensSingleTypePartiallyUnmappedShortField")
            .nestedPath("nullify")
            .since(CompactMultiTypeEsField.CompactMultiTypeEsField)
            .run();
    }

    public void testSubqueryWidensSingleTypePartiallyUnmappedShortFieldLoad() throws Exception {
        // Both branches make id a two-legged short PUNK and only KEEP it; branches and UnionAll output must agree on the widened INTEGER
        // type, else checkUnionAll reports [INTEGER] vs [SHORT]. (Plain short avoided: subqueries don't auto-widen numerics.)
        builder(load("""
            FROM (FROM apps_short, partial_mapping_sample_data | KEEP id),
                 (FROM apps_short, partial_mapping_sample_data | KEEP id)
            | KEEP id
            | SORT id NULLS LAST
            | LIMIT 5
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testSubqueryWidensSingleTypePartiallyUnmappedShortField")
            .nestedPath("load")
            .since(CompactMultiTypeEsField.CompactMultiTypeEsField)
            .run();
    }

    // Single subquery without a main index is merged during analysis (no UnionAll), so does_not_exist is loaded into the merged
    // source - the linear/FORK path, unchanged by Step 2.
    public void testSubqueryOnlyNullify() throws Exception {
        requireSubqueryInFrom();
        builder(nullify("""
            FROM
                (FROM languages
                 | WHERE does_not_exist::LONG > 1)
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testSubqueryOnly").nestedPath("nullify").run();
    }

    public void testSubqueryOnlyLoad() throws Exception {
        requireSubqueryInFrom();
        builder(load("""
            FROM
                (FROM languages
                 | WHERE does_not_exist::LONG > 1)
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testSubqueryOnly").nestedPath("load").run();
    }

    // does_not_exist1 is referenced inside both branches: under load it is loaded into each branch's own source (Decision A); the
    // differing casts apply to the WHERE predicate only, so both branches still surface a keyword and there is no type conflict.
    public void testDoubleSubqueryOnlyNullify() throws Exception {
        requireSubqueryWithoutImplicitLimit();
        builder(nullify("""
            FROM
                (FROM languages
                 | WHERE does_not_exist1::LONG > 1),
                (FROM sample_data
                 | WHERE does_not_exist1::DOUBLE > 10.)
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testDoubleSubqueryOnly").nestedPath("nullify").run();
    }

    public void testDoubleSubqueryOnlyLoad() throws Exception {
        requireSubqueryWithoutImplicitLimit();
        builder(load("""
            FROM
                (FROM languages
                 | WHERE does_not_exist1::LONG > 1),
                (FROM sample_data
                 | WHERE does_not_exist1::DOUBLE > 10.)
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testDoubleSubqueryOnly").nestedPath("load").run();
    }

    // does_not_exist1 is referenced inside each branch, so it is loaded into each branch's own source (in-branch scope); does_not_exist2
    // is referenced only in the outer WHERE and is unmapped in every branch, so it is loaded from _source in all branches (#142033).
    public void testDoubleSubqueryOnlyWithTopFilterAndNoMainNullify() throws Exception {
        requireSubqueryWithoutImplicitLimit();
        builder(nullify("""
            FROM
                (FROM languages
                 | WHERE does_not_exist1::LONG > 1),
                (FROM sample_data
                 | WHERE does_not_exist1::DOUBLE > 10.)
            | WHERE does_not_exist2::LONG < 100
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testDoubleSubqueryOnlyWithTopFilterAndNoMain")
            .nestedPath("nullify")
            .run();
    }

    public void testDoubleSubqueryOnlyWithTopFilterAndNoMainLoad() throws Exception {
        requireSubqueryWithoutImplicitLimit();
        builder(load("""
            FROM
                (FROM languages
                 | WHERE does_not_exist1::LONG > 1),
                (FROM sample_data
                 | WHERE does_not_exist1::DOUBLE > 10.)
            | WHERE does_not_exist2::LONG < 100
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testDoubleSubqueryOnlyWithTopFilterAndNoMain")
            .nestedPath("load")
            .run();
    }

    // does_not_exist1 is in-branch (loaded only in the languages branch, null-filled in employees); does_not_exist2 is outer-only and
    // unmapped everywhere, so loaded from _source in all branches. #142033
    public void testSubqueryAndMainQueryNullify() throws Exception {
        requireSubqueryWithoutImplicitLimit();
        builder(nullify("""
            FROM employees,
                (FROM languages
                 | WHERE does_not_exist1::LONG > 1)
            | WHERE does_not_exist2::LONG < 10 AND emp_no > 0
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testSubqueryAndMainQuery").nestedPath("nullify").run();
    }

    public void testSubqueryAndMainQueryLoad() throws Exception {
        requireSubqueryWithoutImplicitLimit();
        builder(load("""
            FROM employees,
                (FROM languages
                 | WHERE does_not_exist1::LONG > 1)
            | WHERE does_not_exist2::LONG < 10 AND emp_no > 0
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testSubqueryAndMainQuery").nestedPath("load").run();
    }

    // Outer-only reference over a union of an index branch and a ROW branch: does_not_exist loads from _source into the employees
    // EsRelation, while the ROW branch (can't load) is null-filled by resolveFork alignment. #142033
    public void testSubqueryWithRowBranchOuterReferenceNullify() throws Exception {
        requireSubqueryInFrom();
        requireRowSubquery();
        builder(nullify("""
            FROM employees, (ROW synthetic = 1)
            | KEEP emp_no, synthetic, does_not_exist
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testSubqueryWithRowBranchOuterReference").nestedPath("nullify").run();
    }

    public void testSubqueryWithRowBranchOuterReferenceLoad() throws Exception {
        requireSubqueryInFrom();
        requireRowSubquery();
        builder(load("""
            FROM employees, (ROW synthetic = 1)
            | KEEP emp_no, synthetic, does_not_exist
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testSubqueryWithRowBranchOuterReference").nestedPath("load").run();
    }

    // Single subquery merged during analysis (no UnionAll): emp_no_foo is loaded into the merged source (linear path).
    public void testSubqueryMixNullify() throws Exception {
        requireSubqueryInFrom();
        builder(nullify("""
            FROM
                (FROM employees
                 | EVAL emp_no_plus = emp_no_foo::LONG + 1
                 | WHERE emp_no < 10003)
            | KEEP emp_no*
            | SORT emp_no, emp_no_plus
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testSubqueryMix").nestedPath("nullify").run();
    }

    public void testSubqueryMixLoad() throws Exception {
        requireSubqueryInFrom();
        builder(load("""
            FROM
                (FROM employees
                 | EVAL emp_no_plus = emp_no_foo::LONG + 1
                 | WHERE emp_no < 10003)
            | KEEP emp_no*
            | SORT emp_no, emp_no_plus
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testSubqueryMix").nestedPath("load").run();
    }

    // Single subquery merged during analysis (no UnionAll): emp_no_foo is loaded into the merged source (linear path).
    public void testSubqueryMixWithDropPatternNullify() throws Exception {
        requireSubqueryInFrom();
        builder(nullify("""
            FROM
                (FROM employees
                 | EVAL emp_no_plus = emp_no_foo::LONG + 1
                 | WHERE emp_no < 10003)
            | DROP *_name
            | SORT emp_no, emp_no_plus
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testSubqueryMixWithDropPattern").nestedPath("nullify").run();
    }

    public void testSubqueryMixWithDropPatternLoad() throws Exception {
        requireSubqueryInFrom();
        builder(load("""
            FROM
                (FROM employees
                 | EVAL emp_no_plus = emp_no_foo::LONG + 1
                 | WHERE emp_no < 10003)
            | DROP *_name
            | SORT emp_no, emp_no_plus
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testSubqueryMixWithDropPattern").nestedPath("load").run();
    }

    // Single subquery merged during analysis (no UnionAll): does_not_exist is loaded into the merged source (linear path).
    public void testSubqueryAfterUnionAllOfStatsNullify() throws Exception {
        requireSubqueryInFrom();
        builder(nullify("""
            FROM
                (FROM employees
                 | STATS c = COUNT(*) BY does_not_exist)
            | SORT does_not_exist
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testSubqueryAfterUnionAllOfStats").nestedPath("nullify").run();
    }

    public void testSubqueryAfterUnionAllOfStatsLoad() throws Exception {
        requireSubqueryInFrom();
        builder(load("""
            FROM
                (FROM employees
                 | STATS c = COUNT(*) BY does_not_exist)
            | SORT does_not_exist
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testSubqueryAfterUnionAllOfStats").nestedPath("load").run();
    }

    // does_not_exist is outer-only and unmapped everywhere, so loaded in all branches (#142033): the main branch surfaces it; the STATS
    // branch loads it but STATS drops it, so it null-fills at the union.
    public void testSubqueryAfterUnionAllOfStatsAndMainNullify() throws Exception {
        requireSubqueryInFrom();
        builder(nullify("""
            FROM employees,
                (FROM employees | STATS c = count(*))
            | SORT does_not_exist
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testSubqueryAfterUnionAllOfStatsAndMain").nestedPath("nullify").run();
    }

    public void testSubqueryAfterUnionAllOfStatsAndMainLoad() throws Exception {
        requireSubqueryInFrom();
        builder(load("""
            FROM employees,
                (FROM employees | STATS c = count(*))
            | SORT does_not_exist
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testSubqueryAfterUnionAllOfStatsAndMain").nestedPath("load").run();
    }

    // does_not_exist1 is referenced inside both language branches (loaded there, in-branch scope) and again in the outer WHERE (resolves
    // via the union output); does_not_exist2 is outer-only and unmapped everywhere, so it is loaded from _source in all branches (#142033).
    public void testSubquerysWithMainAndSameOptionalNullify() throws Exception {
        requireSubqueryWithoutImplicitLimit();
        builder(nullify("""
            FROM employees,
                (FROM languages
                 | WHERE does_not_exist1::LONG > 1),
                (FROM languages
                 | WHERE does_not_exist1::LONG > 2)
            | WHERE does_not_exist2::LONG < 10 AND emp_no > 0 OR does_not_exist1::LONG < 11
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testSubquerysWithMainAndSameOptional").nestedPath("nullify").run();
    }

    public void testSubquerysWithMainAndSameOptionalLoad() throws Exception {
        requireSubqueryWithoutImplicitLimit();
        builder(load("""
            FROM employees,
                (FROM languages
                 | WHERE does_not_exist1::LONG > 1),
                (FROM languages
                 | WHERE does_not_exist1::LONG > 2)
            | WHERE does_not_exist2::LONG < 10 AND emp_no > 0 OR does_not_exist1::LONG < 11
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testSubquerysWithMainAndSameOptional").nestedPath("load").run();
    }

    public void testSubquerysMixAndLookupJoinNullify() throws Exception {
        requireSubqueryInFrom();
        builder(nullify("""
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
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testSubquerysMixAndLookupJoinNullify").nestedPath("nullify").run();
    }

    // Nullify-only: under load, salary loads as KEYWORD inside AVG(salary), which AVG rejects (numeric required) - a legitimate
    // load-mode semantic unrelated to the subquery scoping under test.
    public void testSubquerysWithMainAndStatsOnlyNullify() throws Exception {
        requireSubqueryInFrom();
        // Adding a main index pattern makes does_not_exist2 and does_not_exist3 resolve, unlike the same query without it.
        builder(nullify("""
            FROM employees, // adding a "main" index/pattern makes does_not_exist2 & 3 resolved (compared to the same query above, w/o it)
                (FROM languages
                 | STATS c = COUNT(*) BY emp_no, does_not_exist1),
                (FROM languages
                 | STATS a = AVG(salary))
            | WHERE does_not_exist2::LONG < 10
            | EVAL x = does_not_exist3
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testSubquerysWithMainAndStatsOnly").nestedPath("nullify").run();
    }

    public void testSingleSubqueryNullify() throws Exception {
        requireSubqueryInFrom();
        // A single subquery without a main index is merged into the main query during analysis,
        // so there is no Subquery node in the plan and no branching — this is allowed in load.
        builder(nullify("""
            FROM (FROM languages | WHERE language_code > 1)\
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testSingleSubquery").nestedPath("nullify").run();
    }

    public void testSingleSubqueryLoad() throws Exception {
        requireSubqueryInFrom();
        // A single subquery without a main index is merged into the main query during analysis,
        // so there is no Subquery node in the plan and no branching — this is allowed in load.
        builder(load("""
            FROM (FROM languages | WHERE language_code > 1)\
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testSingleSubquery").nestedPath("load").run();
    }

    // does_not_exist is referenced inside the languages subquery (WHERE + KEEP): under load it is loaded into that branch's source
    // and null-filled in the employees branch (Decision A in #142033).
    public void testSubqueryLoadsUnmappedFieldReferencedInOneBranchNullify() throws Exception {
        requireSubqueryInFrom();
        builder(nullify("""
            FROM employees,
                (FROM languages | WHERE does_not_exist::LONG > 1 | KEEP language_code, does_not_exist)
            | KEEP emp_no, language_code, does_not_exist
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testSubqueryLoadsUnmappedFieldReferencedInOneBranch")
            .nestedPath("nullify")
            .run();
    }

    public void testSubqueryLoadsUnmappedFieldReferencedInOneBranchLoad() throws Exception {
        requireSubqueryInFrom();
        builder(load("""
            FROM employees,
                (FROM languages | WHERE does_not_exist::LONG > 1 | KEEP language_code, does_not_exist)
            | KEEP emp_no, language_code, does_not_exist
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testSubqueryLoadsUnmappedFieldReferencedInOneBranch")
            .nestedPath("load")
            .run();
    }

    // Outer reference: the languages branch DROPs does_not_exist so it doesn't surface there (null-filled), while employees materializes
    // it from _source - the in-branch DROP no longer suppresses the broadcast to the sibling. #142033
    public void testSubqueryDropInBranchMaterializesSiblingNullify() throws Exception {
        requireSubqueryInFrom();
        builder(nullify("""
            FROM employees,
                (FROM languages | DROP does_not_exist)
            | KEEP emp_no, language_code, does_not_exist
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testSubqueryDropInBranchMaterializesSibling")
            .nestedPath("nullify")
            .run();
    }

    public void testSubqueryDropInBranchMaterializesSiblingLoad() throws Exception {
        requireSubqueryInFrom();
        builder(load("""
            FROM employees,
                (FROM languages | DROP does_not_exist)
            | KEEP emp_no, language_code, does_not_exist
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testSubqueryDropInBranchMaterializesSibling").nestedPath("load").run();
    }

    // The languages branch RENAMEs does_not_exist away; an outer reference to the original name still materializes it in the employees
    // branch (#142033), while the languages branch surfaces the value under the new name and null-fills the original name at the union.
    public void testSubqueryRenameInBranchOuterReferencesOriginalNameNullify() throws Exception {
        requireSubqueryInFrom();
        builder(nullify("""
            FROM employees,
                (FROM languages | RENAME does_not_exist AS renamed)
            | KEEP emp_no, language_code, does_not_exist, renamed
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testSubqueryRenameInBranchOuterReferencesOriginalName")
            .nestedPath("nullify")
            .run();
    }

    public void testSubqueryRenameInBranchOuterReferencesOriginalNameLoad() throws Exception {
        requireSubqueryInFrom();
        builder(load("""
            FROM employees,
                (FROM languages | RENAME does_not_exist AS renamed)
            | KEEP emp_no, language_code, does_not_exist, renamed
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testSubqueryRenameInBranchOuterReferencesOriginalName")
            .nestedPath("load")
            .run();
    }

    // Branching view (expands to ViewUnionAll, a UnionAll subclass): does_not_exist is referenced only in the outer KEEP and is
    // unmapped in every branch, so it is loaded from _source in all branches (#142033). Exercises the ViewUnionAll scope boundary.
    public void testViewBranchingLoadsUnmappedFieldNullify() throws Exception {
        requireBranchingViews();
        builder(nullify("""
            FROM emp_lang_view
            | KEEP emp_no, language_code, does_not_exist
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testViewBranchingLoadsUnmappedField")
            .nestedPath("nullify")
            .views(Map.of("emp_lang_view", "FROM employees, (FROM languages | KEEP language_code)"))
            .run();
    }

    public void testViewBranchingLoadsUnmappedFieldLoad() throws Exception {
        requireBranchingViews();
        builder(load("""
            FROM emp_lang_view
            | KEEP emp_no, language_code, does_not_exist
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testViewBranchingLoadsUnmappedField")
            .nestedPath("load")
            .views(Map.of("emp_lang_view", "FROM employees, (FROM languages | KEEP language_code)"))
            .run();
    }

    // Branching view (ViewUnionAll): does_not_exist is referenced inside the languages branch (via the view's KEEP), so under load it is
    // loaded into that branch's source and null-filled in the employees branch (Decision A), mirroring the subquery case.
    public void testViewBranchingLoadsUnmappedFieldReferencedInOneBranchNullify() throws Exception {
        requireBranchingViews();
        builder(nullify("""
            FROM emp_lang_view
            | KEEP emp_no, language_code, does_not_exist
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testViewBranchingLoadsUnmappedFieldReferencedInOneBranch")
            .nestedPath("nullify")
            .views(Map.of("emp_lang_view", "FROM employees, (FROM languages | KEEP language_code, does_not_exist)"))
            .run();
    }

    public void testViewBranchingLoadsUnmappedFieldReferencedInOneBranchLoad() throws Exception {
        requireBranchingViews();
        builder(load("""
            FROM emp_lang_view
            | KEEP emp_no, language_code, does_not_exist
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testViewBranchingLoadsUnmappedFieldReferencedInOneBranch")
            .nestedPath("load")
            .views(Map.of("emp_lang_view", "FROM employees, (FROM languages | KEEP language_code, does_not_exist)"))
            .run();
    }

    // does_not_exist is in-branch (loaded in the languages branch, null-filled in employees); emp_no/language_code each exist in one
    // branch and null-fill in the other through the union output. Decision A, #142033.
    public void testSubqueryNullify() throws Exception {
        requireSubqueryInFrom();
        builder(nullify("""
            FROM employees, (FROM languages | WHERE does_not_exist::LONG > 0)
            | KEEP emp_no, language_code
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testSubquery").nestedPath("nullify").run();
    }

    public void testSubqueryLoad() throws Exception {
        requireSubqueryInFrom();
        builder(load("""
            FROM employees, (FROM languages | WHERE does_not_exist::LONG > 0)
            | KEEP emp_no, language_code
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testSubquery").nestedPath("load").run();
    }

    // does_not_exist is outer-only and unmapped everywhere, so loaded in all branches (#142033); the ::LONG cast applies per branch via
    // union-type conversion, and the lookup right-side relation is left untouched. Confirms load handles a mixed branching subquery.
    public void testSubqueryWithLookupJoinNullify() throws Exception {
        requireSubqueryWithoutImplicitLimit();
        builder(nullify("""
            FROM employees,
                (FROM languages | WHERE language_code > 0),
                (FROM employees | EVAL language_code = languages | LOOKUP JOIN languages_lookup ON language_code)
            | WHERE does_not_exist::LONG > 0
            | KEEP emp_no, language_code
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testSubqueryWithLookupJoin").nestedPath("nullify").run();
    }

    public void testSubqueryWithLookupJoinLoad() throws Exception {
        requireSubqueryWithoutImplicitLimit();
        builder(load("""
            FROM employees,
                (FROM languages | WHERE language_code > 0),
                (FROM employees | EVAL language_code = languages | LOOKUP JOIN languages_lookup ON language_code)
            | WHERE does_not_exist::LONG > 0
            | KEEP emp_no, language_code
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testSubqueryWithLookupJoin").nestedPath("load").run();
    }
}
