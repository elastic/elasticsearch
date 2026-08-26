/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsAttribute;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsPattern;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

/**
 * Tests for {@link org.elasticsearch.xpack.esql.analysis.rules.DetermineUnmappedFieldsToKeep}, the rule that annotates each non-LOOKUP
 * {@link EsRelation} with the {@link UnmappedFieldsPattern} describing which additional (currently
 * unmapped) source fields survive to the query output under {@code SET unmapped_fields="LOAD_ALL"}.
 *
 * <p>Assertions are behavioural: rather than pinning the exact include/exclude lists, they check which
 * candidate field names the resulting pattern would keep via {@link UnmappedFieldsPattern#matches(String)}.
 */
public class DetermineUnmappedFieldsToKeepTests extends AnalyzerUnmappedTestBase {

    /**
     * Mapped field names in the "test" index (from mapping-basic.json). They are always excluded from
     * {@code _unmapped_fields} (added to the pattern's excludes from {@code EsRelation.output()}), so no
     * pattern should ever keep them.
     */
    private static final List<String> TEST_MAPPED_FIELDS = List.of(
        "_meta_field",
        "emp_no",
        "first_name",
        "gender",
        "hire_date",
        "job",
        "job.raw",
        "languages",
        "last_name",
        "long_noidx",
        "salary"
    );

    /**
     * The field names expected to be excluded from expansion: the {@code extra} names introduced by
     * DROP/RENAME/EVAL, plus every mapped field name.
     */
    private static List<String> excl(String... extra) {
        return Stream.concat(Arrays.stream(extra), TEST_MAPPED_FIELDS.stream()).distinct().toList();
    }

    public void testNoCommand() {
        UnmappedFieldsPattern pattern = patternFor("FROM test");
        assertKept(pattern, "unmapped_extra", "first_name_suffix", "address.city", "address.city.zip");
        assertNotKept(pattern, excl());
        assertKeptAnyOtherName(pattern, excl());
    }

    public void testKeepStar() {
        UnmappedFieldsPattern pattern = patternFor("FROM test | KEEP *");
        assertKept(pattern, "unmapped_extra");
        assertNotKept(pattern, excl());
    }

    public void testKeepWildcard() {
        UnmappedFieldsPattern pattern = patternFor("FROM test | KEEP first_name*");
        assertKept(pattern, "first_name_suffix", "first_name.sub", "first_name.sub.deeper");
        assertNotKept(pattern, excl());
        assertNotKept(pattern, "unmapped_extra", "salary_bonus");
    }

    /**
     * A dotted keep pattern selects source keys several subfield levels deep. {@code job} and {@code job.raw} are mapped, so they
     * are excluded no matter what, but any other {@code job.*} key in {@code _source} survives.
     */
    public void testKeepDottedWildcardMatchesSubfields() {
        UnmappedFieldsPattern pattern = patternFor("FROM test | KEEP job.*");
        assertKept(pattern, "job.title", "job.title.short");
        assertNotKept(pattern, "jobless", "unmapped_extra");
        assertNotKept(pattern, excl());
    }

    public void testKeepExactNameOmitsUnmappedFieldsAttribute() {
        assertNoUnmappedFieldsAttribute("FROM test | KEEP salary");
    }

    public void testKeepExactNameBeforePatternOmitsUnmappedFieldsAttribute() {
        assertNoUnmappedFieldsAttribute("FROM test | KEEP salary | KEEP sal*");
    }

    public void testKeepExactNameAfterPatternOmitsUnmappedFieldsAttribute() {
        assertNoUnmappedFieldsAttribute("FROM test | KEEP first_name* | KEEP first_name");
    }

    public void testKeepThenDropOmitsUnmappedFieldsAttribute() {
        assertNoUnmappedFieldsAttribute("FROM test | KEEP salary | DROP salary");
    }

    public void testStatsOmitsUnmappedFieldsAttribute() {
        assertNoUnmappedFieldsAttribute("FROM test | STATS c = COUNT(*)");
    }

    public void testStatsByMappedFieldOmitsUnmappedFieldsAttribute() {
        assertNoUnmappedFieldsAttribute("FROM test | STATS c = COUNT(*) BY languages");
    }

    public void testStatsByUnmappedFieldOmitsUnmappedFieldsAttribute() {
        assertNoUnmappedFieldsAttribute("FROM test | STATS c = COUNT(*) BY unmapped_extra");
    }

    public void testKeepWildcardThenStatsOmitsUnmappedFieldsAttribute() {
        assertNoUnmappedFieldsAttribute("FROM test | KEEP first_name* | STATS c = COUNT(*)");
    }

    public void testEvalThenStatsOmitsUnmappedFieldsAttribute() {
        assertNoUnmappedFieldsAttribute("FROM test | EVAL z = salary + 1 | STATS c = COUNT(*) BY z");
    }

    public void testDropWildcardThenStatsOmitsUnmappedFieldsAttribute() {
        assertNoUnmappedFieldsAttribute("FROM test | DROP first_name* | STATS c = COUNT(*)");
    }

    public void testStatsThenKeepStarOmitsUnmappedFieldsAttribute() {
        assertNoUnmappedFieldsAttribute("FROM test | STATS c = COUNT(*) | KEEP *");
    }

    public void testStatsThenEvalOmitsUnmappedFieldsAttribute() {
        assertNoUnmappedFieldsAttribute("FROM test | STATS c = COUNT(*) | EVAL z = c + 1");
    }

    /**
     * A wildcard {@code DROP} or {@code KEEP} contributes a pattern of its own, so these check that a projection downstream of
     * {@code STATS} cannot re-open what the aggregate already closed.
     */
    public void testStatsThenDropWildcardOmitsUnmappedFieldsAttribute() {
        assertNoUnmappedFieldsAttribute("FROM test | STATS c = COUNT(*) BY languages | DROP lang*");
    }

    public void testDropWildcardThenStatsThenKeepWildcardOmitsUnmappedFieldsAttribute() {
        assertNoUnmappedFieldsAttribute("FROM test | DROP first_name* | STATS c = COUNT(*) BY languages | KEEP lang*");
    }

    public void testKeepWildcardIgnoresMappedExactNameInSameCommand() {
        UnmappedFieldsPattern pattern = patternFor("FROM test | KEEP first_name*, salary");
        assertKept(pattern, "first_name_suffix");
        assertNotKept(pattern, excl());
        assertNotKept(pattern, "salary_bonus", "unmapped_extra");
    }

    public void testKeepSingleCommandOrAcrossWildcardTerms() {
        UnmappedFieldsPattern pattern = patternFor("FROM test | KEEP first_name*, salary_bonus*");
        assertKept(pattern, "first_name_suffix", "salary_bonus");
        assertNotKept(pattern, excl());
        assertNotKept(pattern, "unmapped_extra", "first_grade");
    }

    public void testChainedKeepCombinesOrGroupsWithAnd() {
        UnmappedFieldsPattern pattern = patternFor("FROM test | KEEP first*, salary_bonus* | KEEP first_name*");
        assertKept(pattern, "first_name_suffix");
        assertNotKept(pattern, "first_grade", "salary_bonus", "unmapped_extra");
        assertNotKept(pattern, excl());
    }

    public void testDrop() {
        UnmappedFieldsPattern pattern = patternFor("FROM test | DROP salary");
        assertKept(pattern, "unmapped_extra", "first_name_suffix", "address.city.zip");
        assertNotKept(pattern, excl("salary"));
        assertKeptAnyOtherName(pattern, excl("salary"));
    }

    public void testDropWildcardDoesNotRemoveSyntheticColumn() {
        UnmappedFieldsPattern pattern = patternFor("FROM test | DROP *unmapped_fields");
        assertKept(pattern, "unmapped_extra");
        assertNotKept(pattern, "source_unmapped_fields");
        assertNotKept(pattern, excl());
    }

    public void testRename() {
        UnmappedFieldsPattern pattern = patternFor("FROM test | RENAME last_name AS x");
        assertKept(pattern, "unmapped_extra");
        assertNotKept(pattern, excl("x"));
        assertKeptAnyOtherName(pattern, excl("x"));
    }

    public void testKeepThenEval() {
        // EVAL uses a literal so it does not reference a field excluded by KEEP.
        UnmappedFieldsPattern pattern = patternFor("FROM test | KEEP first_name* | EVAL z = 1");
        assertKept(pattern, "first_name_suffix");
        assertNotKept(pattern, excl("z"));
        assertNotKept(pattern, "unmapped_extra");
    }

    public void testEvalThenKeep() {
        UnmappedFieldsPattern pattern = patternFor("FROM test | EVAL z = 1 | KEEP first_name*");
        assertKept(pattern, "first_name_suffix");
        assertNotKept(pattern, excl("z"));
        assertNotKept(pattern, "unmapped_extra");
    }

    public void testDropThenRename() {
        UnmappedFieldsPattern pattern = patternFor("FROM test | DROP salary | RENAME last_name AS x");
        assertKept(pattern, "unmapped_extra");
        assertNotKept(pattern, excl("x", "salary"));
        assertKeptAnyOtherName(pattern, excl("x", "salary"));
    }

    public void testKeepWildcardThenEvalShadow() {
        UnmappedFieldsPattern pattern = patternFor("FROM test | KEEP first* | EVAL first_name = to_upper(first_name)");
        assertKept(pattern, "first_grade");
        assertNotKept(pattern, excl("first_name"));
        assertNotKept(pattern, "unmapped_extra");
    }

    public void testKeepWildcardThenDropWildcard() {
        UnmappedFieldsPattern pattern = patternFor("FROM test | KEEP first*, salary | DROP first_name*");
        assertKept(pattern, "first_grade");
        assertNotKept(pattern, "first_name_suffix", "unmapped_extra", "salary_bonus");
        assertNotKept(pattern, excl());
    }

    public void testKeepThenDropRemovesAllMappedColumns() {
        // KEEP first* leaves only the mapped field "first_name"; DROP first_name* then removes it, so NO mapped
        // column survives. This is still valid under LOAD_ALL — analysis must not fail: the DROP wildcard still
        // leaves unmapped source fields matching "first*" but not "first_name*" (e.g. "first_pet"), so the
        // synthetic $$unmapped_fields column remains. We cannot know at planning time whether such fields exist
        // in _source, so erroring would be wrong.
        UnmappedFieldsPattern pattern = patternFor("FROM test | KEEP first* | DROP first_name*");
        assertKept(pattern, "first_pet", "first_grade");
        assertNotKept(pattern, "first_name_suffix", "unmapped_extra");
        assertNotKept(pattern, excl());
    }

    public void testEvalShadowThenKeepWildcard() {
        UnmappedFieldsPattern pattern = patternFor("FROM test | EVAL first_name_x = 1 | KEEP first_name*");
        assertKept(pattern, "first_name_suffix");
        assertNotKept(pattern, excl("first_name_x"));
        assertNotKept(pattern, "unmapped_extra");
    }

    public void testRenameThenEval() {
        UnmappedFieldsPattern pattern = patternFor("FROM test | RENAME last_name AS x | EVAL y = 2");
        assertKept(pattern, "unmapped_extra");
        assertNotKept(pattern, excl("x", "y"));
        assertKeptAnyOtherName(pattern, excl("x", "y"));
    }

    public void testKeepWildcardThenRename() {
        UnmappedFieldsPattern pattern = patternFor("FROM test | KEEP first_name* | RENAME first_name AS first_name_x");
        assertKept(pattern, "first_name_suffix");
        assertNotKept(pattern, excl("first_name_x"));
        assertNotKept(pattern, "unmapped_extra");
    }

    public void testChainedKeepWildcardsIntersect() {
        UnmappedFieldsPattern pattern = patternFor("FROM test | KEEP first* | KEEP first_name*");
        assertKept(pattern, "first_name_suffix");
        assertNotKept(pattern, "first_grade", "unmapped_extra");
        assertNotKept(pattern, excl());
    }

    public void testChainedDropsAccumulateExcludes() {
        UnmappedFieldsPattern pattern = patternFor("FROM test | DROP salary | DROP first_name*");
        assertKept(pattern, "unmapped_extra", "first_grade", "salary_bonus");
        assertNotKept(pattern, "first_name_suffix");
        assertNotKept(pattern, excl("salary"));
    }

    public void testKeepThenDropThenEval() {
        UnmappedFieldsPattern pattern = patternFor("FROM test | KEEP first*, salary | DROP first_name* | EVAL first_grade = 1");
        assertKept(pattern, "first_grade_bonus");
        assertNotKept(pattern, "first_grade", "first_name_suffix", "unmapped_extra");
        assertNotKept(pattern, excl());
    }

    public void testKeepMultipleUnmatchedWildcards() {
        UnmappedFieldsPattern pattern = patternFor("FROM test | KEEP nomatch1*, nomatch2*");
        assertKept(pattern, "nomatch1_a", "nomatch2_b");
        assertNotKept(pattern, "unmapped_extra");
        assertNotKept(pattern, excl());
    }

    public void testDropExactUnmappedName() {
        UnmappedFieldsPattern pattern = patternFor("FROM test | DROP unmapped_extra");
        assertKept(pattern, "first_name_suffix");
        assertNotKept(pattern, excl("unmapped_extra"));
        assertKeptAnyOtherName(pattern, excl("unmapped_extra"));
    }

    /**
     * {@code _unmapped_fields} is not a reserved name: the synthetic column is called
     * {@link UnmappedFieldsAttribute#ATTRIBUTE_NAME}, which no query can spell. So a query referencing
     * {@code _unmapped_fields} gets an ordinary source field demand-loaded as a keyword — which is an explicit
     * column of the relation. A pattern-less KEEP of that name therefore yields no synthetic column at all.
     */
    public void testKeepUnmappedFieldsIsAnOrdinarySourceField() {
        assertNoUnmappedFieldsAttribute("FROM test | KEEP _unmapped_fields");
    }

    public void testDropUnmappedFieldsIsAnOrdinarySourceField() {
        UnmappedFieldsPattern pattern = patternFor("FROM test | DROP _unmapped_fields");
        assertKept(pattern, "unmapped_extra");
        assertNotKept(pattern, excl("_unmapped_fields"));
    }

    public void testRenameUnmappedFieldsIsAnOrdinarySourceField() {
        UnmappedFieldsPattern pattern = patternFor("FROM test | RENAME _unmapped_fields AS extras");
        assertKept(pattern, "unmapped_extra");
        assertNotKept(pattern, excl("_unmapped_fields", "extras"));
    }

    public void testScalarFunctionOnUnmappedFieldsIsAnOrdinarySourceField() {
        UnmappedFieldsPattern pattern = patternFor("FROM test | EVAL len = LENGTH(_unmapped_fields)");
        assertKept(pattern, "unmapped_extra");
        assertNotKept(pattern, excl("_unmapped_fields", "len"));
    }

    // -----------------------------------------------------------------------
    // LOOKUP JOIN: the left side's KEEP/DROP constraints must survive the join
    // -----------------------------------------------------------------------

    public void testKeepWildcardBelowLookupJoinIsRespected() {
        UnmappedFieldsPattern pattern = patternForJoin(
            "FROM test | EVAL language_code = languages | KEEP first_name*, language_code | LOOKUP JOIN languages_lookup ON language_code",
            test().addLanguagesLookup()
        );
        // first_name* pattern should survive the join
        assertKept(pattern, "first_name_suffix", "first_name.sub");
        // lookup index fields (language_name, language_code) and test-mapped fields are excluded
        assertNotKept(pattern, excl("language_code", "language_name"));
        // other unmapped fields not matching first_name* are not kept
        assertNotKept(pattern, "unmapped_extra", "salary_bonus");
    }

    /** A KEEP below the join with a wildcard still restricts — looking at the intersection across the join. */
    public void testDropWildcardBelowLookupJoinIsRespected() {
        UnmappedFieldsPattern pattern = patternForJoin(
            "FROM test | DROP salary | EVAL language_code = languages | LOOKUP JOIN languages_lookup ON language_code",
            test().addLanguagesLookup()
        );
        // salary is excluded, everything else kept
        assertKept(pattern, "unmapped_extra", "first_name_suffix");
        assertNotKept(pattern, excl("salary", "language_code", "language_name"));
        assertKeptAnyOtherName(pattern, excl("salary", "language_code", "language_name"));
    }

    /**
     * The lookup index's own output fields ({@code language_code}, {@code language_name}) are excluded from expansion via
     * {@link UnmappedFieldsPattern#withAdditionalExcludes} on the Join's output, even though they come from the right side.
     */
    public void testLookupIndexFieldsAreExcludedFromPattern() {
        UnmappedFieldsPattern pattern = patternForJoin(
            "FROM test | EVAL language_code = languages | LOOKUP JOIN languages_lookup ON language_code",
            test().addLanguagesLookup()
        );
        assertKept(pattern, "unmapped_extra");
        // language_code and language_name come from the lookup index - they must be excluded from the blob
        assertNotKept(pattern, excl("language_code", "language_name"));
        assertKeptAnyOtherName(pattern, excl("language_code", "language_name"));
    }

    /**
     * Fields from a lookup index with names that overlap existing columns (e.g. {@code salary}) must be excluded from expansion.
     * We use {@code EVAL language_code = languages} to produce an integer join key matching the lookup's integer {@code language_code}.
     */
    public void testLookupIndexOverlappingFieldIsExcluded() {
        UnmappedFieldsPattern pattern = patternForJoin(
            "FROM test | EVAL language_code = languages | LOOKUP JOIN custom_lookup ON language_code",
            test().addLookupIndex("custom_lookup", lookupIndexWithOverlappingFields())
        );
        // salary, lookup_only are lookup-index output fields — excluded from blob
        assertNotKept(pattern, excl("salary", "lookup_only", "language_code"));
        assertKept(pattern, "unmapped_extra");
    }

    /**
     * Multi-column LOOKUP JOIN ({@code ON field1, field2}): all join-key names and lookup output fields are
     * excluded from the unmapped-fields blob.
     */
    public void testMultiColumnLookupJoin() {
        // EVAL two keyword columns that match the lookup's two key fields.
        UnmappedFieldsPattern pattern = patternForJoin(
            "FROM test | EVAL language_code = first_name, language_name = last_name"
                + " | LOOKUP JOIN keyword_languages_lookup ON language_code, language_name",
            test().addLookupIndex(keywordLanguagesLookup())
        );
        // language_code and language_name appear in join output — excluded from blob
        assertNotKept(pattern, excl("language_code", "language_name"));
        assertKept(pattern, "unmapped_extra");
    }

    /**
     * ON-expression LOOKUP JOIN ({@code ON lc == language_code}): the derived column and all lookup output fields
     * are excluded from the unmapped-fields blob.
     */
    public void testLookupJoinOnExpression() {
        UnmappedFieldsPattern pattern = patternForJoin(
            "FROM test | EVAL lc = first_name" + " | LOOKUP JOIN keyword_languages_lookup ON lc == language_code",
            test().addLookupIndex(keywordLanguagesLookup())
        );
        // lc is the derived join key; language_code and language_name come from the lookup — all excluded
        assertNotKept(pattern, excl("lc", "language_code", "language_name"));
        assertKept(pattern, "unmapped_extra");
    }

    // -----------------------------------------------------------------------
    // ENRICH: already a UnaryPlan/GeneratingPlan, so recursion was already correct;
    // these tests confirm nothing regressed and that enrich output names are excluded.
    // -----------------------------------------------------------------------

    public void testEnrichOutputFieldsAreExcludedFromPattern() {
        // languages policy adds language_name; the enrich output name must be excluded from the blob.
        UnmappedFieldsPattern pattern = patternForEnrich(
            "FROM test | ENRICH languages ON languages",
            test().addAnalysisTestsEnrichResolution()
        );
        assertKept(pattern, "unmapped_extra");
        // language_name is the enrich output field — must not reappear from the blob
        assertNotKept(pattern, excl("language_name"));
        assertKeptAnyOtherName(pattern, excl("language_name"));
    }

    public void testKeepWildcardBelowEnrichIsRespected() {
        // EVAL a match key before KEEP so the match field (lc) is available after the wildcard KEEP narrows the output.
        UnmappedFieldsPattern pattern = patternForEnrich(
            "FROM test | EVAL lc = languages | KEEP first_name*, lc | ENRICH languages ON lc",
            test().addAnalysisTestsEnrichResolution()
        );
        assertKept(pattern, "first_name_suffix");
        assertNotKept(pattern, excl("language_name", "lc"));
        assertNotKept(pattern, "unmapped_extra");
    }

    // -----------------------------------------------------------------------
    // Helpers for multi-relation queries (LOOKUP JOIN has left + right EsRelation)
    // -----------------------------------------------------------------------

    /**
     * Like {@link #patternFor(String)}, but for queries involving a LOOKUP JOIN which have two {@link EsRelation}s.
     * Returns the pattern on the non-LOOKUP relation (the primary index).
     * Automatically skips when {@code OPTIONAL_FIELDS_LOAD_ALL_JOIN_AND_ENRICH} is disabled.
     */
    private static UnmappedFieldsPattern patternForJoin(String query, org.elasticsearch.xpack.esql.TestAnalyzer analyzer) {
        assumeTrue(
            "Requires OPTIONAL_FIELDS_LOAD_ALL_JOIN_AND_ENRICH",
            EsqlCapabilities.Cap.OPTIONAL_FIELDS_LOAD_ALL_JOIN_AND_ENRICH.isEnabled()
        );
        LogicalPlan plan = analyzer.statement(setUnmappedLoadAll(query));
        EsRelation primary = plan.collect(EsRelation.class)
            .stream()
            .filter(r -> r.indexMode() != IndexMode.LOOKUP)
            .findFirst()
            .orElseThrow(() -> new AssertionError("No non-LOOKUP EsRelation found"));
        return EsqlTestUtils.singleValue(CollectionUtils.collect(primary.output(), UnmappedFieldsAttribute.class)).pattern();
    }

    /**
     * Like {@link #patternFor(String, org.elasticsearch.xpack.esql.TestAnalyzer)}, but for queries using ENRICH.
     * Automatically skips when {@code OPTIONAL_FIELDS_LOAD_ALL_JOIN_AND_ENRICH} is disabled.
     */
    private static UnmappedFieldsPattern patternForEnrich(String query, org.elasticsearch.xpack.esql.TestAnalyzer analyzer) {
        assumeTrue(
            "Requires OPTIONAL_FIELDS_LOAD_ALL_JOIN_AND_ENRICH",
            EsqlCapabilities.Cap.OPTIONAL_FIELDS_LOAD_ALL_JOIN_AND_ENRICH.isEnabled()
        );
        return patternFor(query, analyzer);
    }

    /** Like {@link #patternFor(String)}, but accepts a pre-configured analyzer (e.g. one with extra enrich policies). */
    private static UnmappedFieldsPattern patternFor(String query, org.elasticsearch.xpack.esql.TestAnalyzer analyzer) {
        LogicalPlan plan = analyzer.statement(setUnmappedLoadAll(query));
        EsRelation relation = EsqlTestUtils.singleValue(plan.collect(EsRelation.class));
        return EsqlTestUtils.singleValue(CollectionUtils.collect(relation.output(), UnmappedFieldsAttribute.class)).pattern();
    }

    private static void assertKept(UnmappedFieldsPattern pattern, String... names) {
        for (String name : names) {
            assertThat("expected [" + name + "] to be kept", pattern.matches(name), is(true));
        }
    }

    private static void assertNoUnmappedFieldsAttribute(String query) {
        LogicalPlan plan = test().statement(setUnmappedLoadAll(query));
        for (EsRelation relation : plan.collect(EsRelation.class)) {
            assertThat(
                "expected no UnmappedFieldsAttribute on " + relation,
                CollectionUtils.collect(relation.output(), UnmappedFieldsAttribute.class),
                empty()
            );
        }
    }

    private static void assertNotKept(UnmappedFieldsPattern pattern, String... names) {
        assertNotKept(pattern, List.of(names));
    }

    private static void assertNotKept(UnmappedFieldsPattern pattern, List<String> names) {
        for (String name : names) {
            assertThat("expected [" + name + "] to NOT be kept", pattern.matches(name), is(false));
        }
    }

    /**
     * For a pattern that only excludes, every name it does not exclude is kept. Asserted on a random name so the test says something
     * about all such names, not only the handful spelled out above. Only for patterns whose excludes are plain names: a wildcard
     * exclude could match the random name.
     */
    private static void assertKeptAnyOtherName(UnmappedFieldsPattern pattern, List<String> excluded) {
        assertKept(pattern, randomValueOtherThanMany(excluded::contains, () -> randomAlphaOfLength(10)));
    }

    private static UnmappedFieldsPattern patternFor(String query) {
        return patternOf(test().statement(setUnmappedLoadAll(query)));
    }

    private static UnmappedFieldsPattern patternOf(LogicalPlan plan) {
        EsRelation relation = EsqlTestUtils.singleValue(plan.collect(EsRelation.class));
        return EsqlTestUtils.singleValue(CollectionUtils.collect(relation.output(), UnmappedFieldsAttribute.class)).pattern();
    }
}
