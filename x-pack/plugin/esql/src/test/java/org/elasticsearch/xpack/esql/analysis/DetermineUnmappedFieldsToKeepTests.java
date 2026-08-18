/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.EsqlTestUtils;
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

    public DetermineUnmappedFieldsToKeepTests(String name, boolean pinCurrentVersion) {
        super(name, pinCurrentVersion);
    }

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

    private static void assertKept(UnmappedFieldsPattern pattern, String... names) {
        for (String name : names) {
            assertThat("expected [" + name + "] to be kept", pattern.matches(name), is(true));
        }
    }

    private void assertNoUnmappedFieldsAttribute(String query) {
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

    private UnmappedFieldsPattern patternFor(String query) {
        return patternOf(test().statement(setUnmappedLoadAll(query)));
    }

    private static UnmappedFieldsPattern patternOf(LogicalPlan plan) {
        EsRelation relation = EsqlTestUtils.singleValue(plan.collect(EsRelation.class));
        return EsqlTestUtils.singleValue(CollectionUtils.collect(relation.output(), UnmappedFieldsAttribute.class)).pattern();
    }
}
