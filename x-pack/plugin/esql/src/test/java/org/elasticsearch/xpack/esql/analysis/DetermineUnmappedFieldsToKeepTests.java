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

import static org.hamcrest.Matchers.containsString;
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
        UnmappedFieldsPattern pattern = patternOf(test().statement(setUnmappedLoadAll("FROM test")));
        assertKept(pattern, "unmapped_extra", "first_name_suffix");
        assertNotKept(pattern, excl());
    }

    public void testKeepStar() {
        UnmappedFieldsPattern pattern = patternOf(test().statement(setUnmappedLoadAll("FROM test | KEEP *")));
        assertKept(pattern, "unmapped_extra");
        assertNotKept(pattern, excl());
    }

    public void testKeepWildcard() {
        UnmappedFieldsPattern pattern = patternOf(test().statement(setUnmappedLoadAll("FROM test | KEEP first_name*")));
        assertKept(pattern, "first_name_suffix");
        // Mapped fields (including "first_name" itself) are excluded even though they match the wildcard.
        assertNotKept(pattern, excl());
        assertNotKept(pattern, "unmapped_extra", "salary_bonus");
    }

    public void testKeepExactName() {
        // KEEP of a mapped field ("salary") keeps no unmapped source field.
        UnmappedFieldsPattern pattern = patternOf(test().statement(setUnmappedLoadAll("FROM test | KEEP salary")));
        assertNotKept(pattern, excl());
        assertNotKept(pattern, "unmapped_extra", "salary_bonus");
    }

    public void testKeepWildcardIgnoresMappedExactNameInSameCommand() {
        // One KEEP lists alternatives (OR): "first_name_suffix" matches "first_name*"; "salary" is mapped and
        // contributes nothing to unmapped-field selection.
        UnmappedFieldsPattern pattern = patternOf(test().statement(setUnmappedLoadAll("FROM test | KEEP first_name*, salary")));
        assertKept(pattern, "first_name_suffix");
        assertNotKept(pattern, excl());
        assertNotKept(pattern, "salary_bonus", "unmapped_extra");
    }

    public void testKeepSingleCommandOrAcrossWildcardTerms() {
        // One KEEP: alternatives within the projection list (OR). Both unmapped wildcard families survive.
        UnmappedFieldsPattern pattern = patternOf(test().statement(setUnmappedLoadAll("FROM test | KEEP first_name*, salary_bonus*")));
        assertKept(pattern, "first_name_suffix", "salary_bonus");
        assertNotKept(pattern, excl());
        assertNotKept(pattern, "unmapped_extra", "first_grade");
    }

    public void testChainedKeepCombinesOrGroupsWithAnd() {
        // Chained KEEPs intersect OR groups: only fields matching every group survive. "first_name_suffix"
        // matches both "first*" and "first_name*"; "first_grade" matches only the first group.
        UnmappedFieldsPattern pattern = patternOf(
            test().statement(setUnmappedLoadAll("FROM test | KEEP first*, salary_bonus* | KEEP first_name*"))
        );
        assertKept(pattern, "first_name_suffix");
        assertNotKept(pattern, "first_grade", "salary_bonus", "unmapped_extra");
        assertNotKept(pattern, excl());
    }

    public void testDrop() {
        UnmappedFieldsPattern pattern = patternOf(test().statement(setUnmappedLoadAll("FROM test | DROP salary")));
        assertKept(pattern, "unmapped_extra", "first_name_suffix");
        assertNotKept(pattern, excl("salary"));
    }

    public void testRename() {
        // The RENAME target "x" shadows any unmapped source field of the same name.
        UnmappedFieldsPattern pattern = patternOf(test().statement(setUnmappedLoadAll("FROM test | RENAME last_name AS x")));
        assertKept(pattern, "unmapped_extra");
        assertNotKept(pattern, excl("x"));
    }

    public void testKeepThenEval() {
        // EVAL uses a literal so it does not reference a field excluded by KEEP.
        UnmappedFieldsPattern pattern = patternOf(test().statement(setUnmappedLoadAll("FROM test | KEEP first_name* | EVAL z = 1")));
        assertKept(pattern, "first_name_suffix");
        assertNotKept(pattern, excl("z"));
        assertNotKept(pattern, "unmapped_extra");
    }

    public void testEvalThenKeep() {
        UnmappedFieldsPattern pattern = patternOf(test().statement(setUnmappedLoadAll("FROM test | EVAL z = 1 | KEEP first_name*")));
        assertKept(pattern, "first_name_suffix");
        assertNotKept(pattern, excl("z"));
        assertNotKept(pattern, "unmapped_extra");
    }

    public void testDropThenRename() {
        UnmappedFieldsPattern pattern = patternOf(test().statement(setUnmappedLoadAll("FROM test | DROP salary | RENAME last_name AS x")));
        assertKept(pattern, "unmapped_extra");
        assertNotKept(pattern, excl("x", "salary"));
    }

    public void testKeepWildcardThenEvalShadow() {
        // EVAL introduces first_name, the same name as a mapped field; it is excluded even though it
        // matches the KEEP wildcard "first*". Other first* source fields are still kept.
        UnmappedFieldsPattern pattern = patternOf(
            test().statement(setUnmappedLoadAll("FROM test | KEEP first* | EVAL first_name = to_upper(first_name)"))
        );
        assertKept(pattern, "first_grade");
        assertNotKept(pattern, excl("first_name"));
        assertNotKept(pattern, "unmapped_extra");
    }

    public void testKeepWildcardThenDropWildcard() {
        // KEEP narrows includes to "first*"; DROP then subtracts the "first_name*" sub-family. Both commands
        // contribute: "first_grade" (first* but not first_name*) survives, "first_name_suffix" does not.
        UnmappedFieldsPattern pattern = patternOf(
            test().statement(setUnmappedLoadAll("FROM test | KEEP first*, salary | DROP first_name*"))
        );
        assertKept(pattern, "first_grade");
        assertNotKept(pattern, "first_name_suffix", "unmapped_extra", "salary_bonus");
        assertNotKept(pattern, excl());
    }

    public void testKeepThenDropRemovesAllMappedColumns() {
        // KEEP first* leaves only the mapped field "first_name"; DROP first_name* then removes it, so NO mapped
        // column survives. This is still valid under LOAD_ALL — analysis must not fail: ResolvingProject always
        // re-appends _unmapped_fields, so the projection is never empty, and unmapped source fields matching
        // "first*" but not "first_name*" (e.g. "first_pet") are still kept. We cannot know at planning time
        // whether such fields exist in _source, so erroring would be wrong.
        UnmappedFieldsPattern pattern = patternOf(test().statement(setUnmappedLoadAll("FROM test | KEEP first* | DROP first_name*")));
        assertKept(pattern, "first_pet", "first_grade");
        assertNotKept(pattern, "first_name_suffix", "unmapped_extra");
        assertNotKept(pattern, excl());
    }

    public void testEvalShadowThenKeepWildcard() {
        // The EVAL output "first_name_x" matches the later KEEP wildcard "first_name*" but is shadowed by the
        // EVAL, so it is not kept; other "first_name*" source fields still are.
        UnmappedFieldsPattern pattern = patternOf(
            test().statement(setUnmappedLoadAll("FROM test | EVAL first_name_x = 1 | KEEP first_name*"))
        );
        assertKept(pattern, "first_name_suffix");
        assertNotKept(pattern, excl("first_name_x"));
        assertNotKept(pattern, "unmapped_extra");
    }

    public void testRenameThenEval() {
        // The RENAME target ("x") and the EVAL output ("y") each shadow a same-named unmapped source field.
        UnmappedFieldsPattern pattern = patternOf(test().statement(setUnmappedLoadAll("FROM test | RENAME last_name AS x | EVAL y = 2")));
        assertKept(pattern, "unmapped_extra");
        assertNotKept(pattern, excl("x", "y"));
    }

    public void testKeepWildcardThenRename() {
        // RENAME after KEEP: the rename target "first_name_x" matches the KEEP wildcard "first_name*" but is
        // shadowed by the rename, so it is not kept; other "first_name*" source fields still are.
        UnmappedFieldsPattern pattern = patternOf(
            test().statement(setUnmappedLoadAll("FROM test | KEEP first_name* | RENAME first_name AS first_name_x"))
        );
        assertKept(pattern, "first_name_suffix");
        assertNotKept(pattern, excl("first_name_x"));
        assertNotKept(pattern, "unmapped_extra");
    }

    public void testChainedKeepWildcardsIntersect() {
        // Chained KEEP wildcards apply AND semantics: only source fields matching BOTH "first*" and
        // "first_name*" survive. "first_name_suffix" matches both; "first_grade" matches only "first*".
        UnmappedFieldsPattern pattern = patternOf(test().statement(setUnmappedLoadAll("FROM test | KEEP first* | KEEP first_name*")));
        assertKept(pattern, "first_name_suffix");
        assertNotKept(pattern, "first_grade", "unmapped_extra");
        assertNotKept(pattern, excl());
    }

    public void testChainedDropsAccumulateExcludes() {
        // Chained DROPs accumulate excludes. DROP salary removes only the exact name "salary", so the unmapped
        // "salary_bonus" survives; DROP first_name* removes the entire "first_name*" family.
        UnmappedFieldsPattern pattern = patternOf(test().statement(setUnmappedLoadAll("FROM test | DROP salary | DROP first_name*")));
        assertKept(pattern, "unmapped_extra", "first_grade", "salary_bonus");
        assertNotKept(pattern, "first_name_suffix");
        assertNotKept(pattern, excl("salary"));
    }

    public void testKeepThenDropThenEval() {
        // Three commands, each observable: KEEP first*, salary narrows includes to "first*"; DROP first_name*
        // excludes that sub-family; EVAL first_grade = 1 shadows the exact name "first_grade". So
        // "first_grade_bonus" (first*, not first_name*, not the exact eval name) survives, but "first_grade" does not.
        UnmappedFieldsPattern pattern = patternOf(
            test().statement(setUnmappedLoadAll("FROM test | KEEP first*, salary | DROP first_name* | EVAL first_grade = 1"))
        );
        assertKept(pattern, "first_grade_bonus");
        assertNotKept(pattern, "first_grade", "first_name_suffix", "unmapped_extra");
        assertNotKept(pattern, excl());
    }

    public void testLoadAllUnmappedFieldsColumnNotDirectlyReferenceable() {
        // _unmapped_fields is a synthetic column; it must not be explicitly referenceable by name.
        test().statementError(
            setUnmappedLoadAll("FROM test | KEEP @timestamp, _unmapped_fields"),
            containsString("Unknown column [_unmapped_fields]")
        );
    }

    public void testDropUnmappedFieldsColumn() {
        // _unmapped_fields is a synthetic column; explicitly DROPping it by name must be rejected.
        test().statementError(setUnmappedLoadAll("FROM test | DROP _unmapped_fields"), containsString("Unknown column [_unmapped_fields]"));
    }

    public void testRenameUnmappedFieldsColumn() {
        // _unmapped_fields is a synthetic column; explicitly RENAMEing it must be rejected.
        test().statementError(
            setUnmappedLoadAll("FROM test | RENAME _unmapped_fields AS extras"),
            containsString("Unknown column [_unmapped_fields]")
        );
    }

    public void testScalarFunctionOnUnmappedFieldsColumn() {
        // _unmapped_fields is a synthetic column; using it as a scalar-function argument must be
        // rejected — the column can only appear in the output implicitly.
        test().statementError(
            setUnmappedLoadAll("FROM test | EVAL len = LENGTH(_unmapped_fields)"),
            containsString("Unknown column [_unmapped_fields]")
        );
    }

    private static void assertKept(UnmappedFieldsPattern pattern, String... names) {
        for (String name : names) {
            assertThat("expected [" + name + "] to be kept", pattern.matches(name), is(true));
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

    /** Returns the unmapped-fields pattern from the plan's {@link EsRelation}. */
    private static UnmappedFieldsPattern patternOf(LogicalPlan plan) {
        EsRelation relation = EsqlTestUtils.singleValue(plan.collect(EsRelation.class));
        return EsqlTestUtils.singleValue(CollectionUtils.collect(relation.output(), UnmappedFieldsAttribute.class)).pattern();
    }
}
