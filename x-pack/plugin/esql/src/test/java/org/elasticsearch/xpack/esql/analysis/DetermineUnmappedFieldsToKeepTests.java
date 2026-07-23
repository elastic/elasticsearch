/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsAttribute;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsPattern;
import org.junit.Ignore;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Tests for {@code Analyzer.DetermineUnmappedFieldsToKeep}, the rule that annotates each non-LOOKUP
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

    @Ignore(
        "Documents intended behaviour: KEEP of only mapped fields should keep no unmapped source field. "
            + "Currently the pattern falls back to includes=[*], so unmapped fields still pass the filter."
    )
    public void testKeepExactName() {
        // KEEP of a mapped field ("salary") keeps no unmapped source field.
        UnmappedFieldsPattern pattern = patternOf(test().statement(setUnmappedLoadAll("FROM test | KEEP salary")));
        assertNotKept(pattern, excl());
        assertNotKept(pattern, "unmapped_extra", "salary_bonus");
    }

    @Ignore(
        "Documents intended behaviour: a mapped exact name in KEEP (\"salary\") should still constrain the includes. "
            + "Currently it is dropped, leaving includes=[first_name*], so \"first_name_suffix\" passes the filter."
    )
    public void testKeepMultiplePatterns() {
        // Includes combine with AND semantics: no field can match both "first_name*" and "salary".
        UnmappedFieldsPattern pattern = patternOf(test().statement(setUnmappedLoadAll("FROM test | KEEP first_name*, salary")));
        assertNotKept(pattern, excl());
        assertNotKept(pattern, "first_name_suffix", "salary_bonus", "unmapped_extra");
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

    public void testLoadAllUnmappedFieldsColumnNotDirectlyReferenceable() {
        // _unmapped_fields is a synthetic column; it must not be explicitly referenceable by name.
        // TODO: the correct error is "Unknown column [_unmapped_fields]", but currently
        // ResolveUnmapped resolves _unmapped_fields as an ordinary unmapped keyword field,
        // then DetermineUnmappedFieldsToKeep adds a second _unmapped_fields attribute —
        // resulting in a "duplicate output attribute" verifier error instead.
        test().statementError(setUnmappedLoadAll("FROM test | KEEP @timestamp, _unmapped_fields"), containsString("_unmapped_fields"));
    }

    @Ignore(
        "Documents intended behaviour: explicitly DROPping the synthetic _unmapped_fields column should be rejected. "
            + "Currently the DROP succeeds silently."
    )
    public void testDropUnmappedFieldsColumn() {
        // _unmapped_fields is a synthetic column; explicitly DROPping it by name must be rejected.
        test().statementError(setUnmappedLoadAll("FROM test | DROP _unmapped_fields"), containsString("_unmapped_fields"));
    }

    @Ignore(
        "Documents intended behaviour: explicitly RENAMEing the synthetic _unmapped_fields column should be rejected. "
            + "Currently the RENAME succeeds silently."
    )
    public void testRenameUnmappedFieldsColumn() {
        // _unmapped_fields is a synthetic column; explicitly RENAMEing it must be rejected.
        test().statementError(setUnmappedLoadAll("FROM test | RENAME _unmapped_fields AS extras"), containsString("_unmapped_fields"));
    }

    public void testScalarFunctionOnUnmappedFieldsColumn() {
        // _unmapped_fields is a synthetic column; using it as a scalar-function argument must be
        // rejected — the column can only appear in the output implicitly.
        test().statementError(setUnmappedLoadAll("FROM test | EVAL len = LENGTH(_unmapped_fields)"), containsString("_unmapped_fields"));
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

    /** Finds the single non-LOOKUP EsRelation in the plan and returns its unmapped-fields pattern. */
    private static UnmappedFieldsPattern patternOf(LogicalPlan plan) {
        List<EsRelation> relations = plan.collect(EsRelation.class);
        assertThat("expected exactly one EsRelation", relations, hasSize(1));
        UnmappedFieldsAttribute attr = relations.get(0)
            .output()
            .stream()
            .filter(a -> a instanceof UnmappedFieldsAttribute)
            .map(a -> (UnmappedFieldsAttribute) a)
            .findFirst()
            .orElse(null);
        assertThat(attr, notNullValue());
        return attr.pattern();
    }
}
