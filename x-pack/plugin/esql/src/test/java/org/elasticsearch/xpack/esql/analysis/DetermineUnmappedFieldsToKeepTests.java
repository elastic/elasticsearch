/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VersionMode;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.analysis.rules.DetermineUnmappedFieldsToKeep;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.enrich.ResolvedEnrichPolicy;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.plan.logical.BranchPlan;
import org.elasticsearch.xpack.esql.plan.logical.Dedup;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LimitBy;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsAttribute;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsPattern;
import org.elasticsearch.xpack.esql.plan.logical.join.AbstractSubqueryJoin;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/**
 * Tests for {@link org.elasticsearch.xpack.esql.analysis.rules.DetermineUnmappedFieldsToKeep}, the rule that annotates each non-LOOKUP
 * {@link EsRelation} with the {@link UnmappedFieldsPattern} describing which additional (currently
 * unmapped) source fields survive to the query output under {@code SET unmapped_fields="LOAD_ALL"}.
 *
 * <p>Assertions are behavioural: rather than pinning the exact include/exclude lists, they check which
 * candidate field names the resulting pattern would keep via {@link UnmappedFieldsPattern#matches(String)}.
 */
public class DetermineUnmappedFieldsToKeepTests extends AnalyzerUnmappedTestBase {

    public DetermineUnmappedFieldsToKeepTests(VersionMode versionMode) {
        super(versionMode);
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

    public void testKeepBackquotedWildcardMatchesLikeUnquoted() {
        UnmappedFieldsPattern pattern = patternFor("FROM test | KEEP `first_name`*");
        assertKept(pattern, "first_name_suffix", "first_name.sub", "first_name.sub.deeper");
        assertNotKept(pattern, excl());
        assertNotKept(pattern, "unmapped_extra", "salary_bonus");
    }

    public void testKeepBackquotedDottedWildcardMatchesSubfields() {
        UnmappedFieldsPattern pattern = patternFor("FROM test | KEEP `job`.`ti`*");
        assertKept(pattern, "job.title", "job.title.short");
        assertNotKept(pattern, "jobless", "job.other", "unmapped_extra");
        assertNotKept(pattern, excl());
    }

    public void testKeepBackquotedStarIsLiteral() {
        UnmappedFieldsPattern pattern = patternFor("FROM test | KEEP first_name, `first*`*");
        assertKept(pattern, "first*", "first*_suffix");
        assertNotKept(pattern, "first_name_suffix", "firstly", "unmapped_extra");
        assertNotKept(pattern, excl());
    }

    public void testDropBackquotedWildcardMatchesLikeUnquoted() {
        UnmappedFieldsPattern pattern = patternFor("FROM test | DROP `first_name`*");
        assertKept(pattern, "unmapped_extra", "salary_bonus");
        assertNotKept(pattern, "first_name_suffix", "first_name.sub");
        assertNotKept(pattern, excl());
    }

    public void testKeepExactNameOmitsUnmappedFieldsAttribute() {
        assertNoUnmappedFieldsAttribute("FROM test | KEEP salary");
    }

    public void testEvalUnmappedThenKeepExactNameOmitsUnmappedFieldsAttribute() {
        assertNoUnmappedFieldsAttribute("FROM test | EVAL z = unmapped_extra::keyword | KEEP emp_no, z");
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

    public void testInlineStatsThenStatsOmitsUnmappedFieldsAttribute() {
        assertNoUnmappedFieldsAttribute("FROM test | INLINE STATS c = COUNT(*) | STATS s = SUM(c)");
    }

    public void testStatsThenInlineStatsOmitsUnmappedFieldsAttribute() {
        assertNoUnmappedFieldsAttribute("FROM test | STATS c = COUNT(*) | INLINE STATS m = MAX(c)");
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

    public void testDropWildcardDoesNotRemoveUnmappedFieldsColumn() {
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
        // leaves unmapped source fields matching "first*" but not "first_name*" (e.g. "first_pet"), so
        // the _unmapped_fields column remains. We cannot know at planning time whether such fields exist
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

    public void testInlineStatsExcludesAggregateAlias() {
        UnmappedFieldsPattern pattern = patternFor("FROM test | INLINE STATS c = COUNT(c2) BY languages");
        assertNotKept(pattern, excl("c", "c2", "languages"));
        assertKept(pattern, "unmapped_extra", "first_name_suffix");
    }

    public void testInlineStatsPrunedStillDropsMentionedFields() {
        UnmappedFieldsPattern pattern = patternFor("FROM test | INLINE STATS c = COUNT(c2) BY languages | DROP c");
        assertNotKept(pattern, excl("c", "c2", "languages"));
        assertKept(pattern, "unmapped_extra", "first_name_suffix");
    }

    public void testInlineStatsPrunedByKeepPatternStillDropsMentionedFields() {
        UnmappedFieldsPattern pattern = patternFor("FROM test | INLINE STATS c = COUNT(c2) BY languages | KEEP first*");
        assertNotKept(pattern, excl("c", "c2", "c3", "languages"));
        assertKept(pattern, "first_name_suffix");
    }

    public void testInlineStatsExcludesUnmappedByAlias() {
        UnmappedFieldsPattern pattern = patternFor("FROM test | INLINE STATS c = COUNT(c2) BY x = unmapped_extra");
        assertNotKept(pattern, excl("c", "c2", "x", "unmapped_extra"));
        assertKept(pattern, "first_name_suffix");
    }

    public void testMultipleInlineStatsCommandsExcludesBothAliases() {
        UnmappedFieldsPattern pattern = patternFor("FROM test | INLINE STATS c = COUNT(c2) | INLINE STATS m = MAX(salary) BY languages");
        assertNotKept(pattern, excl("c", "c2", "m", "languages"));
        assertKept(pattern, "unmapped_extra", "first_name_suffix");
    }

    public void testMultipleInlineStatsExpressionsExcludesBothAliases() {
        UnmappedFieldsPattern pattern = patternFor("FROM test | INLINE STATS c = COUNT(c2), m = MAX(salary) BY languages");
        assertNotKept(pattern, excl("c", "c2", "m", "languages"));
        assertKept(pattern, "unmapped_extra", "first_name_suffix");
    }

    public void testInlineStatsThenDropAggKeepsUnmapped() {
        UnmappedFieldsPattern pattern = patternFor("FROM test | INLINE STATS c = COUNT(*) | DROP c");
        assertNotKept(pattern, excl("c"));
        assertKept(pattern, "unmapped_extra", "first_name_suffix");
    }

    public void testInlineStatsThenKeepWildcardStillExpands() {
        UnmappedFieldsPattern pattern = patternFor("FROM test | INLINE STATS c = COUNT(*) | KEEP first_name*");
        assertKept(pattern, "first_name_suffix");
        assertNotKept(pattern, excl("c", "unmapped_extra"));
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
     * {@code _unmapped_fields} is not a reserved name. A query referencing it gets an ordinary source field demand-loaded as a
     * keyword, which is an explicit column of the relation, and a column of that name wins over the {@link UnmappedFieldsAttribute},
     * which is only named like the column it becomes. A pattern-less KEEP of that name therefore yields no such attribute at all.
     * The query still warns: the column of that name holds the source field, not what its name suggests.
     */
    public void testKeepUnmappedFieldsIsAnOrdinarySourceField() {
        assertUnmappedFieldsNameTaken("FROM test | KEEP _unmapped_fields");
    }

    /**
     * The explicit reference demand-loads a source field named {@code _unmapped_fields}, and a query with such a column of its own
     * gets no {@link UnmappedFieldsAttribute}. So the DROP leaves no {@code _unmapped_fields} column in the response.
     */
    public void testDropUnmappedFieldsOmitsUnmappedFieldsAttribute() {
        assertUnmappedFieldsNameTaken("FROM test | DROP _unmapped_fields");
    }

    public void testSubqueryKeepExactNameOmitsUnmappedFieldsAttribute() {
        assertNoUnmappedFieldsAttribute("""
            FROM (FROM test | KEEP salary), (FROM test | KEEP emp_no)
            | KEEP salary, emp_no
            """);
    }

    public void testSubqueryEvalThenKeepExactNameOmitsUnmappedFieldsAttribute() {
        assertNoUnmappedFieldsAttribute("""
            FROM (FROM test | WHERE emp_no == 10001), (FROM test | WHERE emp_no == 10002)
            | EVAL z = salary + 1
            | KEEP emp_no, z
            | SORT emp_no
            """);
    }

    public void testSubqueryEvalUnmappedConvertThenKeepExactNameOmitsUnmappedFieldsAttribute() {
        assertNoUnmappedFieldsAttribute("""
            FROM (FROM test), (FROM test)
            | EVAL z = unmapped_extra::keyword
            | KEEP emp_no, z
            """);
    }

    public void testSubqueryNoKeepAnnotatesBothRelations() {
        LogicalPlan plan = test().statement(setUnmappedLoadAll("FROM (FROM test), (FROM test)"));
        assertThat(CollectionUtils.collect(plan.output(), UnmappedFieldsAttribute.class), hasSize(1));
        List<EsRelation> relations = plan.collect(EsRelation.class);
        assertThat(relations, hasSize(2));
        for (EsRelation relation : relations) {
            UnmappedFieldsPattern pattern = unmappedFieldsPattern(relation);
            assertKept(pattern, "unmapped_extra");
            assertNotKept(pattern, excl());
        }
    }

    public void testSubqueryKeepStarAnnotatesBothRelations() {
        LogicalPlan plan = test().statement(setUnmappedLoadAll("FROM (FROM test), (FROM test) | KEEP *"));
        assertThat(CollectionUtils.collect(plan.output(), UnmappedFieldsAttribute.class), hasSize(1));
        List<EsRelation> relations = plan.collect(EsRelation.class);
        assertThat(relations, hasSize(2));
        for (EsRelation relation : relations) {
            UnmappedFieldsPattern pattern = unmappedFieldsPattern(relation);
            assertKept(pattern, "unmapped_extra");
            assertNotKept(pattern, excl());
        }
    }

    public void testSubqueryKeepInOneBranchOmitsThatBranch() {
        LogicalPlan plan = test().addLanguages().statement(setUnmappedLoadAll("""
            FROM (FROM test | KEEP emp_no), (FROM languages)
            """));
        assertThat(CollectionUtils.collect(plan.output(), UnmappedFieldsAttribute.class), hasSize(1));
        for (EsRelation relation : plan.collect(EsRelation.class)) {
            List<UnmappedFieldsAttribute> attrs = CollectionUtils.collect(relation.output(), UnmappedFieldsAttribute.class);
            if (relation.indexPattern().equals("test")) {
                assertThat(attrs, empty());
            } else {
                assertThat(attrs, hasSize(1));
                assertKept(attrs.getFirst().pattern(), "unmapped_extra");
            }
        }
    }

    public void testSubqueryStatsInOneBranchOmitsThatBranch() {
        LogicalPlan plan = test().statement(setUnmappedLoadAll("""
            FROM (FROM test), (FROM test | STATS c = COUNT(*))
            """));
        assertThat(CollectionUtils.collect(plan.output(), UnmappedFieldsAttribute.class), hasSize(1));
        UnionAll union = EsqlTestUtils.singleValue(plan.collect(UnionAll.class));
        assertThat(unmappedFieldsAttributes(EsqlTestUtils.singleValue(union.children().get(0).collect(EsRelation.class))), hasSize(1));
        assertThat(unmappedFieldsAttributes(EsqlTestUtils.singleValue(union.children().get(1).collect(EsRelation.class))), empty());
    }

    public void testSubqueryKeepWildcardInOneBranchStampsBothBranchesWithDifferentPatterns() {
        LogicalPlan plan = test().statement(setUnmappedLoadAll("""
            FROM (FROM test | KEEP first_name*), (FROM test | WHERE emp_no > 0)
            """));
        assertThat(CollectionUtils.collect(plan.output(), UnmappedFieldsAttribute.class), hasSize(1));
        UnionAll union = EsqlTestUtils.singleValue(plan.collect(UnionAll.class));
        UnmappedFieldsPattern keepBranch = unmappedFieldsPattern(
            EsqlTestUtils.singleValue(union.children().get(0).collect(EsRelation.class))
        );
        UnmappedFieldsPattern whereBranch = unmappedFieldsPattern(
            EsqlTestUtils.singleValue(union.children().get(1).collect(EsRelation.class))
        );
        assertKept(keepBranch, "first_name_suffix");
        assertNotKept(keepBranch, "unmapped_extra");
        assertKept(whereBranch, "first_name_suffix", "unmapped_extra");
        assertKept(unmappedFieldsPattern(plan), "unmapped_extra");
    }

    public void testInSubqueryRightKeepWildcardOmitsUnmappedFieldsAttribute() {
        LogicalPlan plan = test().statement(setUnmappedLoadAll("""
            FROM test | WHERE emp_no IN (FROM test | KEEP emp_no*)
            """));
        assertInSubqueryRightHasNoUnmappedFields(plan);
    }

    public void testInSubqueryRightInsertedProjectOmitsUnmappedFieldsAttribute() {
        var ids = new EsIndex(
            "ids",
            Map.of("id", keywordField("id")),
            Map.of("ids", new IndexProperties(IndexMode.STANDARD, 0)),
            Map.of(),
            Map.of()
        );
        LogicalPlan plan = analyzer().addIndex(ids).statement(setUnmappedLoadAll("FROM ids | WHERE id IN (FROM ids | LIMIT 1)"));
        assertInSubqueryRightHasNoUnmappedFields(plan);
    }

    // A view is a boundary the setting does not reach into: no relation inside it is annotated, and where the query could have
    // had _unmapped_fields, the view's output carries a null one instead.

    public void testViewAloneIsNotAnnotatedAndHasNoUnmappedFieldsColumn() {
        LogicalPlan plan = test().addView("v", "FROM test").statement(setUnmappedLoadAll("FROM v"));
        assertThat(relationsWithUnmappedFields(plan), empty());
        assertThat(Expressions.names(plan.output()), not(hasItem(UnmappedFieldsAttribute.ATTRIBUTE_NAME)));
    }

    public void testViewsOnlyHaveNoUnmappedFieldsColumn() {
        LogicalPlan plan = test().addView("v", "FROM test")
            .addView("w", "FROM test | KEEP emp_no")
            .statement(setUnmappedLoadAll("FROM v, w"));
        assertThat(relationsWithUnmappedFields(plan), empty());
        assertThat(Expressions.names(plan.output()), not(hasItem(UnmappedFieldsAttribute.ATTRIBUTE_NAME)));
    }

    public void testStatsSubqueryAndViewHaveNoUnmappedFieldsColumn() {
        LogicalPlan plan = test().addView("v", "FROM test")
            .statement(setUnmappedLoadAll("FROM (FROM test | STATS c = COUNT(*)), (FROM v)"));
        assertThat(relationsWithUnmappedFields(plan), empty());
        assertThat(Expressions.names(plan.output()), not(hasItem(UnmappedFieldsAttribute.ATTRIBUTE_NAME)));
    }

    public void testViewBesideIndexOnlyAnnotatesTheIndex() {
        LogicalPlan plan = test().addView("v", "FROM test").statement(setUnmappedLoadAll("FROM test, v"));
        assertOnlyIndexBranchesAnnotated(plan);
        assertThat(plan.collect(EsRelation.class), hasSize(2));
        UnmappedFieldsPattern pattern = unmappedFieldsPattern(EsqlTestUtils.singleValue(relationsWithUnmappedFields(plan)));
        assertKept(pattern, "unmapped_extra");
        assertNotKept(pattern, excl());
    }

    public void testNestedViewBesideIndexOnlyAnnotatesTheIndex() {
        LogicalPlan plan = test().addView("inner", "FROM test")
            .addView("outer", "FROM inner")
            .statement(setUnmappedLoadAll("FROM test, outer"));
        assertOnlyIndexBranchesAnnotated(plan);
    }

    public void testBodyOfViewDoesNotRestrictTheIndexBranch() {
        // KEEP, DROP and STATS written inside the view's body only shape the view's own output.
        for (String body : List.of("FROM test | KEEP emp_no", "FROM test | KEEP first_name*", "FROM test | DROP salary")) {
            LogicalPlan plan = test().addView("v", body).statement(setUnmappedLoadAll("FROM test, v"));
            UnmappedFieldsPattern pattern = unmappedFieldsPattern(EsqlTestUtils.singleValue(relationsWithUnmappedFields(plan)));
            assertKept(pattern, "unmapped_extra", "first_name_suffix");
            assertNotKept(pattern, excl());
        }
        LogicalPlan stats = test().addView("v", "FROM test | STATS c = COUNT(*)").statement(setUnmappedLoadAll("FROM test, v"));
        assertThat(relationsWithUnmappedFields(stats), hasSize(1));
    }

    public void testKeepWildcardOutsideViewRestrictsTheIndexBranchAndNullsTheView() {
        LogicalPlan plan = test().addView("v", "FROM test").statement(setUnmappedLoadAll("FROM test, v | KEEP first_name*"));
        UnmappedFieldsPattern pattern = unmappedFieldsPattern(EsqlTestUtils.singleValue(relationsWithUnmappedFields(plan)));
        assertKept(pattern, "first_name_suffix", "first_name.sub", "first_name.sub.deeper");
        assertNotKept(pattern, excl());
        assertNotKept(pattern, "unmapped_extra", "salary_bonus");
        assertThat(Expressions.names(plan.output()), hasItem(UnmappedFieldsAttribute.ATTRIBUTE_NAME));
    }

    public void testDropOutsideViewRestrictsTheIndexBranch() {
        LogicalPlan plan = test().addView("v", "FROM test").statement(setUnmappedLoadAll("FROM test, v | DROP unmapped_extra"));
        UnmappedFieldsPattern pattern = unmappedFieldsPattern(EsqlTestUtils.singleValue(relationsWithUnmappedFields(plan)));
        assertKept(pattern, "first_name_suffix");
        assertNotKept(pattern, excl("unmapped_extra"));
        assertKeptAnyOtherName(pattern, excl("unmapped_extra"));
    }

    public void testExactKeepOrStatsOutsideViewLeavesNothingToReturn() {
        for (String query : List.of("FROM test, v | KEEP emp_no", "FROM test, v | STATS c = COUNT(*)")) {
            LogicalPlan plan = test().addView("v", "FROM test").statement(setUnmappedLoadAll(query));
            assertThat(query, relationsWithUnmappedFields(plan), empty());
            assertThat(query, Expressions.names(plan.output()), not(hasItem(UnmappedFieldsAttribute.ATTRIBUTE_NAME)));
        }
    }

    public void testColumnOfViewBodyNamedLikeUnmappedFieldsIsNotTheQuerys() {
        // Only the view's output counts as a column of the query; what its body had along the way does not.
        LogicalPlan plan = test().addView("v", "FROM test | EVAL _unmapped_fields = 1 | KEEP emp_no")
            .statement(setUnmappedLoadAll("FROM test, v"));
        assertThat(relationsWithUnmappedFields(plan), hasSize(1));
    }

    public void testColumnOfViewNamedLikeUnmappedFieldsIsTheQuerys() {
        LogicalPlan plan = test().addView("v", "FROM test | EVAL _unmapped_fields = 1 | KEEP emp_no, _unmapped_fields")
            .statement(setUnmappedLoadAll("FROM test, v"));
        assertThat(relationsWithUnmappedFields(plan), empty());
        assertWarnings(DetermineUnmappedFieldsToKeep.UNMAPPED_FIELDS_NAME_TAKEN_WARNING);
    }

    public void testViewInsideUserSubqueryBesideIndexKeepsTheNullColumn() {
        LogicalPlan plan = test().addView("v", "FROM test")
            .statement(setUnmappedLoadAll("FROM test, (FROM v | WHERE emp_no > 1), (FROM v | WHERE emp_no < 0)"));
        assertThat(relationsWithUnmappedFields(plan), hasSize(1));
        assertThat(Expressions.names(plan.output()), hasItem(UnmappedFieldsAttribute.ATTRIBUTE_NAME));
    }

    /**
     * A query that has a column named {@code _unmapped_fields} of its own, at any node, keeps it and gets no
     * {@link UnmappedFieldsAttribute}: union and fork branches line their columns up by name and {@code EVAL} shadows by name, so the
     * two could not live side by side. Here the query's column is a source field of that name, referenced explicitly.
     */
    public void testRenameOfSourceFieldNamedUnmappedFieldsOmitsUnmappedFieldsAttribute() {
        assertUnmappedFieldsNameTaken("FROM test | RENAME _unmapped_fields AS extras");
    }

    public void testScalarFunctionOnSourceFieldNamedUnmappedFieldsOmitsUnmappedFieldsAttribute() {
        assertUnmappedFieldsNameTaken("FROM test | EVAL len = LENGTH(_unmapped_fields)");
    }

    public void testWhereOnSourceFieldNamedUnmappedFieldsOmitsUnmappedFieldsAttribute() {
        assertUnmappedFieldsNameTaken("FROM test | WHERE _unmapped_fields == \"x\"");
    }

    /** The referenced field is demand-loaded as a column of the relation even though no later command outputs it. */
    public void testWhereOnSourceFieldNamedUnmappedFieldsThenKeepOmitsUnmappedFieldsAttribute() {
        assertUnmappedFieldsNameTaken("FROM test | WHERE _unmapped_fields == \"x\" | KEEP first_name*");
    }

    public void testEvalNamedUnmappedFieldsFromUnmappedFieldOmitsUnmappedFieldsAttribute() {
        assertUnmappedFieldsNameTaken("FROM test | EVAL _unmapped_fields = unmapped_extra::keyword");
    }

    /**
     * The query warns even when no unmapped field could have been returned anyway, e.g. after an exact KEEP or a STATS: a column
     * of that name is still not what its name suggests.
     */
    public void testNameTakenWarnsEvenWhenNoUnmappedFieldCouldBeReturnedAnyway() {
        assertUnmappedFieldsNameTaken("FROM test | WHERE _unmapped_fields == \"x\" | KEEP emp_no");
        assertUnmappedFieldsNameTaken("FROM test | EVAL _unmapped_fields = \"mine\" | KEEP emp_no, _unmapped_fields");
        assertUnmappedFieldsNameTaken("FROM test | EVAL _unmapped_fields = \"mine\" | STATS c = COUNT(*) BY _unmapped_fields");
    }

    /** A field of the lookup index named like the column of unmapped fields is a column of the query, and wins. */
    public void testLookupJoinFieldNamedUnmappedFieldsOmitsUnmappedFieldsAttribute() {
        var plan = test().addLookupIndex("colliding_lookup", lookupIndexWithUnmappedFieldsField())
            .statement(setUnmappedLoadAll("FROM test | EVAL language_code = first_name | LOOKUP JOIN colliding_lookup ON language_code"));
        assertThat(CollectionUtils.collect(plan.output(), UnmappedFieldsAttribute.class), empty());
        assertThat(plan.collect(EsRelation.class).stream().flatMap(r -> unmappedFieldsAttributes(r).stream()).toList(), empty());
        assertWarnings(DetermineUnmappedFieldsToKeep.UNMAPPED_FIELDS_NAME_TAKEN_WARNING);
    }

    /** A field added by ENRICH named like the column of unmapped fields is a column of the query, and wins. */
    public void testEnrichFieldNamedUnmappedFieldsOmitsUnmappedFieldsAttribute() {
        var plan = test().addEnrichPolicy(
            Enrich.Mode.ANY,
            "colliding_policy",
            new ResolvedEnrichPolicy(
                "language_code",
                EnrichPolicy.MATCH_TYPE,
                List.of("_unmapped_fields"),
                Map.of("", "colliding_idx"),
                Map.of("language_code", keywordField("language_code"), "_unmapped_fields", keywordField("_unmapped_fields"))
            )
        ).statement(setUnmappedLoadAll("FROM test | EVAL language_code = first_name | ENRICH colliding_policy ON language_code"));
        assertThat(CollectionUtils.collect(plan.output(), UnmappedFieldsAttribute.class), empty());
        assertThat(plan.collect(EsRelation.class).stream().flatMap(r -> unmappedFieldsAttributes(r).stream()).toList(), empty());
        assertWarnings(DetermineUnmappedFieldsToKeep.UNMAPPED_FIELDS_NAME_TAKEN_WARNING);
    }

    public void testDedupDoesNotKeyOnUnmappedFieldsAttribute() {
        assertDedupKeys("FROM test | KEEP emp_no, first_name* | DEDUP", List.of("emp_no", "first_name"));
    }

    public void testDedupAfterSubqueriesDoesNotKeyOnUnmappedFieldsAttribute() {
        assertDedupKeys("FROM (FROM test), (FROM test) | KEEP emp_no, first_name* | DEDUP", List.of("emp_no", "first_name"));
    }

    public void testDedupAfterForkDoesNotKeyOnUnmappedFieldsAttribute() {
        assertDedupKeys(
            "FROM test | FORK (WHERE emp_no > 0) (WHERE emp_no < 0) | KEEP emp_no, first_name*, _fork | DEDUP",
            List.of("emp_no", "first_name", "_fork")
        );
    }

    public void testDedupOverOnlyUnmappedFieldsAttributeKeepsOneRow() {
        LogicalPlan plan = test().statement(setUnmappedLoadAll("FROM test | KEEP unmapped* | DEDUP"));
        Dedup dedup = EsqlTestUtils.singleValue(plan.collect(Dedup.class));
        assertThat(unmappedFieldsAttributes(dedup), hasSize(1));
        as(dedup.surrogate(), Limit.class);
    }

    public void testDedupKeysOnSourceFieldNamedUnmappedFields() {
        LogicalPlan plan = test().statement(setUnmappedLoadAll("FROM test | WHERE _unmapped_fields IS NOT NULL | DEDUP"));
        Dedup dedup = EsqlTestUtils.singleValue(plan.collect(Dedup.class));
        LimitBy limitBy = as(dedup.surrogate(), LimitBy.class);
        assertThat(Expressions.names(limitBy.groupings()), hasItem(UnmappedFieldsAttribute.ATTRIBUTE_NAME));
        assertThat(CollectionUtils.collect(limitBy.groupings(), UnmappedFieldsAttribute.class), empty());
        assertWarnings(DetermineUnmappedFieldsToKeep.UNMAPPED_FIELDS_NAME_TAKEN_WARNING);
    }

    public void testSubqueryBranchNamingUnmappedFieldsOmitsUnmappedFieldsAttribute() {
        assertUnmappedFieldsNameTaken("FROM test, (FROM test | EVAL _unmapped_fields = \"mine\")");
    }

    /**
     * The collision does not have to reach the output of the query: the {@code EVAL} shadows the attribute by name where it sits,
     * so the attribute could not survive it, and it is not added.
     */
    public void testNameTakenOnlyInTheMiddleOfThePlanOmitsUnmappedFieldsAttribute() {
        assertUnmappedFieldsNameTaken("FROM test | EVAL _unmapped_fields = \"mine\" | DROP _unmapped_fields");
    }

    /**
     * Conservative: the collision is local to a branch that keeps only {@code emp_no}, so the other branch could still return its
     * unmapped fields. The check looks at every node of the plan, so none is returned and the query warns.
     */
    public void testNameTakenInsideASubqueryBranchOmitsUnmappedFieldsAttribute() {
        assertUnmappedFieldsNameTaken("FROM test, (FROM test | EVAL _unmapped_fields = \"mine\" | KEEP emp_no)");
    }

    /**
     * Conservative, like {@link #testNameTakenInsideASubqueryBranchOmitsUnmappedFieldsAttribute}: the right side of an {@code IN} is
     * planned on its own and never returns unmapped fields, yet a collision in it also takes the column of unmapped fields from the
     * main query.
     */
    public void testNameTakenInTheRightSideOfInOmitsUnmappedFieldsAttribute() {
        assertUnmappedFieldsNameTaken("FROM test | WHERE emp_no IN (FROM test | EVAL _unmapped_fields = \"mine\" | KEEP emp_no)");
    }

    public void testEvalNamedUnmappedFieldsOmitsUnmappedFieldsAttribute() {
        assertUnmappedFieldsNameTaken("FROM test | EVAL _unmapped_fields = \"mine\"");
    }

    public void testEvalNamedUnmappedFieldsThenDropOmitsUnmappedFieldsAttribute() {
        assertUnmappedFieldsNameTaken("FROM test | EVAL _unmapped_fields = \"mine\" | DROP _unmapped_fields");
    }

    public void testRenameToUnmappedFieldsOmitsUnmappedFieldsAttribute() {
        assertUnmappedFieldsNameTaken("FROM test | RENAME last_name AS _unmapped_fields");
    }

    public void testForkBranchNamingUnmappedFieldsOmitsUnmappedFieldsAttribute() {
        assertUnmappedFieldsNameTaken("FROM test | FORK (EVAL _unmapped_fields = \"mine\") (WHERE true)");
    }

    public void testForkSurfacesUnmappedFieldsAttribute() {
        LogicalPlan plan = test().statement(setUnmappedLoadAll("FROM test | FORK (WHERE true) (WHERE true)"));
        assertThat(unmappedFieldsAttributes(plan), hasSize(1));
        for (EsRelation relation : plan.collect(EsRelation.class)) {
            UnmappedFieldsPattern pattern = unmappedFieldsPattern(relation);
            assertKept(pattern, "unmapped_extra");
            assertNotKept(pattern, excl("_fork"));
        }
    }

    public void testForkMentionExcludesNamedFieldFromBothBranches() {
        LogicalPlan plan = test().statement(setUnmappedLoadAll("FROM test | FORK (WHERE unmapped_extra == \"x\") (WHERE emp_no > 0)"));
        for (EsRelation relation : plan.collect(EsRelation.class)) {
            UnmappedFieldsPattern pattern = unmappedFieldsPattern(relation);
            assertNotKept(pattern, "unmapped_extra");
            assertKept(pattern, "first_name_suffix");
        }
    }

    public void testForkDropInEveryBranchExcludesDroppedKeepsOthers() {
        LogicalPlan plan = test().statement(setUnmappedLoadAll("FROM test | FORK (DROP unmapped_extra) (DROP unmapped_extra)"));
        for (EsRelation relation : plan.collect(EsRelation.class)) {
            UnmappedFieldsPattern pattern = unmappedFieldsPattern(relation);
            assertNotKept(pattern, "unmapped_extra");
            assertKept(pattern, "first_name_suffix");
        }
    }

    public void testForkDifferentFieldsMentionedExcludeBothKeepOthers() {
        LogicalPlan plan = test().statement(
            setUnmappedLoadAll("FROM test | FORK (WHERE unmapped_extra == \"x\") (WHERE first_name_suffix == \"y\")")
        );
        for (EsRelation relation : plan.collect(EsRelation.class)) {
            UnmappedFieldsPattern pattern = unmappedFieldsPattern(relation);
            assertNotKept(pattern, "unmapped_extra", "first_name_suffix");
            assertKept(pattern, "salary_bonus");
        }
    }

    public void testForkKeepInOneBranchStillSurfacesUnmappedFieldsAttribute() {
        LogicalPlan plan = test().statement(setUnmappedLoadAll("FROM test | FORK (KEEP emp_no, first_name) (WHERE emp_no > 0)"));
        assertThat(unmappedFieldsAttributes(plan), hasSize(1));
        int stamped = 0;
        for (EsRelation relation : plan.collect(EsRelation.class)) {
            if (unmappedFieldsAttributes(relation).isEmpty() == false) {
                stamped++;
                assertKept(unmappedFieldsPattern(relation), "unmapped_extra");
            }
        }
        assertThat(stamped, equalTo(1));
    }

    public void testForkStatsInOneBranchStillSurfacesUnmappedFieldsAttribute() {
        LogicalPlan plan = test().statement(setUnmappedLoadAll("FROM test | FORK (STATS c = COUNT(*)) (WHERE emp_no > 0)"));
        assertThat(unmappedFieldsAttributes(plan), hasSize(1));
        int stamped = 0;
        for (EsRelation relation : plan.collect(EsRelation.class)) {
            if (unmappedFieldsAttributes(relation).isEmpty() == false) {
                stamped++;
                assertKept(unmappedFieldsPattern(relation), "unmapped_extra");
            }
        }
        assertThat(stamped, equalTo(1));
    }

    public void testForkKeepWildcardInOneBranchStampsBothBranchesWithDifferentPatterns() {
        LogicalPlan plan = test().statement(setUnmappedLoadAll("FROM test | FORK (KEEP first_name*) (WHERE emp_no > 0)"));
        int keptExtra = 0;
        int droppedExtra = 0;
        for (EsRelation relation : plan.collect(EsRelation.class)) {
            UnmappedFieldsPattern pattern = unmappedFieldsPattern(relation);
            assertKept(pattern, "first_name_suffix");
            if (pattern.matches("unmapped_extra")) {
                keptExtra++;
            } else {
                droppedExtra++;
            }
        }
        assertThat(keptExtra, equalTo(1));
        assertThat(droppedExtra, equalTo(1));
        // Coordinator expansion filters every branch's keys through Fork's pattern, so the union must still
        // keep extras the WHERE sibling loaded even when the KEEP branch is listed first.
        assertKept(unmappedFieldsPattern(plan), "unmapped_extra");
    }

    public void testInlineStatsThenForkStampsBothBranchesAndExcludesAggAlias() {
        LogicalPlan plan = test().statement(
            setUnmappedLoadAll("FROM test | INLINE STATS c = COUNT(*) | FORK (WHERE emp_no > 0) (WHERE emp_no > 0)")
        );
        assertThat(unmappedFieldsAttributes(plan), hasSize(1));
        List<EsRelation> relations = plan.collect(EsRelation.class);
        assertThat(relations, hasSize(2));
        for (EsRelation relation : relations) {
            UnmappedFieldsPattern pattern = unmappedFieldsPattern(relation);
            assertKept(pattern, "unmapped_extra");
            assertNotKept(pattern, "c");
        }
    }

    // -----------------------------------------------------------------------
    // LOOKUP JOIN: the left side's KEEP/DROP constraints must survive the join
    // -----------------------------------------------------------------------

    public void testKeepWildcardBeforeLookupJoinIsRespected() {
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

    /** A DROP of a plain name before the join still restricts — the dropped name must stay excluded across the join. */
    public void testDropBeforeLookupJoinIsRespected() {
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
     * Like {@link #testDropBeforeLookupJoinIsRespected}, but the DROP uses a wildcard: every unmapped name matching it stays
     * excluded across the join, while non-matching names still expand. No {@code assertKeptAnyOtherName} here — a random name
     * could match the wildcard exclude.
     */
    public void testDropWildcardBeforeLookupJoinIsRespected() {
        UnmappedFieldsPattern pattern = patternForJoin(
            "FROM test | DROP first_name* | EVAL language_code = languages | LOOKUP JOIN languages_lookup ON language_code",
            test().addLanguagesLookup()
        );
        // names matching the dropped wildcard stay excluded across the join
        assertNotKept(pattern, "first_name_suffix", "first_name.sub");
        // everything else still expands
        assertKept(pattern, "unmapped_extra", "first_grade", "salary_bonus");
        // lookup index fields (language_code, language_name) and test-mapped fields are excluded
        assertNotKept(pattern, excl("language_code", "language_name"));
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

    public void testKeepWildcardBeforeEnrichIsRespected() {
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
     * Automatically skips when {@code OPTIONAL_FIELDS_LOAD_ALL_V2} is disabled.
     */
    private static UnmappedFieldsPattern patternForJoin(String query, TestAnalyzer analyzer) {
        assumeTrue("Requires OPTIONAL_FIELDS_LOAD_ALL_V2", EsqlCapabilities.Cap.OPTIONAL_FIELDS_LOAD_ALL_V2.isEnabled());
        LogicalPlan plan = analyzer.statement(setUnmappedLoadAll(query));
        EsRelation primary = plan.collect(EsRelation.class)
            .stream()
            .filter(r -> r.indexMode() != IndexMode.LOOKUP)
            .findFirst()
            .orElseThrow(() -> new AssertionError("No non-LOOKUP EsRelation found"));
        return unmappedFieldsPattern(primary);
    }

    /**
     * Like {@link #patternFor(String, TestAnalyzer)}, but for queries using ENRICH.
     * Automatically skips when {@code OPTIONAL_FIELDS_LOAD_ALL_V2} is disabled.
     */
    private static UnmappedFieldsPattern patternForEnrich(String query, TestAnalyzer analyzer) {
        assumeTrue("Requires OPTIONAL_FIELDS_LOAD_ALL_V2", EsqlCapabilities.Cap.OPTIONAL_FIELDS_LOAD_ALL_V2.isEnabled());
        return patternFor(query, analyzer);
    }

    /** Like {@link #patternFor(String)}, but accepts a pre-configured analyzer (e.g. one with extra enrich policies). */
    private static UnmappedFieldsPattern patternFor(String query, TestAnalyzer analyzer) {
        return patternOf(analyzer.statement(setUnmappedLoadAll(query)));
    }

    private static void assertInSubqueryRightHasNoUnmappedFields(LogicalPlan plan) {
        AbstractSubqueryJoin join = EsqlTestUtils.singleValue(plan.collect(AbstractSubqueryJoin.class));
        assertThat(
            CollectionUtils.collect(
                EsqlTestUtils.singleValue(join.right().collect(EsRelation.class)).output(),
                UnmappedFieldsAttribute.class
            ),
            empty()
        );
        assertThat(Expressions.names(join.right().output()), not(hasItem(UnmappedFieldsAttribute.ATTRIBUTE_NAME)));
    }

    private static void assertKept(UnmappedFieldsPattern pattern, String... names) {
        for (String name : names) {
            assertThat("expected [" + name + "] to be kept", pattern.matches(name), is(true));
        }
    }

    private void assertNoUnmappedFieldsAttribute(String query) {
        LogicalPlan plan = test().statement(setUnmappedLoadAll(query));
        assertThat(CollectionUtils.collect(plan.output(), UnmappedFieldsAttribute.class), empty());
        for (EsRelation relation : plan.collect(EsRelation.class)) {
            assertThat("expected no UnmappedFieldsAttribute on " + relation, unmappedFieldsAttributes(relation), empty());
        }
    }

    /** The query has a column named like the one of unmapped fields: it wins, there is no such attribute, and a warning says so. */
    private void assertUnmappedFieldsNameTaken(String query) {
        assertNoUnmappedFieldsAttribute(query);
        assertWarnings(DetermineUnmappedFieldsToKeep.UNMAPPED_FIELDS_NAME_TAKEN_WARNING);
    }

    /** A lookup index with a join key and a field named like the column of unmapped fields. */
    private static IndexResolution lookupIndexWithUnmappedFieldsField() {
        return IndexResolution.valid(
            new EsIndex(
                "colliding_lookup",
                Map.of("language_code", keywordField("language_code"), "_unmapped_fields", keywordField("_unmapped_fields")),
                Map.of("colliding_lookup", new IndexProperties(IndexMode.LOOKUP, 0)),
                Map.of(),
                Map.of()
            )
        );
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

    /** The relations that carry an {@link UnmappedFieldsAttribute}: the ones the shard reads unmapped fields for. */
    private static List<EsRelation> relationsWithUnmappedFields(LogicalPlan plan) {
        return plan.collect(EsRelation.class)
            .stream()
            .filter(relation -> CollectionUtils.collect(relation.output(), UnmappedFieldsAttribute.class).isEmpty() == false)
            .toList();
    }

    /**
     * The output has an {@link UnmappedFieldsAttribute}, the relations in view branches are not annotated, and the ones in the other
     * branches are.
     */
    private static void assertOnlyIndexBranchesAnnotated(LogicalPlan plan) {
        assertThat(CollectionUtils.collect(plan.output(), UnmappedFieldsAttribute.class), hasSize(1));
        BranchPlan branches = EsqlTestUtils.singleValue(plan.collect(BranchPlan.class));
        for (int i = 0; i < branches.children().size(); i++) {
            List<EsRelation> annotated = relationsWithUnmappedFields(branches.children().get(i));
            if (branches.isViewBranch(i)) {
                assertThat("view branch [" + i + "]", annotated, empty());
            } else {
                assertThat("index branch [" + i + "]", annotated, hasSize(1));
            }
        }
    }

    private static UnmappedFieldsPattern patternOf(LogicalPlan plan) {
        return unmappedFieldsPattern(EsqlTestUtils.singleValue(plan.collect(EsRelation.class)));
    }

    private static UnmappedFieldsPattern unmappedFieldsPattern(LogicalPlan plan) {
        return EsqlTestUtils.singleValue(unmappedFieldsAttributes(plan)).pattern();
    }

    private void assertDedupKeys(String query, List<String> keys) {
        LogicalPlan plan = test().statement(setUnmappedLoadAll(query));
        Dedup dedup = EsqlTestUtils.singleValue(plan.collect(Dedup.class));
        assertThat(query, unmappedFieldsAttributes(dedup), hasSize(1));
        LimitBy limitBy = as(dedup.surrogate(), LimitBy.class);
        assertThat(query, Expressions.names(limitBy.groupings()), equalTo(keys));
    }

    private static List<UnmappedFieldsAttribute> unmappedFieldsAttributes(LogicalPlan plan) {
        return CollectionUtils.collect(plan.output(), UnmappedFieldsAttribute.class);
    }
}
