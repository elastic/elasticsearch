/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.expression.function.fulltext.MatchOperator;
import org.elasticsearch.xpack.esql.expression.function.fulltext.MatchPhrase;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToText;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.LogicalVerifier;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.NestedExpand;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.asLimit;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerNestedExpandTests.nestedAnalyzer;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

/**
 * The logical rules that move other commands past {@code NESTED_EXPAND} or narrow it. The index is the one
 * {@link org.elasticsearch.xpack.esql.analysis.AnalyzerNestedExpandTests} describes: {@code ne} has {@code id} and a nested
 * {@code items} with {@code sku}, {@code qty} and the {@code geo_point} {@code loc}.
 */
public class NestedExpandOptimizerTests extends AbstractLogicalPlanOptimizerTests {
    /** Leaves out the {@code geo_point} child, which the expansion cannot read. */
    private static final String KEEP = " | KEEP id, items.sku, items.qty";

    public NestedExpandOptimizerTests(VersionMode versionMode) {
        super(versionMode);
    }

    @Before
    public void requireCapability() {
        assumeTrue("NESTED_EXPAND is snapshot-only", EsqlCapabilities.Cap.NESTED_EXPAND.isEnabled());
    }

    /**
     * A condition on parent columns holds for every row of a parent, so it filters the parents before they are expanded.
     * {@snippet lang="text" :
     * Project[[id, items.sku, items.qty]]
     * \_Limit[1000[INTEGER],true,false]
     *   \_NestedExpand[items]
     *     \_Limit[1000[INTEGER],false,true]
     *       \_Filter[id{f}#1 == "1"[KEYWORD]]
     *         \_EsRelation[ne]
     * }
     */
    public void testFilterOnParentColumnPushedBelow() {
        LogicalPlan plan = nestedPlan("FROM ne | NESTED_EXPAND items | WHERE id == \"1\"" + KEEP);
        var limit = asLimit(as(plan, Project.class).child(), 1000, true, false);
        var expand = as(limit.child(), NestedExpand.class);
        var local = asLimit(expand.child(), 1000, null, true);
        var filter = as(local.child(), Filter.class);
        assertThat(Expressions.name(as(filter.condition(), Equals.class).left()), equalTo("id"));
        as(filter.child(), EsRelation.class);
    }

    /**
     * A condition on a child column only exists after the expansion. The limit stays above the filter, so nothing copies
     * it below the expansion.
     * {@snippet lang="text" :
     * Project[[id, items.sku, items.qty]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Filter[items.qty{r}#1 > 1[INTEGER]]
     *     \_NestedExpand[items]
     *       \_EsRelation[ne]
     * }
     */
    public void testFilterOnChildColumnStaysAbove() {
        LogicalPlan plan = nestedPlan("FROM ne | NESTED_EXPAND items | WHERE items.qty > 1" + KEEP);
        var limit = asLimit(as(plan, Project.class).child(), 1000, false);
        var filter = as(limit.child(), Filter.class);
        assertThat(Expressions.name(as(filter.condition(), GreaterThan.class).left()), equalTo("items.qty"));
        var expand = as(filter.child(), NestedExpand.class);
        as(expand.child(), EsRelation.class);
    }

    /**
     * Each side of a conjunction goes where its columns are.
     * {@snippet lang="text" :
     * Project[[id, items.sku, items.qty]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Filter[items.qty{r}#1 > 1[INTEGER]]
     *     \_NestedExpand[items]
     *       \_Filter[id{f}#2 == "1"[KEYWORD]]
     *         \_EsRelation[ne]
     * }
     */
    public void testConjunctionSplitsAtTheExpansion() {
        LogicalPlan plan = nestedPlan("FROM ne | NESTED_EXPAND items | WHERE id == \"1\" AND items.qty > 1" + KEEP);
        var limit = asLimit(as(plan, Project.class).child(), 1000, false);
        var above = as(limit.child(), Filter.class);
        assertThat(Expressions.name(as(above.condition(), GreaterThan.class).left()), equalTo("items.qty"));
        var expand = as(above.child(), NestedExpand.class);
        var below = as(expand.child(), Filter.class);
        assertThat(Expressions.name(as(below.condition(), Equals.class).left()), equalTo("id"));
        as(below.child(), EsRelation.class);
    }

    /**
     * Each parent gives at least one row, so a limit on the expanded rows also bounds the parents. The copy is local: it stays
     * on the data node with the expansion, which cannot run on the coordinator.
     * {@snippet lang="text" :
     * Project[[id, items.sku, items.qty]]
     * \_Limit[5[INTEGER],true,false]
     *   \_NestedExpand[items]
     *     \_Limit[5[INTEGER],false,true]
     *       \_EsRelation[ne]
     * }
     */
    public void testLimitCopiedBelowAsLocal() {
        LogicalPlan plan = nestedPlan("FROM ne | NESTED_EXPAND items | LIMIT 5" + KEEP);
        var limit = asLimit(as(plan, Project.class).child(), 5, true, false);
        var expand = as(limit.child(), NestedExpand.class);
        var local = asLimit(expand.child(), 5, false, true);
        as(local.child(), EsRelation.class);
    }

    /**
     * A limit after the expansion counts expanded rows, so it does not merge into a smaller limit on the parents before it.
     * {@snippet lang="text" :
     * Project[[id, items.sku, items.qty]]
     * \_Limit[20[INTEGER],true,false]
     *   \_NestedExpand[items]
     *     \_Limit[1[INTEGER],false,false]
     *       \_EsRelation[ne]
     * }
     */
    public void testLimitAfterDoesNotMergeWithLimitBefore() {
        LogicalPlan plan = nestedPlan("FROM ne | LIMIT 1 | NESTED_EXPAND items | LIMIT 20" + KEEP);
        var limit = asLimit(as(plan, Project.class).child(), 20, true);
        var expand = as(limit.child(), NestedExpand.class);
        var before = asLimit(expand.child(), 1);
        as(before.child(), EsRelation.class);
    }

    public void testPrunesUnreadChildColumns() {
        LogicalPlan plan = nestedPlan("FROM ne | NESTED_EXPAND items | KEEP items.qty");
        var project = as(plan, Project.class);
        var expand = as(asLimit(project.child(), 1000).child(), NestedExpand.class);
        assertThat(names(expand.expanded()), contains("items.qty"));
        assertThat(expand.paths(), contains("qty"));
    }

    /** Without any child column the expansion still repeats each parent once per object, so it stays. */
    public void testPrunesAllChildColumns() {
        LogicalPlan plan = nestedPlan("FROM ne | NESTED_EXPAND items | KEEP id");
        var project = as(plan, Project.class);
        var expand = as(asLimit(project.child(), 1000).child(), NestedExpand.class);
        assertThat(expand.expanded(), empty());
        assertThat(expand.paths(), empty());
    }

    public void testChildOfAnyTypeIsAColumn() {
        LogicalPlan plan = nestedPlan("FROM ne | NESTED_EXPAND items | KEEP items.qty, items.loc");
        var project = as(plan, Project.class);
        var expand = as(asLimit(project.child(), 1000).child(), NestedExpand.class);
        assertThat(names(expand.expanded()), containsInAnyOrder("items.qty", "items.loc"));
    }

    /**
     * A Lucene query would run on the parent of each child, so a search of a child column is made per row: the column is
     * wrapped in {@code TO_TEXT} declaring the analyzer the field has, which turns the search into a runtime search.
     */
    public void testMatchOnKeywordChildSearchesPerRow() {
        Filter filter = filterAboveExpansion("FROM ne | NESTED_EXPAND items | WHERE MATCH(items.sku, \"a\")" + KEEP);
        Match match = as(filter.condition(), Match.class);
        assertThat(as(match.field(), ToText.class).valuesAnalyzer(), equalTo("keyword"));
        assertThat(match.isRuntimeSearch(), is(true));
    }

    public void testMatchOnTextChildSearchesPerRowWithTheMappedAnalyzer() {
        Filter filter = filterAboveExpansion("FROM ne_text | NESTED_EXPAND items | WHERE MATCH(items.note, \"a\") | KEEP id, items.note");
        Match match = as(filter.condition(), Match.class);
        ToText field = as(match.field(), ToText.class);
        assertThat(field.valuesAnalyzer(), equalTo("standard"));
        assertThat(Expressions.name(field.field()), equalTo("items.note"));
        assertThat(match.isRuntimeSearch(), is(true));
    }

    public void testMatchPhraseOnChildSearchesPerRow() {
        Filter filter = filterAboveExpansion("FROM ne_text | NESTED_EXPAND items | WHERE MATCH_PHRASE(items.note, \"a b\") | KEEP id");
        MatchPhrase match = as(filter.condition(), MatchPhrase.class);
        assertThat(as(match.field(), ToText.class).valuesAnalyzer(), equalTo("standard"));
        assertThat(match.isRuntimeSearch(), is(true));
    }

    public void testMatchOperatorOnChildSearchesPerRow() {
        Filter filter = filterAboveExpansion("FROM ne_text | NESTED_EXPAND items | WHERE items.note : \"a\" | KEEP id");
        MatchOperator match = as(filter.condition(), MatchOperator.class);
        assertThat(as(match.field(), ToText.class).valuesAnalyzer(), equalTo("standard"));
        assertThat(match.isRuntimeSearch(), is(true));
    }

    /** The rename is removed by the optimizer, which would turn the search of the alias into an index search of the child. */
    public void testMatchOnRenamedChildSearchesPerRow() {
        Filter filter = filterAboveExpansion(
            "FROM ne_text | NESTED_EXPAND items | RENAME items.note AS note | WHERE MATCH(note, \"a\") | KEEP id, note"
        );
        Match match = as(filter.condition(), Match.class);
        assertThat(as(match.field(), ToText.class).valuesAnalyzer(), equalTo("standard"));
        assertThat(match.isRuntimeSearch(), is(true));
    }

    public void testMatchOnChildInADisjunctionSearchesPerRow() {
        Filter filter = filterAboveExpansion("FROM ne | NESTED_EXPAND items | WHERE MATCH(items.sku, \"a\") OR id == \"1\"" + KEEP);
        List<Match> matches = new ArrayList<>();
        filter.condition().forEachDown(Match.class, matches::add);
        assertThat(matches, hasSize(1));
        assertThat(matches.getFirst().isRuntimeSearch(), is(true));
    }

    /** The parent document is the one the query runs on, so a search of a parent column stays an index search below the expansion. */
    public void testMatchOnParentColumnStaysAnIndexSearchBelowTheExpansion() {
        LogicalPlan plan = nestedPlan("FROM ne | NESTED_EXPAND items | WHERE MATCH(id, \"1\")" + KEEP);
        var limit = asLimit(as(plan, Project.class).child(), 1000, true, false);
        var expand = as(limit.child(), NestedExpand.class);
        var local = asLimit(expand.child(), 1000, null, true);
        Match match = as(as(local.child(), Filter.class).condition(), Match.class);
        assertThat(match.field(), instanceOf(FieldAttribute.class));
        assertThat(match.isRuntimeSearch(), is(false));
    }

    /** The check after optimization catches a search of a child that no rule made per row, which would silently match nothing. */
    public void testIndexSearchOfAChildLeftByTheOptimizerIsRejected() {
        LogicalPlan analyzed = analyze("FROM ne | NESTED_EXPAND items | WHERE MATCH(items.sku, \"a\")" + KEEP);
        Failures failures = LogicalVerifier.INSTANCE.verify(analyzed, analyzed.output());
        assertThat(failures.toString(), containsString("Full-text matching on expanded nested fields is not supported yet"));
    }

    /** The filter of a {@code STATS ... WHERE} moves below the aggregation, where it is a filter of the expanded rows. */
    public void testMatchOnChildInAStatsFilterSearchesPerRow() {
        LogicalPlan plan = nestedPlan("FROM ne | NESTED_EXPAND items | STATS c = COUNT(*) WHERE MATCH(items.sku, \"a\")");
        List<Match> matches = new ArrayList<>();
        plan.forEachExpressionDown(Match.class, matches::add);
        assertThat(matches, hasSize(1));
        assertThat(as(matches.getFirst().field(), ToText.class).valuesAnalyzer(), equalTo("keyword"));
        assertThat(matches.getFirst().isRuntimeSearch(), is(true));
    }

    private Filter filterAboveExpansion(String query) {
        LogicalPlan plan = nestedPlan(query);
        var limit = asLimit(as(plan, Project.class).child(), 1000, false);
        Filter filter = as(limit.child(), Filter.class);
        as(filter.child(), NestedExpand.class);
        return filter;
    }

    /** The filter on a child column keeps the limit from reaching the SORT, so nothing bounds it. */
    public void testUnboundedSortBefore() {
        assertThat(
            error("FROM ne | SORT id | NESTED_EXPAND items | WHERE items.qty > 1" + KEEP),
            containsString(
                "NESTED_EXPAND [NESTED_EXPAND items] cannot yet have an unbounded SORT [SORT id] before it: "
                    + "either move the SORT after it, or add a LIMIT after the SORT"
            )
        );
    }

    public void testLocalOptimizerKeepsAnOrdinaryChild() {
        LogicalPlan local = localPlan(nestedPlan("FROM ne_types | NESTED_EXPAND items | EVAL q = items.small + 1 | WHERE q > 2 | KEEP q"));
        assertThat(fieldNames(local), hasItem("items.small"));
    }

    public void testLocalOptimizerKeepsAChildTheExpansionConverts() {
        LogicalPlan local = localPlan(
            nestedPlan("FROM ne_types | NESTED_EXPAND items | EVAL p = TO_LONG(items.v) + 1 | WHERE p > 2 | KEEP p")
        );
        assertThat(fieldNames(local), hasItem("$$items.v$converted_to$long"));
    }

    /** A data node's stats count a field of a nested object as not extractable from its parent, so none exists there. */
    private static LogicalPlan localPlan(LogicalPlan plan) {
        var stats = new EsqlTestUtils.TestSearchStats() {
            @Override
            public boolean exists(FieldAttribute.FieldName field) {
                return field.string().startsWith("items.") == false && field.string().startsWith("$$items.") == false;
            }
        };
        return new LocalLogicalPlanOptimizer(new LocalLogicalOptimizerContext(EsqlTestUtils.TEST_CFG, FoldContext.small(), stats))
            .localOptimize(plan);
    }

    private static List<String> fieldNames(LogicalPlan plan) {
        List<String> names = new ArrayList<>();
        plan.forEachExpressionDown(FieldAttribute.class, field -> names.add(field.name()));
        return names;
    }

    private LogicalPlan nestedPlan(String query) {
        return optimize(analyze(query));
    }

    private String error(String query) {
        LogicalPlan analyzed = analyze(query);
        return expectThrows(VerificationException.class, () -> optimize(analyzed)).getMessage();
    }

    private LogicalPlan analyze(String query) {
        return nestedAnalyzer(minimumVersion.supports(NestedExpand.VERSION) ? minimumVersion : NestedExpand.VERSION).query(query);
    }

    private static List<String> names(List<Attribute> attributes) {
        return attributes.stream().map(Attribute::name).toList();
    }
}
