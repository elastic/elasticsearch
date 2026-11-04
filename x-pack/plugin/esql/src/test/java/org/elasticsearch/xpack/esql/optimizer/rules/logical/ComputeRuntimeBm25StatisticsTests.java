/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.expression.function.fulltext.FullTextFunction;
import org.elasticsearch.xpack.esql.expression.function.fulltext.RuntimeBm25Field;
import org.elasticsearch.xpack.esql.expression.function.fulltext.RuntimeBm25RowStats;
import org.elasticsearch.xpack.esql.expression.function.fulltext.RuntimeBm25Support;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

/**
 * Plans for {@code MATCH} with {@code similarity_function: bm25} on runtime text, after {@link ComputeRuntimeBm25Statistics}
 * and the rest of the logical optimizer.
 */
public class ComputeRuntimeBm25StatisticsTests extends AbstractLogicalPlanOptimizerTests {

    private static final String FROM = "from test metadata _score | eval t = to_text(concat(first_name, \" \", last_name))";
    private static final String BM25 = "{\"similarity_function\": \"bm25\"}";

    public ComputeRuntimeBm25StatisticsTests(VersionMode versionMode) {
        super(versionMode);
    }

    public void testStatisticsJoinShape() {
        LogicalPlan plan = bm25Plan(FROM + " | where emp_no > 10010 and match(t, \"Georgi Facello\", " + BM25 + ") | sort _score desc");

        InlineJoin join = single(statisticsJoins(plan));
        // One per-row statistics call per group: the row is analyzed once, whatever the number of terms.
        RuntimeBm25RowStats rowStats = single(rowStats(join.right()));
        assertThat(rowStats.termList(FoldContext.small()), contains(new BytesRef("facello"), new BytesRef("georgi")));
        // The other condition narrows the statistics rows; the search condition does not.
        assertThat(conditions(join.left()), hasItem("emp_no > 10010"));
        assertThat(searchFiltersBelow(join), empty());
        // The search is re-applied above the join, with its field wrapped in its statistics.
        RuntimeBm25Field field = single(wrappedFields(plan));
        assertThat(field.statistics(), hasSize(4));
        // The statistics columns never reach the user.
        assertThat(plan.output().stream().map(Attribute::name).filter(name -> name.startsWith("$$bm25")).toList(), empty());
    }

    /** CUJ 3: splitting, merging and reordering AND-ed filters gives the same statistics rows and terms. */
    public void testEquivalentFiltersGiveTheSameStatistics() {
        String condition = "emp_no > 10010";
        String search = "match(t, \"Georgi\", " + BM25 + ")";
        List<String> queries = List.of(
            FROM + " | where " + condition + " and " + search + " | sort _score desc",
            FROM + " | where " + search + " and " + condition + " | sort _score desc",
            FROM + " | where " + condition + " | where " + search + " | sort _score desc",
            FROM + " | where " + search + " | where " + condition + " | sort _score desc"
        );
        for (String query : queries) {
            InlineJoin join = single(statisticsJoins(bm25Plan(query)));
            assertThat(query, conditions(join.left()), hasItem(condition));
            assertThat(query, single(rowStats(join.right())).termList(FoldContext.small()), contains(new BytesRef("georgi")));
        }
    }

    public void testNoRewriteWithoutScore() {
        LogicalPlan plan = bm25Plan(
            "from test | eval t = to_text(concat(first_name, \" \", last_name)) | where match(t, \"Georgi\", " + BM25 + ")"
        );
        assertThat(statisticsJoins(plan), empty());
        assertThat(wrappedFields(plan), empty());
    }

    public void testNoRewriteWithBooleanSimilarity() {
        LogicalPlan plan = bm25Plan(FROM + " | where match(t, \"Georgi\", {\"similarity_function\": \"boolean\"}) | sort _score desc");
        assertThat(statisticsJoins(plan), empty());
        assertThat(rowStats(plan), empty());
    }

    public void testCallsOnOneFieldShareAGroup() {
        LogicalPlan plan = bm25Plan(
            FROM + " | where match(t, \"Georgi\", " + BM25 + ") and match(t, \"Facello\", " + BM25 + ") | sort _score desc"
        );
        RuntimeBm25RowStats rowStats = single(rowStats(plan));
        assertThat(rowStats.termList(FoldContext.small()), contains(new BytesRef("facello"), new BytesRef("georgi")));
        assertThat(wrappedFields(plan), hasSize(2));
    }

    public void testCallsOnTwoFieldsMakeTwoGroups() {
        LogicalPlan plan = bm25Plan(
            FROM
                + " | eval u = to_text(last_name) | where match(t, \"Georgi\", "
                + BM25
                + ") and match(u, \"Facello\", "
                + BM25
                + ") | sort _score desc"
        );
        assertThat(rowStats(plan), hasSize(2));
        assertThat(single(statisticsJoins(plan)).right().output(), hasSize(3 + 3));
    }

    /** A boolean runtime MATCH next to a BM25 one is re-applied with it and does not narrow the statistics rows. */
    public void testBooleanMatchIsLiftedNotCounted() {
        LogicalPlan plan = bm25Plan(
            FROM + " | where match(t, \"Georgi\", " + BM25 + ") and (match(t, \"Parto\") or emp_no > 10003) | sort _score desc"
        );
        InlineJoin join = single(statisticsJoins(plan));
        assertThat(searchFiltersBelow(join), empty());
        assertThat(single(wrappedFields(plan)).field().sourceText(), equalTo("t"));
    }

    /** Reordered conjuncts are re-applied in the same order, so their scores add up in the same order. */
    public void testLiftedConditionsHaveAFixedOrder() {
        String first = "match(t, \"Georgi\", " + BM25 + ")";
        String second = "match(t, \"Facello\", " + BM25 + ")";
        List<String> forward = liftedConditions(bm25Plan(FROM + " | where " + first + " and " + second + " | sort _score desc"));
        List<String> backward = liftedConditions(bm25Plan(FROM + " | where " + second + " and " + first + " | sort _score desc"));
        assertThat(forward, equalTo(backward));
    }

    /** A filter reading _score ends the stretch, so it still sees the BM25 scores. */
    public void testScoreFilterEndsTheStretch() {
        LogicalPlan plan = bm25Plan(FROM + " | where match(t, \"Georgi\", " + BM25 + ") | where _score > 0 | sort _score desc");
        assertThat(statisticsJoins(plan), hasSize(1));
        assertThat(wrappedFields(plan), hasSize(1));
    }

    /** An EVAL that redefines the matched column ends the stretch before it. */
    public void testRedefinedColumnEndsTheStretch() {
        LogicalPlan plan = bm25Plan(FROM + " | where match(t, \"Georgi\", " + BM25 + ") | eval t = \"other\" | sort _score desc");
        assertThat(statisticsJoins(plan), hasSize(1));
        assertThat(wrappedFields(plan), hasSize(1));
    }

    /** The filter still carries _score where it runs, so the planner scores it even if a later KEEP drops _score. */
    public void testKeepDroppingScoreStillPreparesTheScorer() {
        LogicalPlan plan = bm25Plan(FROM + " | where match(t, \"Georgi\", " + BM25 + ") | keep emp_no");
        assertThat(wrappedFields(plan), hasSize(1));
    }

    /**
     * A KEEP that drops a column a lower runtime MATCH reads bounds the stretch: that MATCH stays where it is, below the
     * statistics join, instead of being re-applied where its column no longer exists.
     */
    public void testColumnDroppedBetweenFiltersBoundsTheStretch() {
        LogicalPlan plan = bm25Plan(
            "from test metadata _score | eval t1 = to_text(first_name), t2 = to_text(last_name) | where match(t1, \"Georgi\") "
                + "| keep t2, _score | where match(t2, \"Facello\", "
                + BM25
                + ") | sort _score desc"
        );
        InlineJoin join = single(statisticsJoins(plan));
        assertThat(searchFiltersBelow(join), hasSize(1));
        assertThat(single(wrappedFields(plan)).field().sourceText(), equalTo("t2"));
    }

    /** Each call compiles, but together the calls of one group hold more terms than one Lucene query may. */
    public void testTooManyTermsInAGroup() {
        assumeTrue("BM25 needs the current transport version", versionMode == VersionMode.CURRENT);
        int max = IndexSearcher.getMaxClauseCount();
        int half = max / 2 + 1;
        String first = IntStream.range(0, half).mapToObj(i -> "w" + i).collect(Collectors.joining(" "));
        String second = IntStream.range(0, half).mapToObj(i -> "v" + i).collect(Collectors.joining(" "));
        InvalidArgumentException e = expectThrows(
            InvalidArgumentException.class,
            () -> plan(
                FROM
                    + " | where match(t, \""
                    + first
                    + "\", "
                    + BM25
                    + ") and match(t, \""
                    + second
                    + "\", "
                    + BM25
                    + ") | sort _score desc"
            )
        );
        assertThat(
            e.getMessage(),
            equalTo("the BM25 conditions on [t] analyze to [" + (2 * half) + "] distinct terms; at most [" + max + "] are allowed")
        );
    }

    /** One call with more terms than one Lucene query may hold gets the same 400. */
    public void testTooManyTermsInACall() {
        assumeTrue("BM25 needs the current transport version", versionMode == VersionMode.CURRENT);
        int max = IndexSearcher.getMaxClauseCount();
        String text = IntStream.rangeClosed(0, max).mapToObj(i -> "w" + i).collect(Collectors.joining(" "));
        InvalidArgumentException e = expectThrows(
            InvalidArgumentException.class,
            () -> plan(FROM + " | where match(t, \"" + text + "\", " + BM25 + ") | sort _score desc")
        );
        assertThat(
            e.getMessage(),
            equalTo("the BM25 conditions on [t] analyze to more than [" + max + "] terms; at most [" + max + "] are allowed")
        );
    }

    private LogicalPlan bm25Plan(String query) {
        assumeTrue("BM25 needs the current transport version", versionMode == VersionMode.CURRENT);
        return plan(query);
    }

    private static <T> T single(List<T> items) {
        assertThat(items, hasSize(1));
        return items.getFirst();
    }

    private static List<InlineJoin> statisticsJoins(LogicalPlan plan) {
        List<InlineJoin> joins = new ArrayList<>();
        plan.forEachDown(InlineJoin.class, join -> {
            if (RuntimeBm25Support.isStatisticsJoin(join)) {
                joins.add(join);
            }
        });
        return joins;
    }

    private static List<RuntimeBm25RowStats> rowStats(LogicalPlan plan) {
        List<RuntimeBm25RowStats> calls = new ArrayList<>();
        plan.forEachExpressionDown(RuntimeBm25RowStats.class, calls::add);
        return calls;
    }

    private static List<RuntimeBm25Field> wrappedFields(LogicalPlan plan) {
        List<RuntimeBm25Field> fields = new ArrayList<>();
        plan.forEachExpressionDown(RuntimeBm25Field.class, fields::add);
        return fields;
    }

    /** The source text of every conjunct of every filter in {@code plan}. */
    private static Set<String> conditions(LogicalPlan plan) {
        Set<String> conditions = new TreeSet<>();
        plan.forEachDown(Filter.class, filter -> Predicates.splitAnd(filter.condition()).forEach(c -> conditions.add(c.sourceText())));
        return conditions;
    }

    private static List<Filter> searchFiltersBelow(InlineJoin join) {
        List<Filter> filters = new ArrayList<>();
        join.left().forEachDown(Filter.class, filter -> {
            if (filter.condition().anyMatch(FullTextFunction.class::isInstance)) {
                filters.add(filter);
            }
        });
        return filters;
    }

    /** The conjuncts of the filter that holds the wrapped BM25 fields, in order. */
    private static List<String> liftedConditions(LogicalPlan plan) {
        List<String> conditions = new ArrayList<>();
        plan.forEachDown(Filter.class, filter -> {
            if (filter.condition().anyMatch(RuntimeBm25Field.class::isInstance)) {
                Predicates.splitAnd(filter.condition()).stream().map(Expression::sourceText).forEach(conditions::add);
            }
        });
        assertThat(conditions, not(empty()));
        return conditions;
    }
}
