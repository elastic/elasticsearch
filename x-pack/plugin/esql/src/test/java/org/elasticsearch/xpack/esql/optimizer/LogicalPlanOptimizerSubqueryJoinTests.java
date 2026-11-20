/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.parser.EsqlConfig;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.SubqueryJoin;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.session.Versioned;
import org.junit.Before;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Logical optimization of {@code LEFT OUTER JOIN} on a subquery. The session rejects the join before optimization until it can run,
 * so these tests optimize the analyzed plan directly.
 */
public class LogicalPlanOptimizerSubqueryJoinTests extends AbstractLogicalPlanOptimizerTests {

    private static final String JOIN_LANGUAGES = """
        FROM test
        | EVAL language_code = languages
        | LEFT OUTER JOIN (FROM languages) ON language_code
        """;

    public LogicalPlanOptimizerSubqueryJoinTests(VersionMode versionMode) {
        super(versionMode);
    }

    @Before
    public void checkGrammar() {
        assumeTrue("LEFT OUTER JOIN requires snapshot builds", new EsqlConfig(TEST_FUNCTION_REGISTRY).isDevVersion());
    }

    /**
     * A filter on a subquery column stays above the join: it must still reject the left rows that found no match. Pushing a copy
     * into the subquery as well, as for {@code LOOKUP JOIN}, would never settle: the subquery's own rules move the copy below its
     * projections, so the next pass would push it again.
     */
    public void testFilterOnSubqueryColumnStaysAboveJoin() {
        LogicalPlan plan = optimizedJoinPlan(JOIN_LANGUAGES + "| WHERE language_name == \"English\"");
        SubqueryJoin join = findJoin(plan);
        Filter filter = findParent(plan, join, Filter.class);
        assertThat(filter.condition().references().stream().map(Attribute::name).toList(), equalTo(List.of("language_name")));
        assertThat(join.right().collect(Filter.class::isInstance), hasSize(0));
    }

    public void testFilterOnLeftColumnIsPushedBelowJoin() {
        LogicalPlan plan = optimizedJoinPlan(JOIN_LANGUAGES + "| WHERE emp_no > 10");
        SubqueryJoin join = findJoin(plan);
        assertThat(plan.collect(p -> p instanceof Filter f && f.anyMatch(join::equals)), hasSize(0));
        assertThat(join.left().collect(Filter.class::isInstance), hasSize(1));
        assertThat(join.right().collect(Filter.class::isInstance), hasSize(0));
    }

    /**
     * A {@code LIMIT} after the join is copied to the left side only: limiting the subquery would drop matches.
     */
    public void testLimitIsNotPushedIntoSubquery() {
        SubqueryJoin join = findJoin(optimizedJoinPlan(JOIN_LANGUAGES + "| LIMIT 10"));
        assertThat(join.left().collect(Limit.class::isInstance), hasSize(1));
        assertThat(join.right().collect(Limit.class::isInstance), hasSize(0));
    }

    /**
     * Moving a {@code RENAME} past the join needs an {@code Eval} when the renamed column is shadowed by a subquery column. The
     * rebuilt join must still be a {@link SubqueryJoin}, not a plain {@link Join}.
     */
    public void testProjectMovedPastJoinKeepsSubqueryJoin() {
        LogicalPlan plan = optimizedJoinPlan("""
            FROM test
            | EVAL language_code = languages
            | RENAME first_name AS name
            | LEFT OUTER JOIN (FROM languages | EVAL first_name = language_name | KEEP language_code, first_name) ON language_code
            | KEEP emp_no, name, first_name
            """);
        assertThat(plan.collect(p -> p.getClass() == Join.class), hasSize(0));
        SubqueryJoin join = findJoin(plan);
        assertThat(join.left(), instanceOf(Eval.class));
        assertThat(join.rightOutputFields().stream().map(Attribute::name).toList(), equalTo(List.of("first_name")));
    }

    /**
     * The join reads its subquery into a hash table, so a {@code SORT} in the subquery is dropped rather than rejected as an
     * unbounded sort. A {@code SORT} with a {@code LIMIT} decides which rows the subquery returns, so it stays.
     */
    public void testSortInSubquery() {
        SubqueryJoin unbounded = findJoin(optimizedJoinPlan("""
            FROM test
            | EVAL language_code = languages
            | LEFT OUTER JOIN (FROM languages | SORT language_name) ON language_code
            """));
        assertThat(unbounded.right().collect(OrderBy.class::isInstance), hasSize(0));

        SubqueryJoin topN = findJoin(optimizedJoinPlan("""
            FROM test
            | EVAL language_code = languages
            | LEFT OUTER JOIN (FROM languages | SORT language_name | LIMIT 2) ON language_code
            """));
        assertThat(topN.right().collect(TopN.class::isInstance), hasSize(1));
    }

    /**
     * The join copies the subquery's columns with {@code Block#lookup}, which some block types do not implement. The check runs after
     * optimization, so a column that nothing reads after the join is pruned instead of rejected.
     */
    public void testPayloadTypesWithoutLookup() {
        String query = """
            FROM test
            | EVAL language_code = languages
            | LEFT OUTER JOIN (ROW language_code = 1, m = TO_AGGREGATE_METRIC_DOUBLE(1)) ON language_code
            """;
        VerificationException e = expectThrows(VerificationException.class, () -> optimizedJoinPlan(query));
        assertThat(
            e.getMessage(),
            containsString("JOIN subquery cannot return column [m] of type [AGGREGATE_METRIC_DOUBLE]; remove it with KEEP or DROP")
        );

        SubqueryJoin join = findJoin(optimizedJoinPlan(query + "| DROP m"));
        assertThat(join.right().output().stream().map(Attribute::name).toList(), equalTo(List.of("language_code")));
    }

    public void testMapperRejectsJoin() {
        LogicalPlan plan = optimizedJoinPlan(JOIN_LANGUAGES);
        VerificationException e = expectThrows(VerificationException.class, () -> new Mapper().map(new Versioned<>(plan, minimumVersion)));
        assertThat(e.getMessage(), containsString("line 3:3: JOIN on a subquery is not supported yet"));
    }

    private LogicalPlan optimizedJoinPlan(String query) {
        return optimize(analyzer().addEmployees("test").addLanguages().query(query));
    }

    private static SubqueryJoin findJoin(LogicalPlan plan) {
        List<LogicalPlan> joins = plan.collect(SubqueryJoin.class::isInstance);
        assertThat(joins, hasSize(1));
        return (SubqueryJoin) joins.getFirst();
    }

    /** The node directly above {@code child}, which must be of type {@code parentType}. */
    private static <T extends LogicalPlan> T findParent(LogicalPlan plan, LogicalPlan child, Class<T> parentType) {
        List<LogicalPlan> parents = plan.collect(p -> p.children().contains(child));
        assertThat(parents, hasSize(1));
        assertThat(parents.getFirst(), instanceOf(parentType));
        return parentType.cast(parents.getFirst());
    }
}
