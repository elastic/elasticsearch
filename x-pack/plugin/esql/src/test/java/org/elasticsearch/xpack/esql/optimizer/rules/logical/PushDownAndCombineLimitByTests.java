/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LimitBy;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.junit.BeforeClass;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

public class PushDownAndCombineLimitByTests extends AbstractLogicalPlanOptimizerTests {

    @BeforeClass
    public static void checkLimitByCapability() {}

    /**
     * <pre>{@code
     * Limit[1000[INTEGER],false,false]
     * \_LimitBy[1[INTEGER],[emp_no{f}#6],false]
     *   \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     * }</pre>
     */
    public void testLimitByPruneIdenticalLimits() {
        var plan = plan("""
            FROM test
            | LIMIT 1 BY emp_no
            | LIMIT 2 BY emp_no
            | LIMIT 1 BY emp_no
            """);

        var defaultLimit = as(plan, Limit.class);
        assertThat(((Literal) defaultLimit.limit()).value(), equalTo(1000));
        var limit = as(defaultLimit.child(), LimitBy.class);
        assertThat(((Literal) limit.limitPerGroup()).value(), equalTo(1));
        assertThat(Expressions.names(limit.groupings()), contains("emp_no"));
    }

    /**
     * <pre>{@code
     * Limit[1000[INTEGER],false,false]
     * \_LimitBy[1[INTEGER],[first_name{f}#6],false]
     *   \_LimitBy[1[INTEGER],[emp_no{f}#5],false]
     *     \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, ge..]
     * }</pre>
     */
    public void testLimitByKeepDifferentGroupings() {
        var plan = plan("""
            FROM test
            | LIMIT 1 BY emp_no
            | LIMIT 1 BY first_name
            """);

        var defaultLimit = as(plan, Limit.class);
        assertThat(((Literal) defaultLimit.limit()).value(), equalTo(1000));
        var limit1 = as(defaultLimit.child(), LimitBy.class);
        assertThat(((Literal) limit1.limitPerGroup()).value(), equalTo(1));
        assertThat(limit1.groupings().size(), equalTo(1));
        assertThat(Expressions.names(limit1.groupings()), contains("first_name"));
        var limit2 = as(limit1.child(), LimitBy.class);
        assertThat(((Literal) limit2.limitPerGroup()).value(), equalTo(1));
        assertThat(limit2.groupings().size(), equalTo(1));
        assertThat(Expressions.names(limit2.groupings()), contains("emp_no"));
    }

    /**
     * <pre>{@code
     * Limit[2[INTEGER],[],false,false]
     * \_Limit[2[INTEGER],[emp_no{f}#5],false,false]
     *   \_Limit[2[INTEGER],[],false,false]
     *     \_Limit[1[INTEGER],[emp_no{f}#5],false,false]
     *       \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, ge..]
     * }</pre>
     */
    public void testLimitByNotCombinedWhenSeparatedByPlainLimit() {
        var plan = plan("""
            FROM test
            | LIMIT 1 BY emp_no
            | LIMIT 2
            | LIMIT 2 BY emp_no
            """);

        var defaultLimit = as(plan, Limit.class);
        assertThat(((Literal) defaultLimit.limit()).value(), equalTo(2));
        var limit = as(defaultLimit.child(), LimitBy.class);
        assertThat(((Literal) limit.limitPerGroup()).value(), equalTo(2));
        assertThat(Expressions.names(limit.groupings()), contains("emp_no"));
        var limit2 = as(limit.child(), Limit.class);
        assertThat(((Literal) limit2.limit()).value(), equalTo(2));
        var limit3 = as(limit2.child(), LimitBy.class);
        assertThat(((Literal) limit3.limitPerGroup()).value(), equalTo(1));
        assertThat(Expressions.names(limit3.groupings()), contains("emp_no"));
    }

    /**
     * <pre>{@code
     * TopN[[Order[languages{f}#12,ASC,LAST]],10000[INTEGER],false]
     * \_Aggregate[[languages{f}#12],[COUNT(*[KEYWORD],true[BOOLEAN],PT0S[TIME_DURATION]) AS c, languages{f}#12]]
     *   \_LimitBy[2[INTEGER],[languages{f}#12],false,false]
     *     \_TopN[[Order[emp_no{f}#9,ASC,LAST]],1000[INTEGER],false]
     *       \_EsRelation[test][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, g..]
     * }</pre>
     */
    public void testLimitByNotCombinedWithTopN() {
        var plan = plan("""
            FROM test
            | SORT emp_no
            | LIMIT 1000
            | LIMIT 2 BY languages
            | STATS c = COUNT(*) BY languages
            | SORT languages ASC NULLS LAST
            """);

        var topN = as(plan, TopN.class);
        assertThat(topN.limit().fold(FoldContext.small()), equalTo(10000));
        assertThat(orderNames(topN), contains("languages"));
        var agg = as(topN.child(), Aggregate.class);
        assertThat(Expressions.names(agg.groupings()), contains("languages"));
        var limit = as(agg.child(), LimitBy.class);
        assertThat(((Literal) limit.limitPerGroup()).value(), equalTo(2));
        assertThat(Expressions.names(limit.groupings()), contains("languages"));
        var innerTopN = as(limit.child(), TopN.class);
        assertThat(innerTopN.limit().fold(FoldContext.small()), equalTo(1000));
        assertThat(orderNames(innerTopN), contains("emp_no"));
        as(innerTopN.child(), EsRelation.class);
    }

    /**
     * A grouped LIMIT (LIMIT BY) whose grouping references a field introduced by a local Enrich must not be
     * pushed below the Enrich, because the field would not exist there.
     * <pre>{@code
     * Limit[1000[INTEGER],false,false]
     * \_LimitBy[5[INTEGER],[language_name{f}#N],false]
     *   \_Enrich[ANY,languages_idx,first_name{f}#N,...]
     *     \_EsRelation[test][...]
     * }</pre>
     */
    public void testLimitByNotPushedBelowLocalEnrichWhenGroupingReferencesEnrichField() {
        var plan = plan("""
            FROM test
            | ENRICH languages_idx ON first_name
            | LIMIT 5 BY language_name
            """);

        var defaultLimit = as(plan, Limit.class);
        var limit = as(defaultLimit.child(), LimitBy.class);
        assertThat(((Literal) limit.limitPerGroup()).value(), equalTo(5));
        assertThat(Expressions.names(limit.groupings()), contains("language_name"));
        var enrich = as(limit.child(), Enrich.class);
        as(enrich.child(), EsRelation.class);
    }

    /**
     * A grouped LIMIT (LIMIT BY) whose grouping references a field introduced by a remote Enrich must not be
     * duplicated below the Enrich, because the field would not exist there.
     * <pre>{@code
     * Limit[1000[INTEGER],false,false]
     * \_LimitBy[5[INTEGER],[language_name{f}#N],false]
     *   \_Enrich[REMOTE,languages_remote,first_name{f}#N,...]
     *     \_EsRelation[test][...]
     * }</pre>
     */
    public void testLimitByDuplicatedBelowRemoteEnrichWhenGroupingReferencesEnrichField() {
        var plan = plan("""
            FROM test
            | ENRICH _remote:languages_remote ON first_name
            | LIMIT 5 BY language_name
            """);

        var defaultLimit = as(plan, Limit.class);
        var limit = as(defaultLimit.child(), LimitBy.class);
        assertThat(((Literal) limit.limitPerGroup()).value(), equalTo(5));
        assertThat(Expressions.names(limit.groupings()), contains("language_name"));
        var enrich = as(limit.child(), Enrich.class);
        as(enrich.child(), EsRelation.class);
    }

    /**
     * A grouped LIMIT (LIMIT BY) whose grouping references a field introduced by a remote Enrich must not be
     * duplicated below the Enrich, because the field would not exist there.
     * <pre>{@code
     * Limit[1000[INTEGER],false,false]
     * \_LimitBy[5[INTEGER],[language_name{f}#N],false]
     *   \_Enrich[REMOTE,languages_remote,first_name{f}#N,...]
     *     \_EsRelation[test][...]
     * }</pre>
     */
    public void testLimitByNotDuplicatedBelowRemoteEnrichWhenGroupingReferencesEnrichField() {
        var plan = plan("""
            FROM test
            | ENRICH _remote:languages_remote ON first_name
            | LIMIT 5 BY language_name
            """);

        var defaultLimit = as(plan, Limit.class);
        var limit = as(defaultLimit.child(), LimitBy.class);
        assertThat(((Literal) limit.limitPerGroup()).value(), equalTo(5));
        assertThat(Expressions.names(limit.groupings()), contains("language_name"));
        var enrich = as(limit.child(), Enrich.class);
        as(enrich.child(), EsRelation.class);
    }

    /**
     * A grouped LIMIT (LIMIT BY) whose grouping references only source fields should still be pushed below
     * a local Enrich, since the field is available in the Enrich's child.
     * <pre>{@code
     * Limit[1000[INTEGER],false,false]
     * \_LimitBy[5[INTEGER],[emp_no{f}#6, language_name{r}#20],false]
     *   \_Enrich[ANY,languages_idx[KEYWORD],first_name{f}#7,...]
     *     \_EsRelation[test][...]
     * }</pre>
     */
    public void testLimitByNotDuplicatedBelowLocalEnrichWhenSomeGroupingReferencesEnrichField() {
        var plan = plan("""
            FROM test
            | ENRICH languages_idx ON first_name
            | LIMIT 5 BY emp_no, language_name
            """);

        var defaultLimit = as(plan, Limit.class);
        var limit = as(defaultLimit.child(), LimitBy.class);
        assertThat(((Literal) limit.limitPerGroup()).value(), equalTo(5));
        assertThat(Expressions.names(limit.groupings()), contains("emp_no", "language_name"));
        var enrich = as(limit.child(), Enrich.class);
        as(enrich.child(), EsRelation.class);
    }

    /**
     * A grouped LIMIT (LIMIT BY) whose grouping references only source fields should still be pushed below
     * a local Enrich, since the field is available in the Enrich's child.
     * <pre>{@code
     * Enrich[ANY,languages_idx,first_name{f}#N,...]
     * \_Limit[1000[INTEGER],false,false]
     *   \_LimitBy[5[INTEGER],[emp_no{f}#N],false]
     *     \_EsRelation[test][...]
     * }</pre>
     */
    public void testLimitByPushedBelowLocalEnrichWhenGroupingOnSourceField() {
        var plan = plan("""
            FROM test
            | ENRICH languages_idx ON first_name
            | LIMIT 5 BY emp_no
            """);

        var enrich = as(plan, Enrich.class);
        var defaultLimit = as(enrich.child(), Limit.class);
        var limit = as(defaultLimit.child(), LimitBy.class);
        assertThat(((Literal) limit.limitPerGroup()).value(), equalTo(5));
        assertThat(Expressions.names(limit.groupings()), contains("emp_no"));
        as(limit.child(), EsRelation.class);
    }

    /**
     * A grouped LIMIT (LIMIT BY) above a Fork must not be pushed into the fork branches.
     */
    public void testLimitByNotPushedIntoForkBranches() {
        var plan = plan("""
            FROM test
            | FORK (WHERE emp_no > 100) (WHERE emp_no < 10)
            | LIMIT 5 BY emp_no
            """);

        var defaultLimit = as(plan, Limit.class);
        var limit = as(defaultLimit.child(), LimitBy.class);
        assertThat(((Literal) limit.limitPerGroup()).value(), equalTo(5));
        assertThat(Expressions.names(limit.groupings()), contains("emp_no"));
        var fork = as(limit.child(), Fork.class);
        for (var branch : fork.children()) {
            var project = as(branch, Project.class);
            var eval = as(project.child(), Eval.class);
            as(eval.child(), Filter.class);
        }
    }

    /**
     * MV_EXPAND can increase the number of rows, so we duplicate the LimitBy: keep the original above and add a copy below.
     * <pre>{@code
     * Limit[1000[INTEGER],false,false]
     * \_LimitBy[5,duplicated,[emp_no{f}#N]]
     *   \_MvExpand[first_name{f}#N]
     *     \_LimitBy[5,[emp_no{f}#N]]
     *       \_EsRelation[test][...]
     * }</pre>
     */
    public void testLimitByDuplicatedPastMvExpand() {
        var plan = plan("""
            FROM test
            | MV_EXPAND first_name
            | LIMIT 5 BY emp_no
            """);

        var defaultLimit = as(plan, Limit.class);
        var upperLimitBy = as(defaultLimit.child(), LimitBy.class);
        assertThat(((Literal) upperLimitBy.limitPerGroup()).value(), equalTo(5));
        assertThat(Expressions.names(upperLimitBy.groupings()), contains("emp_no"));
        assertTrue(upperLimitBy.duplicated());

        var mvExpand = as(upperLimitBy.child(), MvExpand.class);
        var lowerLimitBy = as(mvExpand.child(), LimitBy.class);
        assertThat(((Literal) lowerLimitBy.limitPerGroup()).value(), equalTo(5));
        assertThat(Expressions.names(lowerLimitBy.groupings()), contains("emp_no"));
        assertFalse(lowerLimitBy.duplicated());

        as(lowerLimitBy.child(), EsRelation.class);
    }

    private static List<String> orderNames(TopN topN) {
        return topN.order().stream().map(o -> as(o.child(), NamedExpression.class).name()).toList();
    }
}
