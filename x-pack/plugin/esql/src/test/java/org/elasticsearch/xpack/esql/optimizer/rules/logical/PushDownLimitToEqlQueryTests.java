/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.eql.EqlQuery;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;
import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class PushDownLimitToEqlQueryTests extends ESTestCase {

    private final PushDownLimitToEqlQuery rule = new PushDownLimitToEqlQuery();
    private final LogicalOptimizerContext ctx = unboundLogicalOptimizerContext();

    private static EqlQuery eqlQuery() {
        return new EqlQuery(Source.EMPTY, "idx", "any where true");
    }

    private static Limit limit(int value, LogicalPlan child) {
        return new Limit(Source.EMPTY, new Literal(Source.EMPTY, value, INTEGER), child);
    }

    public void testPushesLimitDirectlyAboveEqlQuery() {
        Limit limit = limit(10, eqlQuery());
        LogicalPlan optimized = rule.rule(limit, ctx);
        // The Limit node is kept on top (exact row trim) with its literal unchanged ...
        assertThat(optimized, instanceOf(Limit.class));
        assertThat(((Limit) optimized).limit().fold(ctx.foldCtx()), equalTo(10));
        // ... and the pushed size lands on the source.
        assertThat(((Limit) optimized).child(), instanceOf(EqlQuery.class));
        assertThat(((EqlQuery) ((Limit) optimized).child()).limit(), equalTo(10));
    }

    public void testKeepsSmallerExistingLimit() {
        Limit limit = limit(10, eqlQuery().withLimit(3));
        LogicalPlan optimized = rule.rule(limit, ctx);
        // The outer Limit literal is untouched; only the pushed source size takes the smaller value.
        assertThat(((Limit) optimized).limit().fold(ctx.foldCtx()), equalTo(10));
        assertThat(((EqlQuery) ((Limit) optimized).child()).limit(), equalTo(3));
    }

    public void testDoesNotPushThroughIntermediateFilter() {
        // A WHERE between LIMIT and the source means the limit is not on the immediate child, so nothing is pushed:
        // the pushed size could drop events the filter still needs.
        Filter filter = new Filter(Source.EMPTY, eqlQuery(), new Literal(Source.EMPTY, true, BOOLEAN));
        Limit limit = limit(10, filter);
        assertThat(rule.rule(limit, ctx), sameInstance(limit));
    }

    public void testConvergesWhenAlreadyPushed() {
        Limit limit = limit(5, eqlQuery().withLimit(5));
        // Already at the fixed point: the rule must return the same instance so the optimizer does not loop.
        assertThat(rule.rule(limit, ctx), sameInstance(limit));
    }

    public void testLimitZeroIsNotPushed() {
        Limit limit = limit(0, eqlQuery());
        LogicalPlan optimized = rule.rule(limit, ctx);
        // LIMIT 0 is left untouched for SkipQueryOnLimitZero; no size is pushed to EQL.
        assertThat(optimized, sameInstance(limit));
        assertThat(((EqlQuery) ((Limit) optimized).child()).limit(), nullValue());
    }

    public void testDoesNotPushWhenChildIsNotEqlQuery() {
        // Immediate child is another Limit, not an EqlQuery, so the rule leaves it for the inner match.
        Limit inner = limit(5, eqlQuery());
        Limit outer = limit(10, inner);
        assertThat(rule.rule(outer, ctx), sameInstance(outer));
    }
}
