/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.eql.parser.EqlQueryMode;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.EqlRelation;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

/**
 * Folds a row {@code LIMIT} sitting directly above an {@link EqlRelation} into the relation's request {@code size},
 * so {@code EQL idx "process where true" | LIMIT n} fetches ~n matches instead of the EQL engine default. Mirrors
 * {@link PushLimitToKnn}: the value is folded into the leaf but the {@link Limit} node is kept (the operator still
 * trims, and keeping it is harmless if the response over-returns).
 *
 * <p>Applies in every mode. The request {@code size} bounds the number of matches — events, sequences or samples —
 * and each match unnests to at least one row, so pushing {@code size = n} yields at least n rows and the kept Limit
 * still trims to n rows: it can never under-fetch. In ES|QL {@code LIMIT} bounds rows (so it may split a sequence
 * mid-match); {@code WITH {"size"}} is the way to bound whole matches. A limit that does not sit directly above the
 * relation (e.g. {@code | WHERE … | LIMIT n}) is not pushed: the source must over-scan so the downstream filter still
 * sees enough rows. When no limit is pushed the request falls back to the ES|QL result-truncation cap (see
 * {@code LocalExecutionPlanner#planEqlSource}).
 *
 * <p>The push is also skipped when the delegated search would return a tail rather than a head — either the EQL query
 * carries its own explicit {@code head}/{@code tail} pipe, or it asks for {@code WITH {"result_position":"tail"}}. A
 * pushed size over a tail fetches a suffix while the kept {@code Limit} trims a prefix, so the same query would return
 * different events depending on whether the push fired (e.g. {@code "… | tail 3"} or {@code result_position:"tail"}
 * with an outer {@code LIMIT 2} would yield the last 2 rather than the first 2). In that case the query keeps its own
 * limit and the outer {@code LIMIT} trims the result. The default (and explicit {@code head}) position returns a stable
 * prefix, so the push is sound there.
 */
public class PushLimitIntoEqlRelation extends OptimizerRules.ParameterizedOptimizerRule<Limit, LogicalOptimizerContext> {

    public PushLimitIntoEqlRelation() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    public LogicalPlan rule(Limit limit, LogicalOptimizerContext ctx) {
        if (limit.child() instanceof EqlRelation relation && limit.limit().foldable() && pushKeepsLimitStable(relation)) {
            int value = (Integer) limit.limit().fold(ctx.foldCtx());
            // Keep the smallest limit; return the same instance when nothing changes so the fixed-point batch stops.
            if (relation.pushedLimit() == null || value < relation.pushedLimit()) {
                return limit.replaceChild(relation.withPushedLimit(value));
            }
        }
        return limit;
    }

    /**
     * Whether folding the outer {@code LIMIT} into the request size returns the same rows regardless of whether the push
     * fires (a blocking op upstream can prevent it). True only when the delegated search yields a stable prefix: the
     * query has no explicit {@code head}/{@code tail} of its own, and it does not ask for tail results. A pushed size
     * over a tail fetches a suffix while the kept {@code Limit} trims a prefix, so the rows would depend on plan shape.
     */
    private static boolean pushKeepsLimitStable(EqlRelation relation) {
        // The query is a folded string literal by the time this rule runs (ResolveEqlRelation requires it); if for any
        // reason it is not, be conservative and do not push.
        if (relation.query() instanceof Literal literal && literal.value() instanceof BytesRef bytesRef) {
            boolean noExplicitLimit = EqlQueryMode.hasExplicitLimit(BytesRefs.toString(bytesRef)) == false;
            // result_position defaults to "head" in EqlRequests; only an explicit tail override makes the push unstable.
            boolean tailRequested = "tail".equals(relation.options().get("result_position"));
            return noExplicitLimit && tailRequested == false;
        }
        return false;
    }
}
