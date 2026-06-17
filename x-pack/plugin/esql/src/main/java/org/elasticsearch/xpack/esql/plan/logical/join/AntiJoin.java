/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;

/**
 * An anti join used to implement {@code WHERE field NOT IN (subquery)}.
 * <p>
 * The dual of {@link SemiJoin}: it shares the {@link AbstractSubqueryJoin} dedup pipeline and only flips the hooks that distinguish
 * {@code NOT IN} from {@code IN} — it uses {@link JoinTypes#ANTI}, wraps the inline filter in {@code Not}, keeps unmatched rows on the
 * hash-join path ({@code IS NULL} on the sentinel), and short-circuits to {@code Filter(FALSE)} on any NULL right value.
 */
public class AntiJoin extends AbstractSubqueryJoin {

    public AntiJoin(Source source, LogicalPlan left, LogicalPlan right, JoinConfig config) {
        super(source, left, right, config);
        assert config.type() == JoinTypes.ANTI : "AntiJoin requires join type ANTI, got [" + config.type() + "]";
    }

    public AntiJoin(Source source, LogicalPlan left, LogicalPlan right, List<Attribute> leftFields, List<Attribute> rightFields) {
        super(source, left, right, JoinTypes.ANTI, leftFields, rightFields);
    }

    @Override
    protected NodeInfo<Join> info() {
        JoinConfig config = config();
        return NodeInfo.create(this, AntiJoin::new, left(), right(), config.leftFields(), config.rightFields());
    }

    @Override
    public Join replaceChildren(LogicalPlan left, LogicalPlan right) {
        return new AntiJoin(source(), left, right, config());
    }

    @Override
    protected LogicalPlan buildEmptyRightSidePlan(Source source) {
        return new Filter(source, left(), Literal.TRUE);
    }

    @Override
    protected boolean shortCircuitOnAnyRightNull() {
        return true;
    }

    @Override
    protected Expression wrapInExpression(Source source, Expression in) {
        return new Not(source, in);
    }

    @Override
    protected Expression sentinelFilterCondition(Source source, Attribute sentinel) {
        return new IsNull(source, sentinel);
    }
}
