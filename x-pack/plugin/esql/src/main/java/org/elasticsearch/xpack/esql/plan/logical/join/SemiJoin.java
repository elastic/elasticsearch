/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;

/**
 * A semi join implements {@code WHERE field IN (subquery)}.
 * <p>
 * It uses the default {@link AbstractSubqueryJoin} hooks unchanged: the {@code inlineData} pipeline produces {@code Filter(In(...))}
 * (filter path) or a sentinel LEFT-join + Project (hash-join path), keeping the left rows that match at least one subquery value. The dual
 * {@code NOT IN} form lives in {@link AntiJoin}; the OR-embedded form that preserves every row lives in {@link MarkJoin}.
 */
public class SemiJoin extends AbstractSubqueryJoin {

    public SemiJoin(Source source, LogicalPlan left, LogicalPlan right, JoinConfig config) {
        super(source, left, right, config);
        assert config.type() == JoinTypes.SEMI : "SemiJoin requires join type SEMI, got [" + config.type() + "]";
    }

    public SemiJoin(Source source, LogicalPlan left, LogicalPlan right, List<Attribute> leftFields, List<Attribute> rightFields) {
        super(source, left, right, JoinTypes.SEMI, leftFields, rightFields);
    }

    @Override
    protected NodeInfo<Join> info() {
        JoinConfig config = config();
        return NodeInfo.create(this, SemiJoin::new, left(), right(), config.leftFields(), config.rightFields());
    }

    @Override
    public Join replaceChildren(LogicalPlan left, LogicalPlan right) {
        return new SemiJoin(source(), left, right, config());
    }

    @Override
    protected LogicalPlan buildEmptyRightSidePlan(Source source) {
        return new Filter(source, left(), Literal.FALSE);
    }

}
