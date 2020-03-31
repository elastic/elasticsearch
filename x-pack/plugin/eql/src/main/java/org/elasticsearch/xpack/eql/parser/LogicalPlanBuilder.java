/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.parser;

import org.elasticsearch.xpack.eql.parser.EqlBaseParser.JoinContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.JoinTermContext;
import org.elasticsearch.xpack.eql.plan.logical.Join;
import org.elasticsearch.xpack.eql.plan.logical.KeyedFilter;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.Order;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.expression.predicate.logical.And;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.OrderBy;
import org.elasticsearch.xpack.ql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.singletonList;

public abstract class LogicalPlanBuilder extends ExpressionBuilder {

    private final ParserParams params;
    private final UnresolvedRelation RELATION = new UnresolvedRelation(Source.EMPTY, null, "", false, "");

    public LogicalPlanBuilder(ParserParams params) {
        this.params = params;
    }

    @Override
    public LogicalPlan visitEventQuery(EqlBaseParser.EventQueryContext ctx) {
        Source source = source(ctx);
        Expression condition = expression(ctx.expression());

        if (ctx.event != null) {
            Source eventSource = source(ctx.event);
            String eventName = visitIdentifier(ctx.event);
            Literal eventValue = new Literal(eventSource, eventName, DataTypes.KEYWORD);

            UnresolvedAttribute eventField = new UnresolvedAttribute(eventSource, params.fieldEventCategory());
            Expression eventMatch = new Equals(eventSource, eventField, eventValue);

            condition = new And(source, eventMatch, condition);
        }

        Filter filter = new Filter(source, RELATION, condition);
        // add implicit sorting - when pipes are added, this would better sit there (as a default pipe)
        Order order = new Order(source, new UnresolvedAttribute(source, params.fieldTimestamp()), Order.OrderDirection.ASC,
                Order.NullsPosition.FIRST);
        OrderBy orderBy = new OrderBy(source, filter, singletonList(order));
        return orderBy;
    }

    @Override
    public Join visitJoin(JoinContext ctx) {
        List<Expression> parentJoinKeys = visitJoinKeys(ctx.by);

        LogicalPlan until;
        
        if (ctx.until != null) {
            until = visitJoinTerm(ctx.until, parentJoinKeys);
        } else {
            // no until declared means the condition never gets executed and thus folds to false
            until = new Filter(source(ctx), RELATION, new Literal(source(ctx), Boolean.FALSE, DataTypes.BOOLEAN));
        }
        
        int numberOfKeys = -1;
        List<LogicalPlan> queries = new ArrayList<>(ctx.joinTerm().size());

        for (JoinTermContext joinTermCtx : ctx.joinTerm()) {
            KeyedFilter joinTerm = visitJoinTerm(joinTermCtx, parentJoinKeys);
            int keySize = joinTerm.keys().size();
            if (numberOfKeys < 0) {
                numberOfKeys = keySize;
            } else {
                if (numberOfKeys != keySize) {
                    Source src = source(joinTermCtx.by != null ? joinTermCtx.by : joinTermCtx);
                    int expected = numberOfKeys - parentJoinKeys.size();
                    int found = keySize - parentJoinKeys.size();
                    throw new ParsingException(src, "Inconsistent number of join keys specified; expected [{}] but found [{}]", expected,
                            found);
                }
                queries.add(joinTerm);
            }
        }

        return new Join(source(ctx), queries, until);
    }

    public KeyedFilter visitJoinTerm(JoinTermContext ctx, List<Expression> joinKeys) {
        List<Expression> keys = CollectionUtils.combine(joinKeys, visitJoinKeys(ctx.by));
        return new KeyedFilter(source(ctx), visitEventQuery(ctx.subquery().eventQuery()), keys);
    }
}