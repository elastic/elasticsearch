/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.parser;

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

import static java.util.Collections.singletonList;

public abstract class LogicalPlanBuilder extends ExpressionBuilder {

    private final ParserParams params;

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

            UnresolvedAttribute eventField = new UnresolvedAttribute(eventSource, params.fieldEventType());
            Expression eventMatch = new Equals(eventSource, eventField, eventValue);

            condition = new And(source, eventMatch, condition);
        }

        Filter filter = new Filter(source, new UnresolvedRelation(Source.EMPTY, null, "", false, ""), condition);
        // add implicit sorting - when pipes are added, this would better seat there (as a default pipe)
        Order order = new Order(source, new UnresolvedAttribute(source, params.fieldTimestamp()), Order.OrderDirection.ASC,
                Order.NullsPosition.FIRST);
        OrderBy orderBy = new OrderBy(source, filter, singletonList(order));
        return orderBy;
    }
}
