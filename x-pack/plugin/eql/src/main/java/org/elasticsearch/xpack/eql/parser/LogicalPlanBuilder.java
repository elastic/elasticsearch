/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.parser;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.expression.predicate.logical.And;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;

public abstract class LogicalPlanBuilder extends ExpressionBuilder {

    // TODO: these need to be made configurable
    private static final String EVENT_TYPE = "event_type";

    @Override
    public LogicalPlan visitEventQuery(EqlBaseParser.EventQueryContext ctx) {
        Source source = source(ctx);
        Expression condition = expression(ctx.expression());

        if (ctx.event != null) {
            Source eventTypeSource = source(ctx.event);
            String eventTypeName = visitIdentifier(ctx.event);
            Literal eventTypeValue = new Literal(eventTypeSource, eventTypeName, DataTypes.KEYWORD);

            UnresolvedAttribute eventTypeField = new UnresolvedAttribute(eventTypeSource, EVENT_TYPE);
            Expression eventTypeCheck = new Equals(eventTypeSource, eventTypeField, eventTypeValue);

            condition = new And(source, eventTypeCheck, condition);

        }

        return new Filter(source(ctx), new UnresolvedRelation(Source.EMPTY, null, "", false, ""), condition);
    }
}
