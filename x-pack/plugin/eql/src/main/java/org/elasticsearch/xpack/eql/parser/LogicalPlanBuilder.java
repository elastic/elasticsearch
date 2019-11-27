/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.parser;

import org.elasticsearch.xpack.eql.parser.EqlBaseParser.ConditionContext;
import org.elasticsearch.xpack.sql.analysis.index.EsIndex;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.sql.expression.predicate.logical.And;
import org.elasticsearch.xpack.sql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.sql.plan.logical.EsRelation;
import org.elasticsearch.xpack.sql.plan.logical.Filter;
import org.elasticsearch.xpack.sql.tree.Source;

abstract class LogicalPlanBuilder extends ExpressionBuilder {

    // TODO: needs to be made configurable
    private static final String EVENT_TYPE = "event_type";

    @Override
    public Object visitCondition(ConditionContext ctx) {
        Source eventSource = source(ctx.event);
        Literal typeValue = Literal.of(eventSource, visitQualifiedName(ctx.event));
        Equals typeCondition = new Equals(eventSource, new UnresolvedAttribute(eventSource, EVENT_TYPE), typeValue);
        Expression condition = expression(ctx.booleanExpression());

        return new Filter(source(ctx), new EsRelation(Source.EMPTY, esIndex(), false),
                new And(source(ctx.WHERE()), typeCondition, condition));
    }

    abstract EsIndex esIndex();
}
