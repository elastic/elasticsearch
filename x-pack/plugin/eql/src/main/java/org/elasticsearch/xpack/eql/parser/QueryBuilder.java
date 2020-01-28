/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.parser;

import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.expression.predicate.logical.And;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;

public class QueryBuilder extends ExpressionBuilder {

    @Override
    public Expression visitEventQuery(EqlBaseParser.EventQueryContext ctx) {
        Source source = source(ctx);
        Expression condition = expression(ctx.expression());

        if (ctx.event != null) {
            Source eventTypeSource = source(ctx.event);
            String eventTypeName = visitIdentifier(ctx.event);
            Literal eventTypeValue =  new Literal(eventTypeSource, eventTypeName, DataTypes.KEYWORD);

            UnresolvedAttribute eventTypeField = new UnresolvedAttribute(eventTypeSource, "event.category");
            Expression eventTypeCheck = new Equals(eventTypeSource, eventTypeField, eventTypeValue);

            return new And(source, eventTypeCheck, condition);

        } else {
            return condition;
        }
    }
}
