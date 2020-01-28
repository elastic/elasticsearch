/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
