/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.parser;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Order;
import org.elasticsearch.xpack.ql.expression.Order.NullsPosition;
import org.elasticsearch.xpack.ql.expression.Order.OrderDirection;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.OrderBy;
import org.elasticsearch.xpack.ql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.ql.tree.Source;

import static java.util.Collections.singletonList;

public class LogicalPlanTests extends ESTestCase {

    private final EqlParser parser = new EqlParser();

    public Expression expr(String source) {
        return parser.createExpression(source);
    }

    public void testEventQuery() {
        LogicalPlan fullQuery = parser.createStatement("process where process_name == 'net.exe'");
        Expression fullExpression = expr("event_type == 'process' and process_name == 'net.exe'");

        LogicalPlan filter = new Filter(Source.EMPTY, new UnresolvedRelation(Source.EMPTY, null, "", false, ""), fullExpression);
        Order order = new Order(Source.EMPTY, new UnresolvedAttribute(Source.EMPTY, "timestamp"), OrderDirection.ASC, NullsPosition.FIRST);
        LogicalPlan expected = new OrderBy(Source.EMPTY, filter, singletonList(order));
        assertEquals(expected, fullQuery);
    }

    public void testParameterizedEventQuery() {
        ParserParams params = new ParserParams().fieldEventType("myCustomEvent");
        LogicalPlan fullQuery = parser.createStatement("process where process_name == 'net.exe'", params);
        Expression fullExpression = expr("myCustomEvent == 'process' and process_name == 'net.exe'");

        LogicalPlan filter = new Filter(Source.EMPTY, new UnresolvedRelation(Source.EMPTY, null, "", false, ""), fullExpression);
        Order order = new Order(Source.EMPTY, new UnresolvedAttribute(Source.EMPTY, "timestamp"), OrderDirection.ASC, NullsPosition.FIRST);
        LogicalPlan expected = new OrderBy(Source.EMPTY, filter, singletonList(order));
        assertEquals(expected, fullQuery);
    }
}
