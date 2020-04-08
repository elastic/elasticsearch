/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.parser;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.eql.plan.logical.Join;
import org.elasticsearch.xpack.eql.plan.logical.KeyedFilter;
import org.elasticsearch.xpack.eql.plan.logical.Sequence;
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

import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonList;

public class LogicalPlanTests extends ESTestCase {

    private final EqlParser parser = new EqlParser();

    private Expression expr(String source) {
        return parser.createExpression(source);
    }

    public void testAnyQuery() {
        LogicalPlan fullQuery = parser.createStatement("any where process_name == 'net.exe'");
        Expression fullExpression = expr("process_name == 'net.exe'");

        LogicalPlan filter = new Filter(Source.EMPTY, new UnresolvedRelation(Source.EMPTY, null, "", false, ""), fullExpression);
        Order order = new Order(Source.EMPTY, new UnresolvedAttribute(Source.EMPTY, "@timestamp"), OrderDirection.ASC, NullsPosition.FIRST);
        LogicalPlan expected = new OrderBy(Source.EMPTY, filter, singletonList(order));
        assertEquals(expected, fullQuery);
    }

    public void testEventQuery() {
        LogicalPlan fullQuery = parser.createStatement("process where process_name == 'net.exe'");
        Expression fullExpression = expr("event.category == 'process' and process_name == 'net.exe'");

        LogicalPlan filter = new Filter(Source.EMPTY, new UnresolvedRelation(Source.EMPTY, null, "", false, ""), fullExpression);
        Order order = new Order(Source.EMPTY, new UnresolvedAttribute(Source.EMPTY, "@timestamp"), OrderDirection.ASC, NullsPosition.FIRST);
        LogicalPlan expected = new OrderBy(Source.EMPTY, filter, singletonList(order));
        assertEquals(expected, fullQuery);
    }

    public void testParameterizedEventQuery() {
        ParserParams params = new ParserParams().fieldEventCategory("myCustomEvent");
        LogicalPlan fullQuery = parser.createStatement("process where process_name == 'net.exe'", params);
        Expression fullExpression = expr("myCustomEvent == 'process' and process_name == 'net.exe'");

        LogicalPlan filter = new Filter(Source.EMPTY, new UnresolvedRelation(Source.EMPTY, null, "", false, ""), fullExpression);
        Order order = new Order(Source.EMPTY, new UnresolvedAttribute(Source.EMPTY, "@timestamp"), OrderDirection.ASC, NullsPosition.FIRST);
        LogicalPlan expected = new OrderBy(Source.EMPTY, filter, singletonList(order));
        assertEquals(expected, fullQuery);
    }
    

    public void testJoinPlan() {
        LogicalPlan plan = parser.createStatement(
                "join by pid " +
                "  [process where true] " +
                "  [network where true] " +
                "  [registry where true] " +
                "  [file where true] " +
                " " +
                "until [process where event_subtype_full == \"termination_event\"]");

        assertEquals(Join.class, plan.getClass());
        Join join = (Join) plan;
        assertEquals(KeyedFilter.class, join.until().getClass());
        KeyedFilter f = (KeyedFilter) join.until();
        Expression key = f.keys().get(0);
        assertEquals(UnresolvedAttribute.class, key.getClass());
        assertEquals("pid", ((UnresolvedAttribute) key).name());

        List<LogicalPlan> queries = join.queries();
        assertEquals(4, queries.size());
        LogicalPlan subPlan = queries.get(0);
        assertEquals(KeyedFilter.class, subPlan.getClass());
        KeyedFilter kf = (KeyedFilter) subPlan;

        List<Expression> keys = kf.keys();
        key = keys.get(0);
        assertEquals(UnresolvedAttribute.class, key.getClass());
        assertEquals("pid", ((UnresolvedAttribute) key).name());

    }
    
    
    public void testSequencePlan() {
        LogicalPlan plan = parser.createStatement(
                "sequence by pid with maxspan=2s " +
                "    [process where process_name == \"*\" ] " +
                "    [file where file_path == \"*\"]");

        assertEquals(Sequence.class, plan.getClass());
        Sequence seq = (Sequence) plan;
        assertEquals(Filter.class, seq.until().getClass());
        Filter f = (Filter) seq.until();
        assertEquals(false, f.condition().fold());

        List<LogicalPlan> queries = seq.queries();
        assertEquals(1, queries.size());
        LogicalPlan subPlan = queries.get(0);
        assertEquals(KeyedFilter.class, subPlan.getClass());
        KeyedFilter kf = (KeyedFilter) subPlan;

        List<Expression> keys = kf.keys();
        Expression key = keys.get(0);
        assertEquals(UnresolvedAttribute.class, key.getClass());
        assertEquals("pid", ((UnresolvedAttribute) key).name());

        TimeValue maxSpan = seq.maxSpan();
        assertEquals(new TimeValue(2, TimeUnit.SECONDS), maxSpan);

    }
}