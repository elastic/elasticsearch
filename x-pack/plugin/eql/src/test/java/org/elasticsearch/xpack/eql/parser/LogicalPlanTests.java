/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.parser;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.eql.action.RequestDefaults;
import org.elasticsearch.xpack.eql.plan.logical.Head;
import org.elasticsearch.xpack.eql.plan.logical.Join;
import org.elasticsearch.xpack.eql.plan.logical.KeyedFilter;
import org.elasticsearch.xpack.eql.plan.logical.LimitWithOffset;
import org.elasticsearch.xpack.eql.plan.logical.Sequence;
import org.elasticsearch.xpack.eql.plan.physical.LocalRelation;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.Order;
import org.elasticsearch.xpack.ql.expression.Order.NullsPosition;
import org.elasticsearch.xpack.ql.expression.Order.OrderDirection;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.expression.predicate.logical.And;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.OrderBy;
import org.elasticsearch.xpack.ql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.ql.type.DateUtils.UTC;

public class LogicalPlanTests extends ESTestCase {

    private final EqlParser parser = new EqlParser();

    private Expression expr(String source) {
        return parser.createExpression(source);
    }

    private static Attribute timestamp() {
        return new UnresolvedAttribute(Source.EMPTY, "@timestamp");
    }

    private static LogicalPlan relation() {
        return new UnresolvedRelation(Source.EMPTY, null, "", false, "");
    }

    public void testAnyQuery() {
        LogicalPlan fullQuery = parser.createStatement("any where process_name == \"net.exe\"");
        Expression fullExpression = expr("process_name == \"net.exe\"");

        assertEquals(wrapFilter(fullExpression), fullQuery);
    }

    public void testEventQuery() {
        LogicalPlan fullQuery = parser.createStatement("process where process_name == \"net.exe\"");
        Expression fullExpression = expr("event.category == \"process\" and process_name == \"net.exe\"");

        assertEquals(wrapFilter(fullExpression), fullQuery);
    }

    public void testParameterizedEventQuery() {
        ParserParams params = new ParserParams(UTC).fieldEventCategory("myCustomEvent");
        LogicalPlan fullQuery = parser.createStatement("process where process_name == \"net.exe\"", params);
        Expression fullExpression = expr("myCustomEvent == \"process\" and process_name == \"net.exe\"");

        assertEquals(wrapFilter(fullExpression), fullQuery);
    }

    public void testJoinPlan() {
        LogicalPlan plan = parser.createStatement(
            "join by pid "
                + "  [process where true] "
                + "  [network where true] "
                + "  [registry where true] "
                + "  [file where true] "
                + " "
                + "until [process where event_subtype_full == \"termination_event\"]"
        );

        plan = defaultPipes(plan);
        assertEquals(Join.class, plan.getClass());
        Join join = (Join) plan;
        assertEquals(KeyedFilter.class, join.until().getClass());
        KeyedFilter f = join.until();
        Expression key = f.keys().get(0);
        assertEquals(UnresolvedAttribute.class, key.getClass());
        assertEquals("pid", ((UnresolvedAttribute) key).name());

        List<? extends LogicalPlan> queries = join.queries();
        assertEquals(4, queries.size());
        LogicalPlan subPlan = queries.get(0);
        assertEquals(KeyedFilter.class, subPlan.getClass());
        KeyedFilter kf = (KeyedFilter) subPlan;

        List<? extends NamedExpression> keys = kf.keys();
        key = keys.get(0);
        assertEquals(UnresolvedAttribute.class, key.getClass());
        assertEquals("pid", ((UnresolvedAttribute) key).name());
    }

    public void testSequencePlan() {
        LogicalPlan plan = parser.createStatement(
            "sequence by pid with maxspan=2s " + "    [process where process_name == \"*\" ] " + "    [file where file_path == \"*\"]"
        );

        plan = defaultPipes(plan);
        assertEquals(Sequence.class, plan.getClass());
        Sequence seq = (Sequence) plan;
        assertEquals(KeyedFilter.class, seq.until().getClass());
        assertEquals(LocalRelation.class, seq.until().child().getClass());

        List<? extends LogicalPlan> queries = seq.queries();
        assertEquals(2, queries.size());
        LogicalPlan subPlan = queries.get(0);
        assertEquals(KeyedFilter.class, subPlan.getClass());
        KeyedFilter kf = (KeyedFilter) subPlan;

        List<? extends NamedExpression> keys = kf.keys();
        NamedExpression key = keys.get(0);
        assertEquals(UnresolvedAttribute.class, key.getClass());
        assertEquals("pid", key.name());

        TimeValue maxSpan = seq.maxSpan();
        assertEquals(new TimeValue(2, TimeUnit.SECONDS), maxSpan);
    }

    public void testQuotedEventType() {
        LogicalPlan plan = parser.createStatement(
            "sequence by pid with maxspan=2s "
                + "    [\"12\\\"34!@#$\" where process_name == \"test.exe\" ] "
                + "    [\"\"\"!@#$%test\"\"\\)(*&^\"\"\" where file_path == \"test.exe\"]"
        );

        plan = defaultPipes(plan);
        assertEquals(Sequence.class, plan.getClass());
        Sequence seq = (Sequence) plan;
        assertEquals(KeyedFilter.class, seq.until().getClass());
        assertEquals(LocalRelation.class, seq.until().child().getClass());

        List<? extends LogicalPlan> queries = seq.queries();
        assertEquals(2, queries.size());

        LogicalPlan query1 = queries.get(0);
        assertEquals(KeyedFilter.class, query1.getClass());
        KeyedFilter kf = (KeyedFilter) query1;
        assertEquals(Filter.class, kf.child().getClass());
        Filter f = (Filter) kf.child();
        assertEquals(And.class, f.condition().getClass());
        assertEquals(Equals.class, ((And) f.condition()).left().getClass());
        Equals eq = (Equals) ((And) f.condition()).left();
        assertEquals("12\"34!@#$", eq.right().fold());

        LogicalPlan query2 = queries.get(1);
        assertEquals(KeyedFilter.class, query2.getClass());
        kf = (KeyedFilter) query2;
        assertEquals(Filter.class, kf.child().getClass());
        f = (Filter) kf.child();
        assertEquals(And.class, f.condition().getClass());
        assertEquals(Equals.class, ((And) f.condition()).left().getClass());
        eq = (Equals) ((And) f.condition()).left();
        assertEquals("!@#$%test\"\"\\)(*&^", eq.right().fold());

        List<? extends NamedExpression> keys = kf.keys();
        NamedExpression key = keys.get(0);
        assertEquals(UnresolvedAttribute.class, key.getClass());
        assertEquals("pid", key.name());

        TimeValue maxSpan = seq.maxSpan();
        assertEquals(new TimeValue(2, TimeUnit.SECONDS), maxSpan);
    }

    public void testRepeatedQuery() throws Exception {
        LogicalPlan plan = parser.createStatement("sequence " + " [any where true] with runs=2" + " [any where true]");
        plan = defaultPipes(plan);
        assertEquals(Sequence.class, plan.getClass());
        Sequence seq = (Sequence) plan;

        List<? extends LogicalPlan> queries = seq.queries();
        assertEquals(3, queries.size());
    }

    private LogicalPlan wrapFilter(Expression exp) {
        LogicalPlan filter = new Filter(Source.EMPTY, relation(), exp);
        Order order = new Order(Source.EMPTY, timestamp(), OrderDirection.ASC, NullsPosition.FIRST);
        LogicalPlan sorted = new OrderBy(Source.EMPTY, filter, singletonList(order));
        return new Head(Source.EMPTY, new Literal(Source.EMPTY, RequestDefaults.SIZE, DataTypes.INTEGER), sorted);
    }

    private LogicalPlan defaultPipes(LogicalPlan plan) {
        assertTrue(plan instanceof LimitWithOffset);
        plan = ((LimitWithOffset) plan).child();
        assertTrue(plan instanceof OrderBy);
        return ((OrderBy) plan).child();
    }
}
