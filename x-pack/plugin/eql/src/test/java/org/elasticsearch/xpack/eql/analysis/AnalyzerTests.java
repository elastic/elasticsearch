/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.analysis;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.eql.expression.OptionalMissingAttribute;
import org.elasticsearch.xpack.eql.expression.OptionalResolvedAttribute;
import org.elasticsearch.xpack.eql.expression.function.EqlFunctionRegistry;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.Concat;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.ToString;
import org.elasticsearch.xpack.eql.parser.EqlParser;
import org.elasticsearch.xpack.eql.plan.logical.Head;
import org.elasticsearch.xpack.eql.plan.logical.KeyedFilter;
import org.elasticsearch.xpack.eql.plan.logical.Sequence;
import org.elasticsearch.xpack.eql.stats.Metrics;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.predicate.logical.And;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.OrderBy;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.ql.type.TypesTests;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.eql.EqlTestUtils.TEST_CFG;

public class AnalyzerTests extends ESTestCase {

    private static final String INDEX_NAME = "test";
    private IndexResolution index = loadIndexResolution("mapping-default.json");

    private static Map<String, EsField> loadEqlMapping(String name) {
        return TypesTests.loadMapping(name);
    }

    public static IndexResolution loadIndexResolution(String name) {
        return IndexResolution.valid(new EsIndex(INDEX_NAME, loadEqlMapping(name)));
    }

    public void testOptionalFieldOnTheLeft() {
        Equals check = equalsCondition("process where ?foo == 123");
        checkMissingOptional(check.left());
        assertTrue(check.right() instanceof Literal);
        assertEquals(123, ((Literal) check.right()).value());
    }

    public void testOptionalFieldOnTheRight() {
        Equals check = equalsCondition("process where 123 == ?bar");
        checkMissingOptional(check.right());
        assertTrue(check.left() instanceof Literal);
        assertEquals(123, ((Literal) check.left()).value());
    }

    public void testOptionalFieldsInsideFunction() {
        Equals check = equalsCondition("process where concat(?foo, \" \", ?bar) == \"test\"");
        assertEquals("test", ((Literal) check.right()).value());
        assertTrue(check.left() instanceof Concat);
        Concat concat = (Concat) check.left();
        List<Expression> arguments = new ArrayList<>(3);
        checkMissingOptional(concat.arguments().get(0));
        assertEquals(new Literal(Source.EMPTY, " ", DataTypes.KEYWORD), concat.arguments().get(1));
        checkMissingOptional(concat.arguments().get(2));
    }

    public void testOptionalFieldExistsInMapping() {
        Equals check = equalsCondition("process where ?pid == 123");
        assertTrue(check.left() instanceof FieldAttribute);
        assertEquals("pid", ((FieldAttribute) check.left()).name());
        assertTrue(check.right() instanceof Literal);
        assertEquals(123, ((Literal) check.right()).value());
    }

    public void testOptionalFieldsAsSequenceKey() {
        String eql = "sequence by ?x "
            + "[any where ?x == 123] by ?pid "
            + "[any where true] by pid "
            + "[any where ?y != null] by ?z "
            + "until [any where string(?t) == \"null\"] by ?w";
        LogicalPlan plan = accept(index, eql);
        assertTrue(plan instanceof Head);
        Head head = (Head) plan;
        assertTrue(head.child() instanceof OrderBy);
        OrderBy orderBy = (OrderBy) head.child();
        assertTrue(orderBy.child() instanceof Sequence);

        Sequence s = (Sequence) orderBy.child();
        List<KeyedFilter> queries = s.queries();
        assertEquals(3, queries.size());

        // any where ?x == 123 by ?x, ?pid
        KeyedFilter q = queries.get(0);
        assertEquals(2, q.keys().size());
        List<? extends NamedExpression> keys = q.keys();
        assertEquals(OptionalMissingAttribute.class, keys.get(0).getClass());
        assertEquals(OptionalResolvedAttribute.class, keys.get(1).getClass());
        OptionalMissingAttribute optional = (OptionalMissingAttribute) keys.get(0);
        assertEquals(true, optional.resolved());
        assertEquals("x", optional.name());
        FieldAttribute field = (FieldAttribute) keys.get(1);
        assertEquals("pid", field.name());

        assertTrue(q.child() instanceof Filter);
        Filter filter = (Filter) q.child();
        assertTrue(filter.condition() instanceof Equals);
        Equals equals = (Equals) filter.condition();
        checkMissingOptional(equals.left());
        assertEquals(123, ((Literal) equals.right()).value());

        // any where true by ?x, pid
        q = queries.get(1);
        assertEquals(2, q.keys().size());
        keys = q.keys();
        assertEquals(OptionalMissingAttribute.class, keys.get(0).getClass());
        assertEquals(FieldAttribute.class, keys.get(1).getClass());
        optional = (OptionalMissingAttribute) keys.get(0);
        assertEquals(true, optional.resolved());
        assertEquals("x", optional.name());
        field = (FieldAttribute) keys.get(1);
        assertEquals("pid", field.name());

        assertTrue(q.child() instanceof Filter);
        filter = (Filter) q.child();
        assertTrue(filter.condition() instanceof Literal);
        Literal l = (Literal) filter.condition();
        assertEquals(Literal.TRUE, l);

        // any where ?y != null by ?x, ?z
        q = queries.get(2);
        assertEquals(2, q.keys().size());
        keys = q.keys();
        assertEquals(OptionalMissingAttribute.class, keys.get(0).getClass());
        assertEquals(OptionalMissingAttribute.class, keys.get(1).getClass());
        optional = (OptionalMissingAttribute) keys.get(0);
        assertEquals(true, optional.resolved());
        assertEquals("x", optional.name());
        optional = (OptionalMissingAttribute) keys.get(1);
        assertEquals(true, optional.resolved());
        assertEquals("z", optional.name());

        assertTrue(q.child() instanceof Filter);
        filter = (Filter) q.child();
        assertTrue(filter.condition() instanceof Not);
        Not not = (Not) filter.condition();
        equals = (Equals) not.field();
        checkMissingOptional(equals.left());
        checkMissingOptional(equals.right());

        // until [any where string(?t) == \"null\"] by ?w
        q = s.until();
        keys = q.keys();
        assertEquals(OptionalMissingAttribute.class, keys.get(0).getClass());
        assertEquals(OptionalMissingAttribute.class, keys.get(1).getClass());
        optional = (OptionalMissingAttribute) keys.get(0);
        assertEquals(true, optional.resolved());
        assertEquals("x", optional.name());
        optional = (OptionalMissingAttribute) keys.get(1);
        assertEquals(true, optional.resolved());
        assertEquals("w", optional.name());

        assertTrue(q.child() instanceof Filter);
        filter = (Filter) q.child();
        assertTrue(filter.condition() instanceof Equals);
        equals = (Equals) filter.condition();
        assertTrue(equals.right() instanceof Literal);
        assertEquals("null", ((Literal) equals.right()).value());
        assertEquals(DataTypes.KEYWORD, ((Literal) equals.right()).dataType());
        assertTrue(equals.left() instanceof ToString);
        checkMissingOptional(((ToString) equals.left()).value());
    }

    private LogicalPlan accept(IndexResolution resolution, String eql) {
        PreAnalyzer preAnalyzer = new PreAnalyzer();
        Analyzer analyzer = new Analyzer(TEST_CFG, new EqlFunctionRegistry(), new Verifier(new Metrics()));
        EqlParser parser = new EqlParser();
        LogicalPlan plan = parser.createStatement(eql);
        return analyzer.analyze(preAnalyzer.preAnalyze(plan, resolution));
    }

    private LogicalPlan accept(String eql) {
        return accept(index, eql);
    }

    private Equals equalsCondition(String query) {
        LogicalPlan plan = accept(query);
        assertTrue(plan instanceof Head);
        Head head = (Head) plan;
        assertTrue(head.child() instanceof OrderBy);
        OrderBy orderBy = (OrderBy) head.child();
        assertTrue(orderBy.child() instanceof Filter);

        Filter filter = (Filter) orderBy.child();
        assertTrue(filter.condition() instanceof And);
        And condition = (And) filter.condition();
        assertTrue(condition.right() instanceof Equals);
        return (Equals) condition.right();
    }

    private void checkMissingOptional(Expression e) {
        assertEquals(DataTypes.NULL, e.dataType());
        assertTrue(e.foldable());
        assertNull(e.fold());
    }
}
