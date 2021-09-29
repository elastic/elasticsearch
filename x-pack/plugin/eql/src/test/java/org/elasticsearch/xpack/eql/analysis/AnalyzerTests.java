/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.analysis;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.eql.expression.function.EqlFunctionRegistry;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.Concat;
import org.elasticsearch.xpack.eql.parser.EqlParser;
import org.elasticsearch.xpack.eql.plan.logical.Head;
import org.elasticsearch.xpack.eql.stats.Metrics;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.expression.predicate.logical.And;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.eql.EqlTestUtils.TEST_CFG;

public class AnalyzerTests extends ESTestCase {

    private static final String INDEX_NAME = "test";
    private static Set<UnresolvedAttribute> optionals = new HashSet<>();
    static {
        optionals.add(new UnresolvedAttribute(Source.EMPTY, "foo"));
        optionals.add(new UnresolvedAttribute(Source.EMPTY, "bar"));
        optionals.add(new UnresolvedAttribute(Source.EMPTY, "pid")); // this one also exists in the index mapping
    }
    private EqlParser parser = new EqlParser(optionals);
    private IndexResolution index = loadIndexResolution("mapping-default.json");

    private static Map<String, EsField> loadEqlMapping(String name) {
        return TypesTests.loadMapping(name);
    }

    public static IndexResolution loadIndexResolution(String name) {
        return IndexResolution.valid(new EsIndex(INDEX_NAME, loadEqlMapping(name)));
    }

    private LogicalPlan accept(IndexResolution resolution, String eql) {
        PreAnalyzer preAnalyzer = new PreAnalyzer();
        Analyzer analyzer = new Analyzer(TEST_CFG, new EqlFunctionRegistry(), new Verifier(new Metrics()), optionals);
        return analyzer.analyze(preAnalyzer.preAnalyze(parser.createStatement(eql), resolution));
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

    public void testOptionalFieldOnTheLeft() {
        Equals check = equalsCondition("process where ?foo == 123");
        assertEquals(Literal.NULL, check.left());
        assertTrue(check.right() instanceof Literal);
        assertEquals(123, ((Literal) check.right()).value());
    }

    public void testOptionalFieldOnTheRight() {
        Equals check = equalsCondition("process where 123 == ?bar");
        assertEquals(Literal.NULL, check.right());
        assertTrue(check.left() instanceof Literal);
        assertEquals(123, ((Literal) check.left()).value());
    }

    public void testOptionalFieldsInsideFunction() {
        Equals check = equalsCondition("process where concat(?foo, \" \", ?bar) == \"test\"");
        assertEquals("test", ((Literal) check.right()).value());
        assertTrue(check.left() instanceof Concat);
        Concat concat = (Concat) check.left();
        List<Expression> arguments = new ArrayList<>(3);
        arguments.add(Literal.NULL);
        arguments.add(new Literal(Source.EMPTY, " ", DataTypes.KEYWORD));
        arguments.add(Literal.NULL);
        assertEquals(arguments, concat.arguments());
    }

    public void testOptionalFieldExistsInMapping() {
        Equals check = equalsCondition("process where ?pid == 123");
        assertTrue(check.left() instanceof FieldAttribute);
        assertEquals("pid", ((FieldAttribute) check.left()).name());
        assertTrue(check.right() instanceof Literal);
        assertEquals(123, ((Literal) check.right()).value());
    }
}
