/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import net.nextencia.rrdiagram.grammar.model.Repetition;
import net.nextencia.rrdiagram.grammar.model.Sequence;
import net.nextencia.rrdiagram.grammar.model.SpecialSequence;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class RailRoadDiagramTests extends ESTestCase {
    private static final EsqlFunctionRegistry registry = new EsqlFunctionRegistry();

    /**
     * Test a very simple single argument function
     */
    public void testToString() {
        var definition = registry.resolveFunction("to_string");
        var rails = RailRoadDiagram.svgSequence(definition);
        var expressions = rails.getExpressions();
        var expected = List.of("(? TO_STRING ?)", "'('", "'field'", "')'");
        assertThat("Expression count", expressions.length, equalTo(expected.size()));
        for (int i = 0; i < expected.size(); i++) {
            assertThat("expression " + i, expressions[i].toString(), equalTo(expected.get(i)));
        }
        assertThat("First expression is a sequence", expressions[0], instanceOf(SpecialSequence.class));
    }

    /**
     * Test a simple two argument function
     */
    public void testStIntersects() {
        var definition = registry.resolveFunction("st_intersects");
        var rails = RailRoadDiagram.svgSequence(definition);
        var expressions = rails.getExpressions();
        var expected = List.of("(? ST_INTERSECTS ?)", "'('", "'geomA'", "','", "'geomB'", "')'");
        assertThat("Expression count", expressions.length, equalTo(expected.size()));
        for (int i = 0; i < expected.size(); i++) {
            assertThat("expression " + i, expressions[i].toString(), equalTo(expected.get(i)));
        }
        assertThat("First expression is a sequence", expressions[0], instanceOf(SpecialSequence.class));
    }

    /**
     * Test a three argument function with the last argument optional
     */
    public void testMvSlice() {
        var definition = registry.resolveFunction("mv_slice");
        var rails = RailRoadDiagram.svgSequence(definition);
        var expressions = rails.getExpressions();
        var expected = List.of("(? MV_SLICE ?)", "'('", "'field'", "','", "'start'", "[ ',' 'end' ]", "')'");
        assertThat("Expression count", expressions.length, equalTo(expected.size()));
        for (int i = 0; i < expected.size(); i++) {
            assertThat("expression " + i, expressions[i].toString(), equalTo(expected.get(i)));
        }
        assertThat("First expression is a sequence", expressions[0], instanceOf(SpecialSequence.class));
        assertThat("Sixth expression is a repetition", expressions[5], instanceOf(Repetition.class));
        var repetition = (Repetition) expressions[5];
        assertThat("Repetition min", repetition.getMinRepetitionCount(), equalTo(0));
        assertThat("Repetition max", repetition.getMaxRepetitionCount(), equalTo(1));
        var inner = repetition.getExpression();
        assertThat("Repetition inner is a sequence", inner, instanceOf(Sequence.class));
        var innerSeq = (Sequence) inner;
        var innerExpressions = innerSeq.getExpressions();
        assertThat("Repetition inner expression count", innerExpressions.length, equalTo(2));
        assertThat("Repetition inner expression 0", innerExpressions[0].toString(), equalTo("','"));
        assertThat("Repetition inner expression 1", innerExpressions[1].toString(), equalTo("'end'"));
    }

    /**
     * Test a four argument function with the last two arguments optional
     */
    public void testBucket() {
        var definition = registry.resolveFunction("bucket");
        var rails = RailRoadDiagram.svgSequence(definition);
        var expressions = rails.getExpressions();
        var expected = List.of("(? BUCKET ?)", "'('", "'field'", "','", "'buckets'", "[ ',' 'from' ',' 'to' ]", "')'");
        assertThat("Expression count", expressions.length, equalTo(expected.size()));
        for (int i = 0; i < expected.size(); i++) {
            assertThat("expression " + i, expressions[i].toString(), equalTo(expected.get(i)));
        }
        assertThat("First expression is a sequence", expressions[0], instanceOf(SpecialSequence.class));
        assertThat("Sixth expression is a repetition", expressions[5], instanceOf(Repetition.class));
        var repetition = (Repetition) expressions[5];
        assertThat("Repetition min", repetition.getMinRepetitionCount(), equalTo(0));
        assertThat("Repetition max", repetition.getMaxRepetitionCount(), equalTo(1));
        var inner = repetition.getExpression();
        assertThat("Repetition inner is a sequence", inner, instanceOf(Sequence.class));
        var innerSeq = (Sequence) inner;
        var innerExpressions = innerSeq.getExpressions();
        var innerExpected = List.of("','", "'from'", "','", "'to'");
        assertThat("Repetition inner expression count", innerExpressions.length, equalTo(innerExpected.size()));
        for (int i = 0; i < innerExpected.size(); i++) {
            assertThat("Repetition inner expression " + i, innerExpressions[i].toString(), equalTo(innerExpected.get(i)));
        }
    }

    /**
     * Test a two argument function with the first argument optional
     */
    public void testDateFormat() {
        var definition = registry.resolveFunction("date_format");
        var rails = RailRoadDiagram.svgSequence(definition);
        var expressions = rails.getExpressions();
        var expected = List.of("(? DATE_FORMAT ?)", "'('", "[ 'dateFormat' ',' ]", "'date'", "')'");
        assertThat("Expression count", expressions.length, equalTo(expected.size()));
        for (int i = 0; i < expected.size(); i++) {
            assertThat("expression " + i, expressions[i].toString(), equalTo(expected.get(i)));
        }
        assertThat("First expression is a sequence", expressions[0], instanceOf(SpecialSequence.class));
        assertThat("Third expression is a repetition", expressions[2], instanceOf(Repetition.class));
        var repetition = (Repetition) expressions[2];
        assertThat("Repetition min", repetition.getMinRepetitionCount(), equalTo(0));
        assertThat("Repetition max", repetition.getMaxRepetitionCount(), equalTo(1));
        var inner = repetition.getExpression();
        assertThat("Repetition inner is a sequence", inner, instanceOf(Sequence.class));
        var innerSeq = (Sequence) inner;
        var innerExpressions = innerSeq.getExpressions();
        var innerExpected = List.of("'dateFormat'", "','");
        assertThat("Repetition inner expression count", innerExpressions.length, equalTo(innerExpected.size()));
        for (int i = 0; i < innerExpected.size(); i++) {
            assertThat("Repetition inner expression " + i, innerExpressions[i].toString(), equalTo(innerExpected.get(i)));
        }
    }

    public void testCoalesce() {
        var definition = registry.resolveFunction("coalesce");
        var rails = RailRoadDiagram.svgSequence(definition);
        var expressions = rails.getExpressions();
        var strings = Arrays.stream(expressions).map(Object::toString).toList();
        var expected = List.of("(? COALESCE ?)", "'('", "'first'", "{ ',' 'rest' }", "')'");
        assertThat("Coalesce expression count", expressions.length, equalTo(expected.size()));
        for (int i = 0; i < expected.size(); i++) {
            assertThat("expression " + i, expressions[i].toString(), equalTo(expected.get(i)));
        }
        assertThat("First expression is a sequence", expressions[0], instanceOf(SpecialSequence.class));
        assertThat("Fourth expression is a repetition", expressions[3], instanceOf(Repetition.class));
        var repetition = (Repetition) expressions[3];
        assertThat("Repetition min", repetition.getMinRepetitionCount(), equalTo(0));
        assertThat("Repetition max", repetition.getMaxRepetitionCount(), equalTo(null));
        var inner = repetition.getExpression();
        assertThat("Repetition inner is a sequence", inner, instanceOf(Sequence.class));
        var innerSeq = (Sequence) inner;
        var innerExpressions = innerSeq.getExpressions();
        assertThat("Repetition inner expression count", innerExpressions.length, equalTo(2));
        assertThat("Repetition inner expression 0", innerExpressions[0].toString(), equalTo("','"));
        assertThat("Repetition inner expression 1", innerExpressions[1].toString(), equalTo("'rest'"));
    }

}
