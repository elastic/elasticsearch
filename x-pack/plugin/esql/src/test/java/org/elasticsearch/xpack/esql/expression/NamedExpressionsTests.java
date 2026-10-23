/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeList;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.instanceOf;

public class NamedExpressionsTests extends ESTestCase {

    public void testMergeOutputAttributesAppends() {
        List<Attribute> child = List.of(attribute("a"), attribute("b"));
        List<NamedExpression> fields = List.of(alias("c"), alias("d"));

        List<Attribute> output = NamedExpressions.mergeOutputAttributes(fields, child);
        assertEquals(expected(fields, child), output);
        assertThat(output, instanceOf(AttributeList.class));
    }

    public void testMergeOutputAttributesShadows() {
        List<Attribute> child = List.of(attribute("a"), attribute("b"), attribute("c"));
        List<NamedExpression> fields = List.of(alias("b"), alias("d"));

        assertEquals(expected(fields, child), NamedExpressions.mergeOutputAttributes(fields, child));
    }

    public void testMergeOutputAttributesKeepsLastOfRepeatedFieldNames() {
        List<Attribute> child = List.of(attribute("a"));
        List<NamedExpression> fields = List.of(alias("b"), alias("c"), alias("b"));

        List<Attribute> output = NamedExpressions.mergeOutputAttributes(fields, child);
        assertEquals(expected(fields, child), output);
        assertThat(output, instanceOf(AttributeList.class));
        assertEquals(List.of("a", "c", "b"), output.stream().map(Attribute::name).toList());
    }

    public void testMergeOutputAttributesWithAttributeListFields() {
        AttributeList child = AttributeList.of(List.of(attribute("a"), attribute("b")));
        AttributeList fields = AttributeList.of(List.of(attribute("c"), attribute("b")));

        List<Attribute> output = NamedExpressions.mergeOutputAttributes(fields, child);
        assertEquals(expected(fields, child), output);
        assertEquals(List.of("a", "c", "b"), output.stream().map(Attribute::name).toList());
    }

    public void testMergeOutputAttributesRepeatedId() {
        Attribute a = attribute("a");
        Attribute renamed = new ReferenceAttribute(Source.EMPTY, null, "renamed", DataType.INTEGER, Nullability.FALSE, a.id(), false);
        List<Attribute> child = List.of(a);
        List<Attribute> fields = List.of(renamed);

        assertEquals(expected(fields, child), NamedExpressions.mergeOutputAttributes(fields, child));
    }

    public void testMergeOutputAttributesDuplicateChild() {
        Attribute a = attribute("a");
        List<Attribute> child = List.of(a, a);
        List<NamedExpression> fields = List.of(alias("b"));

        assertEquals(expected(fields, child), NamedExpressions.mergeOutputAttributes(fields, child));
    }

    public void testMergeOutputAttributesWithoutFieldsOrChild() {
        List<Attribute> child = List.of(attribute("a"), attribute("b"));
        assertEquals(child, NamedExpressions.mergeOutputAttributes(List.of(), child));
        assertEquals(List.of(), NamedExpressions.mergeOutputAttributes(List.of(), List.of()));
        List<NamedExpression> fields = List.of(alias("c"));
        assertEquals(expected(fields, List.of()), NamedExpressions.mergeOutputAttributes(fields, List.of()));
    }

    /**
     * Whatever the mix of shadowed, repeated and duplicated names and ids, the output equals the original algorithm's.
     */
    public void testMergeOutputAttributesMatchesMergeOutputExpressions() {
        for (int run = 0; run < 200; run++) {
            List<Attribute> pool = new ArrayList<>();
            for (int i = 0; i < 6; i++) {
                pool.add(attribute("n" + i));
            }
            List<Attribute> child = randomAttributes(pool, between(0, 12));
            List<Attribute> fields = randomAttributes(pool, between(0, 4));
            assertEquals(expected(fields, child), NamedExpressions.mergeOutputAttributes(fields, child));
        }
    }

    /**
     * A chain of commands that each append a field: every output and output set is correct even though they share storage.
     */
    public void testMergeOutputAttributesChain() {
        int width = between(0, 50);
        int depth = between(1, 50);
        List<Attribute> expected = new ArrayList<>();
        for (int i = 0; i < width; i++) {
            expected.add(attribute("w" + i));
        }
        List<Attribute> output = NamedExpressions.mergeOutputAttributes(List.of(), expected);
        List<List<Attribute>> outputs = new ArrayList<>();
        for (int d = 0; d < depth; d++) {
            Alias field = alias("d" + d);
            output = NamedExpressions.mergeOutputAttributes(List.of(field), output);
            expected.add(field.toAttribute());
            outputs.add(output);
        }
        for (int d = 0; d < depth; d++) {
            List<Attribute> nodeOutput = outputs.get(d);
            assertThat(nodeOutput, instanceOf(AttributeList.class));
            assertEquals(expected.subList(0, width + d + 1), nodeOutput);
            AttributeSet set = AttributeSet.of(nodeOutput);
            assertEquals(width + d + 1, set.size());
            assertTrue(set.containsAll(expected.subList(0, width + d + 1)));
            if (d + 1 < depth) {
                assertFalse(set.contains(expected.get(width + d + 1)));
            }
        }
    }

    /**
     * Shadowing in the middle of a chain starts a new list, and commands after it keep sharing that one.
     */
    public void testMergeOutputAttributesChainWithShadowing() {
        List<Attribute> output = NamedExpressions.mergeOutputAttributes(List.of(alias("a"), alias("b")), List.of());
        List<Attribute> expected = new ArrayList<>(output);
        for (int d = 0; d < 20; d++) {
            Alias field = alias(d % 5 == 0 ? "a" : "d" + d);
            output = NamedExpressions.mergeOutputAttributes(List.of(field), output);
            expected.removeIf(a -> a.name().equals(field.name()));
            expected.add(field.toAttribute());
            assertEquals(expected, output);
            assertThat(output, instanceOf(AttributeList.class));
        }
    }

    private static List<Attribute> expected(List<? extends NamedExpression> fields, List<? extends NamedExpression> child) {
        return Expressions.asAttributes(NamedExpressions.mergeOutputExpressions(fields, child));
    }

    private static List<Attribute> randomAttributes(List<Attribute> pool, int count) {
        List<Attribute> attributes = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Attribute base = randomFrom(pool);
            attributes.add(switch (between(0, 3)) {
                case 0 -> base;
                case 1 -> attribute(base.name());
                case 2 -> new ReferenceAttribute(
                    Source.EMPTY,
                    null,
                    "x" + between(0, 3),
                    DataType.INTEGER,
                    Nullability.FALSE,
                    base.id(),
                    false
                );
                default -> attribute("u" + between(0, 1000));
            });
        }
        return attributes;
    }

    private static Attribute attribute(String name) {
        return new ReferenceAttribute(Source.EMPTY, name, DataType.INTEGER);
    }

    private static Alias alias(String name) {
        return new Alias(Source.EMPTY, name, new Literal(Source.EMPTY, 1, DataType.INTEGER));
    }
}
