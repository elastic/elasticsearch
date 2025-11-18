/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.xpack.esql.core.expression.AttributeMapTests.a;

public class AttributeSetTests extends ESTestCase {

    public void testEquals() {
        Attribute a1 = a("1");
        Attribute a2 = a("2");

        AttributeSet first = AttributeSet.of(a1, a2);
        assertEquals(first, first);

        var secondBuilder = AttributeSet.builder();
        secondBuilder.add(a1);
        secondBuilder.add(a2);

        var second = secondBuilder.build();
        assertEquals(first, second);
        assertEquals(second, first);

        var thirdBuilder = AttributeSet.builder();
        thirdBuilder.add(a("1"));
        thirdBuilder.add(a("2"));

        AttributeSet third = thirdBuilder.build();
        assertNotEquals(first, third);
        assertNotEquals(third, first);

        assertEquals(AttributeSet.EMPTY, AttributeSet.EMPTY);
        assertEquals(AttributeSet.EMPTY, first.intersect(third));
        assertEquals(third.intersect(first), AttributeSet.EMPTY);
    }

    public void testSetIsImmutable() {
        AttributeSet set = AttributeSet.of(a("1"), a("2"));
        expectThrows(UnsupportedOperationException.class, () -> set.add(a("3")));
        expectThrows(UnsupportedOperationException.class, () -> set.remove(a("1")));
        expectThrows(UnsupportedOperationException.class, () -> set.addAll(set));
        expectThrows(UnsupportedOperationException.class, () -> set.retainAll(set));
        expectThrows(UnsupportedOperationException.class, () -> set.removeAll(set));
        expectThrows(UnsupportedOperationException.class, set::clear);
        expectThrows(UnsupportedOperationException.class, () -> set.removeIf((x -> true)));
    }
}
