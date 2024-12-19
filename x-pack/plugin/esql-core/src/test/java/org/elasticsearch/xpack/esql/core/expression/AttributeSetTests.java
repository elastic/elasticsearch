/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.AttributeMapTests.a;

public class AttributeSetTests extends ESTestCase {

    public void testEquals() {
        Attribute a1 = a("1");
        Attribute a2 = a("2");

        AttributeSet first = new AttributeSet(List.of(a1, a2));
        assertEquals(first, first);

        AttributeSet second = new AttributeSet();
        second.add(a1);
        second.add(a2);

        assertEquals(first, second);
        assertEquals(second, first);

        AttributeSet third = new AttributeSet();
        third.add(a("1"));
        third.add(a("2"));

        assertNotEquals(first, third);
        assertNotEquals(third, first);

        assertEquals(AttributeSet.EMPTY, AttributeSet.EMPTY);
        assertEquals(AttributeSet.EMPTY, first.intersect(third));
        assertEquals(third.intersect(first), AttributeSet.EMPTY);
    }
}
