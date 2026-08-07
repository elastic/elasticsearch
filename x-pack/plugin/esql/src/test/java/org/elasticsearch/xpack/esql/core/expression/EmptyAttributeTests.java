/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class EmptyAttributeTests extends ESTestCase {
    public void testToString() {
        EmptyAttribute attr = new EmptyAttribute(Source.EMPTY);
        assertThat(attr.toString(), equalTo("{e}#" + attr.id()));
    }

    public void testEquals() {
        EmptyAttribute a = new EmptyAttribute(Source.EMPTY);
        EmptyAttribute b = new EmptyAttribute(Source.EMPTY);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    public void testDataType() {
        assertEquals(DataType.NULL, new EmptyAttribute(Source.EMPTY).dataType());
    }

    public void testCloneReturnsSelf() {
        EmptyAttribute attr = new EmptyAttribute(Source.EMPTY);
        assertThat(attr.withId(new NameId()), sameInstance(attr));
    }
}
