/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

/** {@code NESTED_OBJECT} only names a collection during analysis, so nothing about it is evaluated or sent between nodes. */
public class NestedObjectReferenceTests extends ESTestCase {
    private static NestedObject object() {
        return new NestedObject(Source.EMPTY, new UnresolvedAttribute(Source.EMPTY, "items"));
    }

    public void testIsAnObjectNamedByItsCollection() {
        NestedObject object = object();
        assertThat(object.dataType(), equalTo(DataType.OBJECT));
        assertThat(((UnresolvedAttribute) object.collection()).name(), equalTo("items"));
    }

    public void testCannotBeWritten() {
        expectThrows(UnsupportedOperationException.class, () -> object().writeTo(null));
        expectThrows(UnsupportedOperationException.class, () -> object().getWriteableName());
    }

    public void testReplaceChildrenKeepsTheCollection() {
        NestedObject object = object();
        UnresolvedAttribute other = new UnresolvedAttribute(Source.EMPTY, "events");
        NestedObject replaced = object.replaceChildren(List.of(other));
        assertThat(replaced.collection(), sameInstance(other));
    }
}
