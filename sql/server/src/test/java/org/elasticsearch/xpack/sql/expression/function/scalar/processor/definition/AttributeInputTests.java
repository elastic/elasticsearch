/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.querydsl.container.ColumnReference;

import static org.mockito.Mockito.mock;

public class AttributeInputTests extends ESTestCase {
    public void testResolveAttributes() {
        ColumnReference column = mock(ColumnReference.class);
        Expression expression = mock(Expression.class);
        Attribute attribute = mock(Attribute.class);

        ReferenceInput expected = new ReferenceInput(expression, column);

        assertEquals(expected, new AttributeInput(expression, attribute).resolveAttributes(a -> {
            assertSame(attribute, a);
            return column;
        }));
    }
}
