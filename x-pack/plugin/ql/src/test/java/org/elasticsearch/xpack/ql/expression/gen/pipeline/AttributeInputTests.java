/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression.gen.pipeline;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.execution.search.FieldExtraction;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;

import static org.mockito.Mockito.mock;

public class AttributeInputTests extends ESTestCase {
    public void testResolveAttributes() {
        FieldExtraction column = mock(FieldExtraction.class);
        Expression expression = mock(Expression.class);
        Attribute attribute = mock(Attribute.class);

        ReferenceInput expected = new ReferenceInput(expression.source(), expression, column);

        assertEquals(expected, new AttributeInput(expression.source(), expression, attribute).resolveAttributes(a -> {
            assertSame(attribute, a);
            return column;
        }));
    }
}
