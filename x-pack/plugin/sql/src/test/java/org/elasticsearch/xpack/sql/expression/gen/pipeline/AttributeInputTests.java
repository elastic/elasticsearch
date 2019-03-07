/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.gen.pipeline;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.execution.search.FieldExtraction;
import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.AttributeInput;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.ReferenceInput;

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
