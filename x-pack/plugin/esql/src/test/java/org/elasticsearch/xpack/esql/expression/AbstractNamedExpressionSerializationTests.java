/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression;

import org.elasticsearch.xpack.esql.core.expression.NamedExpression;

public abstract class AbstractNamedExpressionSerializationTests<T extends NamedExpression> extends AbstractExpressionSerializationTests<T> {
    public void testEqualsAndHashCodeIgnoringId() throws Exception {
        T instance = createTestInstance();
        T withNewId = mutateNameId(instance);

        assertTrue(instance.equals(withNewId, true));
        assertEquals(instance.hashCode(true), withNewId.hashCode(true));

        assertEquals(instance.equals(withNewId), instance.equals(withNewId, false));
    }

    public void testEqualsAndHashCodeWithId() throws Exception {
        T instance = createTestInstance();
        T withNewId = mutateNameId(instance);

        if (equalityIgnoresId()) {
            assertEquals(instance, withNewId);
            assertEquals(instance.hashCode(), withNewId.hashCode());
        } else {
            assertNotEquals(instance, withNewId);
        }
    }

    protected abstract T mutateNameId(T instance);

    protected abstract boolean equalityIgnoresId();
}
