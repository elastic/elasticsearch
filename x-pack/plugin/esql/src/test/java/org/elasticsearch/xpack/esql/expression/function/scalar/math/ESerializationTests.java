/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class ESerializationTests extends AbstractExpressionSerializationTests<E> {
    @Override
    protected E createTestInstance() {
        return new E(randomSource());
    }

    @Override
    protected E mutateInstance(E instance) throws IOException {
        return null;
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
