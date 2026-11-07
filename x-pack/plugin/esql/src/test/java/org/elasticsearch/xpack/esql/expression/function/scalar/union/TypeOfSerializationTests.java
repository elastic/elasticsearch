/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.union;

import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class TypeOfSerializationTests extends AbstractExpressionSerializationTests<TypeOf> {
    @Override
    protected TypeOf createTestInstance() {
        return new TypeOf(randomSource(), randomChild());
    }

    @Override
    protected TypeOf mutateInstance(TypeOf instance) throws IOException {
        return new TypeOf(instance.source(), randomValueOtherThan(instance.field(), AbstractExpressionSerializationTests::randomChild));
    }
}
