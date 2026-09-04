/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.approximate;

import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class RandomSerializationTests extends AbstractExpressionSerializationTests<Random> {

    @Override
    protected Random createTestInstance() {
        return new Random(randomSource(), randomChild());
    }

    @Override
    protected Random mutateInstance(Random instance) throws IOException {
        return new Random(instance.source(), randomValueOtherThan(instance.field(), AbstractExpressionSerializationTests::randomChild));
    }
}
