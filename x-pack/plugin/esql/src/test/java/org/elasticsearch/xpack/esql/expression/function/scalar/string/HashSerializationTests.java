/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class HashSerializationTests extends AbstractExpressionSerializationTests<Hash> {

    @Override
    protected Hash createTestInstance() {
        return new Hash(randomSource(), randomChild(), randomChild());
    }

    @Override
    protected Hash mutateInstance(Hash instance) throws IOException {
        return randomBoolean()
            ? new Hash(instance.source(), mutateExpression(instance.algorithm()), instance.input())
            : new Hash(instance.source(), instance.algorithm(), mutateExpression(instance.input()));
    }
}
