/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.util.List;

public class JsonStringSerializationTests extends AbstractExpressionSerializationTests<JsonString> {
    @Override
    protected JsonString createTestInstance() {
        return new JsonString(randomSource(), randomList(1, 10, AbstractExpressionSerializationTests::randomChild));
    }

    @Override
    protected JsonString mutateInstance(JsonString instance) {
        List<Expression> children = randomValueOtherThan(
            instance.children(),
            () -> randomList(1, 10, AbstractExpressionSerializationTests::randomChild)
        );
        return new JsonString(instance.source(), children);
    }
}
