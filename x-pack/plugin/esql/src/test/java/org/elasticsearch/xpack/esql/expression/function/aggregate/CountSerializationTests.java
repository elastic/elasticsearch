/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class CountSerializationTests extends AbstractExpressionSerializationTests<Count> {
    @Override
    protected Count createTestInstance() {
        return new Count(randomSource(), randomChild());
    }

    @Override
    protected Count mutateInstance(Count instance) throws IOException {
        return new Count(instance.source(), randomValueOtherThan(instance.field(), AbstractExpressionSerializationTests::randomChild));
    }
}
