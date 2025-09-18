/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class StYSerializationTests extends AbstractExpressionSerializationTests<StY> {
    @Override
    protected StY innerCreateTestInstance() {
        return new StY(randomSource(), randomChild());
    }

    @Override
    protected StY innerMutateInstance(StY instance) throws IOException {
        return new StY(instance.source(), randomValueOtherThan(instance.field(), AbstractExpressionSerializationTests::randomChild));
    }
}
