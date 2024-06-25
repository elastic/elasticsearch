/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;

import java.io.IOException;
import java.util.List;

public class StXSerializationTests extends AbstractExpressionSerializationTests<StX> {
    @Override
    protected List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return UnaryScalarFunction.getNamedWriteables();
    }

    @Override
    protected StX createTestInstance() {
        return new StX(randomSource(), randomChild());
    }

    @Override
    protected StX mutateInstance(StX instance) throws IOException {
        return new StX(instance.source(), randomValueOtherThan(instance.field(), AbstractExpressionSerializationTests::randomChild));
    }
}
