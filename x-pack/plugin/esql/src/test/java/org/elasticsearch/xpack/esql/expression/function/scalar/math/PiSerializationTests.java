/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;

import java.io.IOException;
import java.util.List;

public class PiSerializationTests extends AbstractExpressionSerializationTests<Pi> {
    @Override
    protected List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return EsqlScalarFunction.getNamedWriteables();
    }

    @Override
    protected Pi createTestInstance() {
        return new Pi(randomSource());
    }

    @Override
    protected Pi mutateInstance(Pi instance) throws IOException {
        return null;
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
