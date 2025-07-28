/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class TauSerializationTests extends AbstractExpressionSerializationTests<Tau> {
    @Override
    protected Tau createTestInstance() {
        return new Tau(randomSource());
    }

    @Override
    protected Tau mutateInstance(Tau instance) throws IOException {
        return null;
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
