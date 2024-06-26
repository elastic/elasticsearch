/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;
import java.util.List;

public class MaxSerializationTests extends AbstractExpressionSerializationTests<Max> {
    @Override
    protected Max createTestInstance() {
        return new Max(randomSource(), randomChild());
    }

    @Override
    protected Max mutateInstance(Max instance) throws IOException {
        return new Max(instance.source(), randomValueOtherThan(instance.field(), AbstractExpressionSerializationTests::randomChild));
    }

    @Override
    protected List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return AggregateFunction.getNamedWriteables();
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
