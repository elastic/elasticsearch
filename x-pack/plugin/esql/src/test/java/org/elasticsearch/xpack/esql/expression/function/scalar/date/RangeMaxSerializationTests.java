/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class RangeMaxSerializationTests extends AbstractExpressionSerializationTests<RangeMax> {
    @Override
    protected RangeMax createTestInstance() {
        return new RangeMax(randomSource(), randomChild());
    }

    @Override
    protected RangeMax mutateInstance(RangeMax instance) throws IOException {
        return new RangeMax(instance.source(), randomValueOtherThan(instance.field(), AbstractExpressionSerializationTests::randomChild));
    }
}
