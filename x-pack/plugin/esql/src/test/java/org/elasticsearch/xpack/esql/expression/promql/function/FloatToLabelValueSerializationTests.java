/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.function;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class FloatToLabelValueSerializationTests extends AbstractExpressionSerializationTests<FloatToLabelValue> {
    @Override
    protected FloatToLabelValue createTestInstance() {
        return new FloatToLabelValue(randomSource(), randomChild());
    }

    @Override
    protected FloatToLabelValue mutateInstance(FloatToLabelValue instance) throws IOException {
        Expression value = randomValueOtherThan(instance.children().getFirst(), AbstractExpressionSerializationTests::randomChild);
        return new FloatToLabelValue(instance.source(), value);
    }
}
