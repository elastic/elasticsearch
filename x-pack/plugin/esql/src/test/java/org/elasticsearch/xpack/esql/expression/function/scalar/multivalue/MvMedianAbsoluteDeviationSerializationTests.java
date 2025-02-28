/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class MvMedianAbsoluteDeviationSerializationTests extends AbstractExpressionSerializationTests<MvMedianAbsoluteDeviation> {
    @Override
    protected MvMedianAbsoluteDeviation createTestInstance() {
        return new MvMedianAbsoluteDeviation(randomSource(), randomChild());
    }

    @Override
    protected MvMedianAbsoluteDeviation mutateInstance(MvMedianAbsoluteDeviation instance) throws IOException {
        return new MvMedianAbsoluteDeviation(
            instance.source(),
            randomValueOtherThan(instance.field(), AbstractExpressionSerializationTests::randomChild)
        );
    }
}
