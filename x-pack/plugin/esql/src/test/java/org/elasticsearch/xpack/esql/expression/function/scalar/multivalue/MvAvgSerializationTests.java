/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class MvAvgSerializationTests extends AbstractExpressionSerializationTests<MvAvg> {
    @Override
    protected MvAvg createTestInstance() {
        return new MvAvg(randomSource(), randomChild());
    }

    @Override
    protected MvAvg mutateInstance(MvAvg instance) throws IOException {
        return new MvAvg(instance.source(), randomValueOtherThan(instance.field(), AbstractExpressionSerializationTests::randomChild));
    }
}
