/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class SparklineSerializationTests extends AbstractExpressionSerializationTests<Sparkline> {
    @Override
    protected Sparkline createTestInstance() {
        return new Sparkline(randomSource(), randomChild(), randomChild());
    }

    @Override
    protected Sparkline mutateInstance(Sparkline instance) throws IOException {
        return new Sparkline(
            instance.source(),
            randomValueOtherThan(instance.field(), AbstractExpressionSerializationTests::randomChild),
            randomValueOtherThan(instance.sortField(), AbstractExpressionSerializationTests::randomChild)
        );
    }
}
