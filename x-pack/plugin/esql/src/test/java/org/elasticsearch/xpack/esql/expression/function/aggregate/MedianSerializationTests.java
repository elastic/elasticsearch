/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class MedianSerializationTests extends AbstractExpressionSerializationTests<Median> {
    @Override
    protected Median createTestInstance() {
        return new Median(randomSource(), randomChild());
    }

    @Override
    protected Median mutateInstance(Median instance) throws IOException {
        return new Median(instance.source(), randomValueOtherThan(instance.field(), AbstractExpressionSerializationTests::randomChild));
    }
}
