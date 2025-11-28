/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class HistogramMergeSerializationTests extends AbstractExpressionSerializationTests<HistogramMerge> {
    @Override
    protected HistogramMerge createTestInstance() {
        return new HistogramMerge(randomSource(), randomChild());
    }

    @Override
    protected HistogramMerge mutateInstance(HistogramMerge instance) throws IOException {
        return new HistogramMerge(
            instance.source(),
            randomValueOtherThan(instance.field(), AbstractExpressionSerializationTests::randomChild)
        );
    }
}
