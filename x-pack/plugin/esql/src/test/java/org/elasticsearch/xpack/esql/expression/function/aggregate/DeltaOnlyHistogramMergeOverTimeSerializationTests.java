/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class DeltaOnlyHistogramMergeOverTimeSerializationTests extends AbstractExpressionSerializationTests<
    DeltaOnlyHistogramMergeOverTime> {
    @Override
    protected DeltaOnlyHistogramMergeOverTime createTestInstance() {
        Source source = randomSource();
        Expression field = randomChild();
        Expression filter = randomChild();
        Expression window = randomChild();
        Expression temporality = randomBoolean() ? randomChild() : null;
        return new DeltaOnlyHistogramMergeOverTime(source, field, filter, window, temporality);
    }

    @Override
    protected DeltaOnlyHistogramMergeOverTime mutateInstance(DeltaOnlyHistogramMergeOverTime instance) throws IOException {
        Source source = randomSource();
        Expression field = instance.field();
        Expression filter = instance.filter();
        Expression window = instance.window();
        Expression temporality = instance.temporality();
        switch (between(0, 3)) {
            case 0 -> field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
            case 1 -> filter = randomValueOtherThan(filter, AbstractExpressionSerializationTests::randomChild);
            case 2 -> window = randomValueOtherThan(window, AbstractExpressionSerializationTests::randomChild);
            case 3 -> temporality = randomValueOtherThan(temporality, AbstractExpressionSerializationTests::randomChild);
        }
        return new DeltaOnlyHistogramMergeOverTime(source, field, filter, window, temporality);
    }
}
