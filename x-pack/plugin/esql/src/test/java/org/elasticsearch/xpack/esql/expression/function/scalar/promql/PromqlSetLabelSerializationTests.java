/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.promql;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class PromqlSetLabelSerializationTests extends AbstractExpressionSerializationTests<PromqlSetLabel> {
    @Override
    protected PromqlSetLabel createTestInstance() {
        return new PromqlSetLabel(randomSource(), randomChild(), randomChild(), randomChild());
    }

    @Override
    protected PromqlSetLabel mutateInstance(PromqlSetLabel instance) throws IOException {
        Source source = instance.source();
        Expression timeseries = instance.children().get(0);
        Expression value = instance.children().get(1);
        Expression dstName = instance.children().get(2);
        switch (between(0, 2)) {
            case 0 -> timeseries = randomValueOtherThan(timeseries, AbstractExpressionSerializationTests::randomChild);
            case 1 -> value = randomValueOtherThan(value, AbstractExpressionSerializationTests::randomChild);
            case 2 -> dstName = randomValueOtherThan(dstName, AbstractExpressionSerializationTests::randomChild);
        }
        return new PromqlSetLabel(source, timeseries, value, dstName);
    }
}
