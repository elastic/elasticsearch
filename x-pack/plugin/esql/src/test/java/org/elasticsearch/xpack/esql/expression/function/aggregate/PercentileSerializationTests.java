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

public class PercentileSerializationTests extends AbstractExpressionSerializationTests<Percentile> {
    @Override
    protected Percentile createTestInstance() {
        Source source = randomSource();
        Expression field = randomChild();
        Expression percentile = randomChild();
        return new Percentile(source, field, percentile);
    }

    @Override
    protected Percentile mutateInstance(Percentile instance) throws IOException {
        Source source = instance.source();
        Expression field = instance.field();
        Expression percentile = instance.percentile();
        if (randomBoolean()) {
            field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
        } else {
            percentile = randomValueOtherThan(percentile, AbstractExpressionSerializationTests::randomChild);
        }
        return new Percentile(source, field, percentile);
    }
}
