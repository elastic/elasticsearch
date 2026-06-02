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

public class RateSerializationTests extends AbstractExpressionSerializationTests<Rate> {
    @Override
    protected Rate createTestInstance() {
        Source source = randomSource();
        Expression field = randomChild();
        Expression filter = randomChild();
        Expression window = randomChild();
        Expression timestamp = randomChild();
        Expression temporality = randomChild();
        return new Rate(source, field, filter, window, timestamp, temporality);
    }

    @Override
    protected Rate mutateInstance(Rate instance) throws IOException {
        Source source = randomSource();
        Expression field = instance.field();
        Expression timestamp = instance.timestamp();
        Expression filter = instance.filter();
        Expression window = instance.window();
        Expression temporality = instance.temporality();
        switch (between(0, 4)) {
            case 0 -> field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
            case 1 -> timestamp = randomValueOtherThan(timestamp, AbstractExpressionSerializationTests::randomChild);
            case 2 -> filter = randomValueOtherThan(filter, AbstractExpressionSerializationTests::randomChild);
            case 3 -> window = randomValueOtherThan(window, AbstractExpressionSerializationTests::randomChild);
            case 4 -> temporality = randomValueOtherThan(temporality, AbstractExpressionSerializationTests::randomChild);
            default -> throw new AssertionError("unexpected value");
        }
        return new Rate(source, field, filter, window, timestamp, temporality);
    }

}
