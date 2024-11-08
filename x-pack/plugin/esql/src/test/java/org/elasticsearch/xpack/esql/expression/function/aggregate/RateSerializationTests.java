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
        Expression timestamp = randomChild();
        Expression unit = randomBoolean() ? null : randomChild();
        return new Rate(source, field, timestamp, unit);
    }

    @Override
    protected Rate mutateInstance(Rate instance) throws IOException {
        Source source = randomSource();
        Expression field = instance.field();
        Expression timestamp = instance.timestamp();
        Expression unit = instance.unit();
        switch (between(0, 2)) {
            case 0 -> field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
            case 1 -> timestamp = randomValueOtherThan(timestamp, AbstractExpressionSerializationTests::randomChild);
            case 2 -> unit = randomValueOtherThan(unit, () -> randomBoolean() ? null : randomChild());
        }
        return new Rate(source, field, timestamp, unit);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
