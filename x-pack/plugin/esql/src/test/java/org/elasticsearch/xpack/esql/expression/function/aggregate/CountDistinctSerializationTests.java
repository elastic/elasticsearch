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

public class CountDistinctSerializationTests extends AbstractExpressionSerializationTests<CountDistinct> {
    @Override
    protected CountDistinct createTestInstance() {
        Source source = randomSource();
        Expression field = randomChild();
        Expression precision = randomBoolean() ? null : randomChild();
        return new CountDistinct(source, field, precision);
    }

    @Override
    protected CountDistinct mutateInstance(CountDistinct instance) throws IOException {
        Source source = randomSource();
        Expression field = instance.field();
        Expression precision = instance.precision();
        if (randomBoolean()) {
            field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
        } else {
            precision = randomValueOtherThan(precision, () -> randomBoolean() ? null : randomChild());
        }
        return new CountDistinct(source, field, precision);
    }
}
