/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;
import org.elasticsearch.xpack.esql.expression.AbstractUnaryScalarSerializationTests;

import java.io.IOException;

public class StGeohashSerializationTests extends AbstractExpressionSerializationTests<StGeohash> {
    @Override
    protected StGeohash createTestInstance() {
        Source source = randomSource();
        Expression field = randomChild();
        Expression precision = randomChild();
        Expression bounds = randomBoolean() ? null : randomChild();
        return new StGeohash(source, field, precision, bounds);
    }

    @Override
    protected StGeohash mutateInstance(StGeohash instance) throws IOException {
        Source source = instance.source();
        Expression field = instance.spatialField();
        Expression precision = instance.parameter();
        Expression bounds = instance.bounds();
        switch (between(0, 2)) {
            case 0 -> field = randomValueOtherThan(field, AbstractUnaryScalarSerializationTests::randomChild);
            case 1 -> precision = randomValueOtherThan(precision, AbstractUnaryScalarSerializationTests::randomChild);
            case 2 -> bounds = randomValueOtherThan(bounds, () -> randomBoolean() ? null : randomChild());
        }
        return new StGeohash(source, field, precision, bounds);
    }
}
