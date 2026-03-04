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

import java.io.IOException;

public class StGeotileSerializationTests extends AbstractExpressionSerializationTests<StGeotile> {
    @Override
    protected StGeotile createTestInstance() {
        Source source = randomSource();
        Expression field = randomChild();
        Expression precision = randomChild();
        Expression bounds = randomBoolean() ? null : randomChild();
        return new StGeotile(source, field, precision, bounds);
    }

    @Override
    protected StGeotile mutateInstance(StGeotile instance) throws IOException {
        Source source = instance.source();
        Expression field = instance.spatialField();
        Expression precision = instance.parameter();
        Expression bounds = instance.bounds();
        switch (between(0, 2)) {
            case 0 -> field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
            case 1 -> precision = randomValueOtherThan(precision, AbstractExpressionSerializationTests::randomChild);
            case 2 -> bounds = randomValueOtherThan(bounds, () -> randomBoolean() ? null : randomChild());
        }
        return new StGeotile(source, field, precision, bounds);
    }
}
