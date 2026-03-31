/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class MvPSeriesWeightedSumSerializationTests extends AbstractExpressionSerializationTests<MvPSeriesWeightedSum> {
    @Override
    protected MvPSeriesWeightedSum createTestInstance() {
        Source source = randomSource();
        Expression field = randomChild();
        Expression p = randomChild();

        return new MvPSeriesWeightedSum(source, field, p);
    }

    @Override
    protected MvPSeriesWeightedSum mutateInstance(MvPSeriesWeightedSum instance) throws IOException {
        Source source = instance.source();
        Expression field = instance.field();
        Expression p = instance.p();

        switch (between(0, 1)) {
            case 0 -> field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
            case 1 -> p = randomValueOtherThan(p, AbstractExpressionSerializationTests::randomChild);

        }
        return new MvPSeriesWeightedSum(source, field, p);
    }
}
