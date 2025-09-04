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

public class SampleSerializationTests extends AbstractExpressionSerializationTests<Sample> {
    @Override
    protected Sample createTestInstance() {
        Source source = randomSource();
        Expression field = randomChild();
        Expression limit = randomChild();
        return new Sample(source, field, limit);
    }

    @Override
    protected Sample mutateInstance(Sample instance) throws IOException {
        Source source = randomSource();
        Expression field = instance.field();
        Expression limit = instance.limitField();
        switch (between(0, 1)) {
            case 0 -> field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
            case 1 -> limit = randomValueOtherThan(limit, AbstractExpressionSerializationTests::randomChild);
        }
        return new Sample(source, field, limit);
    }
}
