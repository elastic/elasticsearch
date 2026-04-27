/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class RangeMinSerializationTests extends AbstractExpressionSerializationTests<RangeMin> {
    @Override
    protected RangeMin createTestInstance() {
        return new RangeMin(randomSource(), randomChild());
    }

    @Override
    protected RangeMin mutateInstance(RangeMin instance) throws IOException {
        return new RangeMin(instance.source(), randomValueOtherThan(instance.field(), AbstractExpressionSerializationTests::randomChild));
    }
}
