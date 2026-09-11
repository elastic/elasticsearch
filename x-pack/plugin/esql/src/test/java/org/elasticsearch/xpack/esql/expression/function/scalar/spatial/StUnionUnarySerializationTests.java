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

public class StUnionUnarySerializationTests extends AbstractExpressionSerializationTests<StUnionUnary> {
    @Override
    protected StUnionUnary createTestInstance() {
        Source source = randomSource();
        Expression left = randomChild();
        return new StUnionUnary(source, left);
    }

    @Override
    protected StUnionUnary mutateInstance(StUnionUnary instance) throws IOException {
        Source source = instance.source();
        Expression left = instance.spatialField();
        left = randomValueOtherThan(left, AbstractExpressionSerializationTests::randomChild);
        return new StUnionUnary(source, left);
    }
}
