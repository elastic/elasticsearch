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

public class StSimplifyPreserveTopologySerializationTests extends AbstractExpressionSerializationTests<StSimplifyPreserveTopology> {
    @Override
    protected StSimplifyPreserveTopology createTestInstance() {
        Source source = randomSource();
        Expression geometry = randomChild();
        Expression tolerance = randomChild();
        return new StSimplifyPreserveTopology(source, geometry, tolerance);
    }

    @Override
    protected StSimplifyPreserveTopology mutateInstance(StSimplifyPreserveTopology instance) throws IOException {
        Source source = instance.source();
        Expression geometry = instance.spatialField();
        Expression tolerance = instance.tolerance();
        if (randomBoolean()) {
            geometry = randomValueOtherThan(geometry, AbstractUnaryScalarSerializationTests::randomChild);
        } else {
            tolerance = randomValueOtherThan(tolerance, AbstractUnaryScalarSerializationTests::randomChild);
        }
        return new StSimplifyPreserveTopology(source, geometry, tolerance);
    }
}
