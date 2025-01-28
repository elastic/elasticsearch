/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;

public class SpatialDisjointSerializationTests extends AbstractBinarySpatialFunctionSerializationTestCase<SpatialDisjoint> {
    @Override
    protected SpatialDisjoint build(Source source, Expression left, Expression right) {
        return new SpatialDisjoint(source, left, right);
    }
}
