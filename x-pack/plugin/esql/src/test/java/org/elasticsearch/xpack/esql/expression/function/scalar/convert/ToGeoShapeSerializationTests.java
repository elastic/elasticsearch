/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractUnaryScalarSerializationTests;

public class ToGeoShapeSerializationTests extends AbstractUnaryScalarSerializationTests<ToGeoShape> {
    @Override
    protected ToGeoShape create(Source source, Expression child) {
        return new ToGeoShape(source, child);
    }
}
