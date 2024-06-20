/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.TernaryScalarFunction;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

/**
 * Spatial functions that take two arguments that must both be spatial types can inherit from this class.
 * This provides common support for type resolution and validation. Ensuring that both arguments are spatial types
 * and of compatible CRS. For example geo_point and geo_shape can be compared, but not geo_point and cartesian_point.
 */
public abstract class TernarySpatialFunction extends TernaryScalarFunction implements SpatialEvaluatorFactory.SpatialSourceResolution {
    private final BinarySpatialFunction.SpatialTypeResolver spatialTypeResolver;
    protected BinarySpatialFunction.SpatialCrsType crsType;
    protected final boolean leftDocValues;
    protected final boolean rightDocValues;

    protected TernarySpatialFunction(
        Source source,
        Expression left,
        Expression right,
        Expression arg,
        boolean leftDocValues,
        boolean rightDocValues,
        boolean pointsOnly
    ) {
        super(source, left, right, arg);
        this.leftDocValues = leftDocValues;
        this.rightDocValues = rightDocValues;
        this.spatialTypeResolver = new BinarySpatialFunction.SpatialTypeResolver(this, pointsOnly);
    }

    @Override
    protected Expression.TypeResolution resolveType() {
        var spatialResolution = spatialTypeResolver.resolveType();
        if (spatialResolution.unresolved()) {
            return spatialResolution;
        }
        return resolveArgType();
    }

    protected abstract Expression.TypeResolution resolveArgType();

    @Override
    public void setCrsType(DataType dataType) {
        crsType = BinarySpatialFunction.SpatialCrsType.fromDataType(dataType);
    }

    @Override
    public BinarySpatialFunction.SpatialCrsType crsType() {
        if (crsType == null) {
            resolveType();
        }
        return crsType;
    }

    public boolean leftDocValues() {
        return leftDocValues;
    }

    public boolean rightDocValues() {
        return rightDocValues;
    }
}
