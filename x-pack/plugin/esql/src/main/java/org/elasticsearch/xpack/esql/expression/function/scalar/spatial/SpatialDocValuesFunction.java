/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;

import java.util.List;
import java.util.Objects;

/**
 * Spatial functions that take one spatial argument, one parameter and one optional bounds can inherit from this class.
 * Obvious choices are: StGeohash, StGeotile and StGeohex.
 */
public abstract class SpatialDocValuesFunction extends EsqlScalarFunction {
    protected final boolean spatialDocValues;

    protected SpatialDocValuesFunction(Source source, List<Expression> expressions, boolean spatialDocValues) {
        super(source, expressions);
        this.spatialDocValues = spatialDocValues;
    }

    /**
     * Mark the function as expecting the specified field to arrive as doc-values.
     * This only applies to geo_point and cartesian_point types.
     */
    public abstract SpatialDocValuesFunction withDocValues(boolean useDocValues);

    @Override
    public int hashCode() {
        // NB: the hashcode is currently used for key generation, so to avoid clashes
        // between aggs with the same arguments, add the class name as variation
        return Objects.hash(getClass(), children(), spatialDocValues);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            SpatialDocValuesFunction other = (SpatialDocValuesFunction) obj;
            return Objects.equals(other.children(), children()) && Objects.equals(other.spatialDocValues, spatialDocValues);
        }
        return false;
    }

    public abstract Expression spatialField();

    public boolean spatialDocValues() {
        return spatialDocValues;
    }
}
