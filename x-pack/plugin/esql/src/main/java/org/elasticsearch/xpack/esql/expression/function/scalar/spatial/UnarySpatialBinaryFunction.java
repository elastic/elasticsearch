/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.Objects;

/**
 * Spatial functions that take one spatial argument can inherit from this class.
 */
public abstract class UnarySpatialBinaryFunction extends BinaryScalarFunction {
    protected final boolean spatialDocsValues;

    protected UnarySpatialBinaryFunction(Source source, Expression left, Expression right, boolean spatialDocsValues) {
        super(source, left, right);
        this.spatialDocsValues = spatialDocsValues;
    }

    protected UnarySpatialBinaryFunction(StreamInput in, boolean spatialDocsValues) throws IOException {
        super(in);
        this.spatialDocsValues = spatialDocsValues;
    }

    /**
     * Mark the function as expecting the specified fields to arrive as doc-values.
     */
    public abstract UnarySpatialBinaryFunction withDocValues(boolean useDocValues);

    @Override
    public int hashCode() {
        // NB: the hashcode is currently used for key generation so
        // to avoid clashes between aggs with the same arguments, add the class name as variation
        return Objects.hash(getClass(), children(), spatialDocsValues);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            UnarySpatialBinaryFunction other = (UnarySpatialBinaryFunction) obj;
            return Objects.equals(other.children(), children()) && Objects.equals(other.spatialDocsValues, spatialDocsValues);
        }
        return false;
    }

    public boolean spatialDocsValues() {
        return spatialDocsValues;
    }
}
