/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.PlanStreamInput;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Spatial functions that take one spatial argument, one parameter and one optional bounds can inherit from this class.
 * Obvious choices are: StGeohash, StGeotile and StGeohex.
 */
public abstract class SpatialGridFunction extends ScalarFunction implements OptionalArgument {
    protected final Expression spatialField;
    protected final Expression parameter;
    protected final Expression bounds;
    protected final boolean spatialDocsValues;

    protected SpatialGridFunction(
        Source source,
        Expression spatialField,
        Expression parameter,
        Expression bounds,
        boolean spatialDocsValues
    ) {
        super(source, bounds == null ? Arrays.asList(spatialField, parameter) : Arrays.asList(spatialField, parameter, bounds));
        this.spatialField = spatialField;
        this.parameter = parameter;
        this.bounds = bounds;
        this.spatialDocsValues = spatialDocsValues;
    }

    protected SpatialGridFunction(StreamInput in, boolean spatialDocsValues) throws IOException {
        this(
            Source.readFrom((StreamInput & PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readOptionalNamedWriteable(Expression.class),
            spatialDocsValues
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(spatialField);
        out.writeNamedWriteable(parameter);
        out.writeOptionalNamedWriteable(bounds);
    }

    /**
     * Mark the function as expecting the specified field to arrive as doc-values.
     * This only applies to geo_point and cartesian_point types.
     */
    public abstract SpatialGridFunction withDocValues(boolean useDocValues);

    @Override
    public int hashCode() {
        // NB: the hashcode is currently used for key generation so
        // to avoid clashes between aggs with the same arguments, add the class name as variation
        return Objects.hash(getClass(), children(), spatialDocsValues);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            SpatialGridFunction other = (SpatialGridFunction) obj;
            return Objects.equals(other.children(), children()) && Objects.equals(other.spatialDocsValues, spatialDocsValues);
        }
        return false;
    }

    public boolean spatialDocsValues() {
        return spatialDocsValues;
    }

    @Override
    public final SpatialGridFunction replaceChildren(List<Expression> newChildren) {
        Expression newSpatialField = newChildren.get(0);
        Expression newParameter = newChildren.get(1);
        Expression newBounds = newChildren.size() > 2 ? newChildren.get(2) : null;

        return spatialField.equals(newSpatialField)
            && parameter.equals(newParameter)
            && (bounds == null && newBounds == null || bounds != null && bounds.equals(newBounds))
                ? this
                : replaceChildren(newSpatialField, newParameter, newBounds);
    }

    protected abstract SpatialGridFunction replaceChildren(Expression newSpatialField, Expression newParameter, Expression newBounds);

    public Expression spatialField() {
        return spatialField;
    }

    public Expression parameter() {
        return parameter;
    }

    public Expression bounds() {
        return bounds;
    }

    @Override
    public boolean foldable() {
        return spatialField.foldable() && parameter.foldable() && (bounds == null || bounds.foldable());
    }
}
