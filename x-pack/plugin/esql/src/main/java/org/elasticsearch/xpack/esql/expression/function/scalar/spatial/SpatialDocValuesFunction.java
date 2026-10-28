/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.List;
import java.util.Objects;

/**
 * Spatial functions that can take doc values as an argument can inherit from this class.
 * Examples: StGeohash, StGeotile, StGeohex and StSimplify
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

    @Override
    public boolean foldable() {
        return children().stream().allMatch(Expression::foldable);
    }

    public abstract Expression spatialField();

    public boolean spatialDocValues() {
        return spatialDocValues;
    }

    /**
     * Indicates that this function prefers to have its spatial argument extracted from doc values.
     * This is a hint to the planner that can be ignored if necessary. If a function does not
     * prefer doc values extraction, it should override this method to return false. Examples of
     * such functions are ST_X and ST_Y that should provide exact values from source.
     * Note that if the original field is not dropped from the output, this request will be ignored.
     */
    public boolean prefersDocValuesExtraction() {
        return true;
    }

    /**
     * Returns the {@code geo_shape} or {@code cartesian_shape} field that a function transforming {@code geometry} can be
     * fused into loading, or {@code null} if {@code geometry} is not such a field or its doc values cannot reconstruct
     * the original geometry. See {@link org.elasticsearch.xpack.esql.expression.function.blockloader.BlockLoaderExpression}.
     */
    @Nullable
    protected static FieldAttribute fusableShapeField(Expression geometry, SearchStats stats) {
        if (geometry instanceof FieldAttribute field
            && (field.dataType() == DataType.GEO_SHAPE || field.dataType() == DataType.CARTESIAN_SHAPE)
            && stats.hasDocValues(field.fieldName())
            && stats.supportsGeometryDocValueReconstruction(field.fieldName())) {
            return field;
        }
        return null;
    }

    /**
     * Returns the value of a constant numeric argument, or {@code null} if it is not a constant number. Arguments that
     * are not constant numbers keep using the evaluator, so that it reports the error.
     */
    @Nullable
    protected static Double constantNumber(Expression expression) {
        return expression instanceof Literal literal && literal.value() instanceof Number number ? number.doubleValue() : null;
    }
}
