/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.ann.ConvertEvaluator;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;
import static org.elasticsearch.xpack.esql.expression.EsqlTypeResolutions.isSpatialPoint;

/**
 * Extracts the {@code x} coordinate from the supplied point.
 * If the point is of type {@code geo_point} this is equivalent to extracting the {@code longitude} value.
 */
public class StX extends SpatialUnaryDocValuesFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "StX", StX::new);
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(StX.class).unary(StX::new).name("st_x");

    @FunctionInfo(
        returnType = "double",
        examples = @Example(file = "spatial", tag = "st_x_y"),
        depthOffset = 1  // So this appears as a subsection of geometry functions
    )
    public StX(
        Source source,
        @Param(
            name = "point",
            type = { "geo_point", "cartesian_point" },
            description = "Expression of type `geo_point` or `cartesian_point`. If `null`, the function returns `null`."
        ) Expression field
    ) {
        this(source, field, false);
    }

    private StX(Source source, Expression field, boolean spatialDocValues) {
        super(source, field, spatialDocValues);
    }

    private StX(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public SpatialDocValuesFunction withDocValues(boolean useDocValues) {
        return new StX(source(), spatialField(), useDocValues);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected Expression.TypeResolution resolveType() {
        return isSpatialPoint(spatialField(), sourceText(), TypeResolutions.ParamOrdinal.DEFAULT);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        if (spatialDocValues) {
            return switch (spatialField().dataType()) {
                case GEO_POINT -> new StXFromGeoDocValuesEvaluator.Factory(source(), toEvaluator.apply(spatialField()));
                case CARTESIAN_POINT -> new StXFromCartesianDocValuesEvaluator.Factory(source(), toEvaluator.apply(spatialField()));
                default -> throw new IllegalArgumentException("Cannot use doc values for type " + spatialField().dataType());
            };
        }
        return switch (spatialField().dataType()) {
            case GEO_POINT -> new StXFromGeoWKBEvaluator.Factory(source(), toEvaluator.apply(spatialField()));
            case CARTESIAN_POINT -> new StXFromCartesianWKBEvaluator.Factory(source(), toEvaluator.apply(spatialField()));
            default -> throw new IllegalArgumentException("ST_X unsupported for type " + spatialField().dataType());
        };
    }

    @Override
    public DataType dataType() {
        return DOUBLE;
    }

    @Override
    public boolean prefersDocValuesExtraction() {
        return false;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new StX(source(), newChildren.getFirst());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, StX::new, spatialField());
    }

    private static double quantizeFromWKB(SpatialCoordinateTypes coordinateType, BytesRef in) {
        Point point = coordinateType.wkbAsPoint(in);
        long encoded = coordinateType.pointAsLong(point.getX(), point.getY());
        return coordinateType.decodeX(encoded);
    }

    @ConvertEvaluator(extraName = "FromCartesianWKB", warnExceptions = { IllegalArgumentException.class })
    static double fromCartesianWellKnownBinary(BytesRef in) {
        return quantizeFromWKB(CARTESIAN, in);
    }

    @ConvertEvaluator(extraName = "FromGeoWKB", warnExceptions = { IllegalArgumentException.class })
    static double fromGeoWellKnownBinary(BytesRef in) {
        return quantizeFromWKB(GEO, in);
    }

    @ConvertEvaluator(extraName = "FromCartesianDocValues", warnExceptions = { IllegalArgumentException.class })
    static double fromCartesianDocValues(long encoded) {
        return CARTESIAN.decodeX(encoded);
    }

    @ConvertEvaluator(extraName = "FromGeoDocValues", warnExceptions = { IllegalArgumentException.class })
    static double fromGeoDocValues(long encoded) {
        return GEO.decodeX(encoded);
    }
}
