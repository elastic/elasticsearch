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
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Position;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;

/**
 * Returns the geometry type of the given geometry as a string.
 * The function {@code st_geometry_type} is defined in the
 * <a href="https://www.ogc.org/standard/sfs/">OGC Simple Feature Access</a> standard.
 * Alternatively, it is well described in PostGIS documentation at
 * <a href="https://postgis.net/docs/ST_GeometryType.html">PostGIS:ST_GeometryType</a>.
 */
public class StGeometryType extends SpatialUnaryDocValuesFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "StGeometryType",
        StGeometryType::new
    );

    private static final BytesRef ST_POINT = new BytesRef("ST_Point");

    @FunctionInfo(
        returnType = "keyword",
        preview = true,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.1.0") },
        description = "Returns the geometry type of the supplied geometry, as a string.\n"
            + "For example: `ST_Point`, `ST_LineString`, `ST_Polygon`, `ST_MultiPoint`, `ST_MultiLineString`, "
            + "`ST_MultiPolygon`, or `ST_GeometryCollection`.",
        examples = @Example(file = "spatial_shapes", tag = "st_geometrytype"),
        depthOffset = 1  // So this appears as a subsection of geometry functions
    )
    public StGeometryType(
        Source source,
        @Param(
            name = "geometry",
            type = { "geo_point", "geo_shape", "cartesian_point", "cartesian_shape" },
            description = "Expression of type `geo_point`, `geo_shape`, `cartesian_point` or `cartesian_shape`. "
                + "If `null`, the function returns `null`."
        ) Expression field
    ) {
        this(source, field, false);
    }

    private StGeometryType(Source source, Expression field, boolean spatialDocValues) {
        super(source, field, spatialDocValues);
    }

    private StGeometryType(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        if (spatialDocValues) {
            return new StGeometryTypeFromPointDocValuesEvaluator.Factory(source(), toEvaluator.apply(spatialField()));
        } else {
            return new StGeometryTypeFromWKBEvaluator.Factory(source(), toEvaluator.apply(spatialField()));
        }
    }

    @Override
    public SpatialDocValuesFunction withDocValues(boolean useDocValues) {
        return new StGeometryType(source(), spatialField(), useDocValues);
    }

    @Override
    public DataType dataType() {
        return KEYWORD;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new StGeometryType(source(), newChildren.getFirst(), spatialDocValues);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, StGeometryType::new, spatialField());
    }

    static String geometryTypeName(ShapeType type) {
        return switch (type) {
            case POINT -> "ST_Point";
            case MULTIPOINT -> "ST_MultiPoint";
            case LINESTRING -> "ST_LineString";
            case MULTILINESTRING -> "ST_MultiLineString";
            case POLYGON -> "ST_Polygon";
            case MULTIPOLYGON -> "ST_MultiPolygon";
            case GEOMETRYCOLLECTION -> "ST_GeometryCollection";
            case LINEARRING -> "ST_LineString";
            case ENVELOPE -> "ST_Polygon";
            case CIRCLE -> "ST_Point";
        };
    }

    @Evaluator(extraName = "FromPointDocValues", warnExceptions = { IllegalArgumentException.class })
    static void fromPointDocValues(BytesRefBlock.Builder results, @Position int p, LongBlock encoded) {
        // Doc values are only used for points, so we always return "ST_Point"
        int valueCount = encoded.getValueCount(p);
        if (valueCount == 1) {
            results.appendBytesRef(ST_POINT);
        } else {
            // Multi-valued points are still points
            results.appendBytesRef(ST_POINT);
        }
    }

    @Evaluator(extraName = "FromWKB", warnExceptions = { IllegalArgumentException.class })
    static void fromWellKnownBinary(BytesRefBlock.Builder results, @Position int p, BytesRefBlock wkbBlock) {
        BytesRef scratch = new BytesRef();
        int firstValueIndex = wkbBlock.getFirstValueIndex(p);
        int valueCount = wkbBlock.getValueCount(p);
        if (valueCount == 1) {
            BytesRef wkb = wkbBlock.getBytesRef(firstValueIndex, scratch);
            Geometry geometry = UNSPECIFIED.wkbToGeometry(wkb);
            results.appendBytesRef(new BytesRef(geometryTypeName(geometry.type())));
        } else {
            // For multi-valued fields, return "ST_GeometryCollection"
            results.appendBytesRef(new BytesRef("ST_GeometryCollection"));
        }
    }
}
