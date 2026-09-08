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
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.operation.union.UnaryUnionOp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;

/**
 * Unary form of ST_UNION: unions all multi-values of a single geometry field into one geometry,
 * returning single values unchanged.
 * <p>
 * The function definition, documentation, and binary form (two geometry arguments) all live in
 * {@link StUnion}. The {@link StUnion#DEFINITION} builder dispatches here when called with a single
 * argument.
 * </p>
 */
public class StUnionUnary extends SpatialUnaryDocValuesFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "StUnionUnary",
        StUnionUnary::new
    );

    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

    public StUnionUnary(Source source, Expression left) {
        this(source, left, false);
    }

    private StUnionUnary(Source source, Expression left, boolean spatialDocValues) {
        super(source, left, spatialDocValues);
    }

    StUnionUnary(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public StUnionUnary withDocValues(boolean useDocValues) {
        return useDocValues ? new StUnionUnary(source(), spatialField(), true) : this;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new StUnionUnary(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, StUnionUnary::new, spatialField());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        if (DataType.isNull(spatialField().dataType())) {
            return DataType.NULL;
        }
        return DataType.isSpatialGeo(spatialField().dataType()) ? DataType.GEO_SHAPE : DataType.CARTESIAN_SHAPE;
    }

    @Override
    public Object fold(FoldContext ctx) {
        Object leftVal = spatialField().fold(ctx);
        if (leftVal == null) {
            return null;
        }
        return switch (leftVal) {
            case BytesRef wkb -> wkb; // single value: return unchanged
            case List<?> list -> {
                // multi-value: union all into one geometry
                List<Geometry> geoms = new ArrayList<>(list.size());
                for (Object item : list) {
                    if (item instanceof BytesRef wkb) {
                        try {
                            geoms.add(UNSPECIFIED.wkbToJtsGeometry(wkb));
                        } catch (org.locationtech.jts.io.ParseException e) {
                            throw new IllegalArgumentException("could not parse the geometry expression: " + e.getMessage(), e);
                        }
                    } else {
                        throw new IllegalArgumentException("unsupported list element type: " + item.getClass().getSimpleName());
                    }
                }
                yield UNSPECIFIED.jtsGeometryToWkb(UnaryUnionOp.union(geoms));
            }
            default -> throw new IllegalArgumentException("unsupported geometry type: " + leftVal.getClass().getSimpleName());
        };
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        ExpressionEvaluator.Factory sourceEval = toEvaluator.apply(spatialField());
        if (spatialDocValues && spatialField().dataType() == DataType.GEO_POINT) {
            return new StUnionUnaryGeoPointDocValuesEvaluator.Factory(source(), sourceEval);
        } else if (spatialDocValues && spatialField().dataType() == DataType.CARTESIAN_POINT) {
            return new StUnionUnaryCartesianPointDocValuesEvaluator.Factory(source(), sourceEval);
        } else {
            return new StUnionUnarySourceEvaluator.Factory(source(), sourceEval);
        }
    }

    @Evaluator(extraName = "Source", warnExceptions = { IllegalArgumentException.class })
    static void processSource(BytesRefBlock.Builder builder, @Position int p, BytesRefBlock geom) {
        processUnaryWkb(builder, p, geom);
    }

    @Evaluator(extraName = "GeoPointDocValues", warnExceptions = { IllegalArgumentException.class })
    static void processGeoPointDocValues(BytesRefBlock.Builder builder, @Position int p, LongBlock geom) {
        processUnaryDocValues(builder, p, geom, GEO);
    }

    @Evaluator(extraName = "CartesianPointDocValues", warnExceptions = { IllegalArgumentException.class })
    static void processCartesianPointDocValues(BytesRefBlock.Builder builder, @Position int p, LongBlock geom) {
        processUnaryDocValues(builder, p, geom, CARTESIAN);
    }

    /**
     * Unions all multi-values at position {@code p} into a single geometry (WKB output).
     * For a single value, the WKB bytes are returned unchanged. For multiple values, a
     * {@link UnaryUnionOp} is used to produce their union.
     */
    private static void processUnaryWkb(BytesRefBlock.Builder builder, int p, BytesRefBlock source) {
        if (source.isNull(p)) {
            builder.appendNull();
            return;
        }
        int firstValueIndex = source.getFirstValueIndex(p);
        int valueCount = source.getValueCount(p);
        BytesRef scratch = new BytesRef();
        if (valueCount == 1) {
            builder.appendBytesRef(source.getBytesRef(firstValueIndex, scratch));
            return;
        }
        try {
            List<Geometry> geoms = new ArrayList<>(valueCount);
            for (int i = 0; i < valueCount; i++) {
                geoms.add(UNSPECIFIED.wkbToJtsGeometry(source.getBytesRef(firstValueIndex + i, scratch)));
            }
            builder.appendBytesRef(UNSPECIFIED.jtsGeometryToWkb(UnaryUnionOp.union(geoms)));
        } catch (ParseException e) {
            throw new IllegalArgumentException("could not parse the geometry expression: " + e.getMessage(), e);
        }
    }

    /**
     * Unions all multi-value long-encoded points at position {@code p} into a single geometry (WKB output).
     * For a single value, the point is encoded as a Point geometry. For multiple values, they are
     * encoded as a MultiPoint geometry (union of points is a MultiPoint, not a further reduced shape).
     */
    private static void processUnaryDocValues(
        BytesRefBlock.Builder builder,
        int p,
        LongBlock source,
        SpatialCoordinateTypes coordinateType
    ) {
        if (source.isNull(p)) {
            builder.appendNull();
            return;
        }
        int firstValueIndex = source.getFirstValueIndex(p);
        int valueCount = source.getValueCount(p);
        if (valueCount == 1) {
            org.elasticsearch.geometry.Point pt = coordinateType.longAsPoint(source.getLong(firstValueIndex));
            builder.appendBytesRef(UNSPECIFIED.jtsGeometryToWkb(GEOMETRY_FACTORY.createPoint(new Coordinate(pt.getX(), pt.getY()))));
            return;
        }
        Coordinate[] coords = new Coordinate[valueCount];
        for (int i = 0; i < valueCount; i++) {
            org.elasticsearch.geometry.Point pt = coordinateType.longAsPoint(source.getLong(firstValueIndex + i));
            coords[i] = new Coordinate(pt.getX(), pt.getY());
        }
        builder.appendBytesRef(UNSPECIFIED.jtsGeometryToWkb(GEOMETRY_FACTORY.createMultiPointFromCoords(coords)));
    }
}
