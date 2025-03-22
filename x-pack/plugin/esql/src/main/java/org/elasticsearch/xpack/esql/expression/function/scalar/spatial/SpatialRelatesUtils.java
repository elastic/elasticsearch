/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.geo.XYGeometry;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.LuceneGeometriesUtils;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.index.mapper.ShapeIndexer;
import org.elasticsearch.lucene.spatial.CartesianShapeIndexer;
import org.elasticsearch.lucene.spatial.CentroidCalculator;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;
import org.elasticsearch.lucene.spatial.GeometryDocValueReader;
import org.elasticsearch.lucene.spatial.GeometryDocValueWriter;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.core.expression.Foldables.valueOf;

public class SpatialRelatesUtils {

    /** Converts a {@link Expression} into a {@link Component2D}. */
    static Component2D asLuceneComponent2D(FoldContext ctx, BinarySpatialFunction.SpatialCrsType crsType, Expression expression) {
        return asLuceneComponent2D(crsType, makeGeometryFromLiteral(ctx, expression));
    }

    /** Converts a {@link Geometry} into a {@link Component2D}. */
    static Component2D asLuceneComponent2D(BinarySpatialFunction.SpatialCrsType crsType, Geometry geometry) {
        if (crsType == BinarySpatialFunction.SpatialCrsType.GEO) {
            var luceneGeometries = LuceneGeometriesUtils.toLatLonGeometry(geometry, true, t -> {});
            return LatLonGeometry.create(luceneGeometries);
        } else {
            var luceneGeometries = LuceneGeometriesUtils.toXYGeometry(geometry, t -> {});
            return XYGeometry.create(luceneGeometries);
        }
    }

    /** Converts a {@link BytesRefBlock} at a given {@code position} into a {@link Component2D}. */
    static Component2D asLuceneComponent2D(BinarySpatialFunction.SpatialCrsType type, BytesRefBlock valueBlock, int position) {
        return asLuceneComponent2D(type, asGeometry(valueBlock, position));
    }

    /**
     * Converts a {@link Expression} at a given {@code position} into a {@link Component2D} array.
     * The reason for generating an array instead of a single component is for multi-shape support with ST_CONTAINS.
     */
    static Component2D[] asLuceneComponent2Ds(FoldContext ctx, BinarySpatialFunction.SpatialCrsType crsType, Expression expression) {
        return asLuceneComponent2Ds(crsType, makeGeometryFromLiteral(ctx, expression));
    }

    /**
     * Converts a {@link Geometry} at a given {@code position} into a {@link Component2D} array.
     * The reason for generating an array instead of a single component is for multi-shape support with ST_CONTAINS.
     */
    static Component2D[] asLuceneComponent2Ds(BinarySpatialFunction.SpatialCrsType crsType, Geometry geometry) {
        if (crsType == BinarySpatialFunction.SpatialCrsType.GEO) {
            var luceneGeometries = LuceneGeometriesUtils.toLatLonGeometry(geometry, true, t -> {});
            return LuceneComponent2DUtils.createLatLonComponents(luceneGeometries);
        } else {
            var luceneGeometries = LuceneGeometriesUtils.toXYGeometry(geometry, t -> {});
            return LuceneComponent2DUtils.createXYComponents(luceneGeometries);
        }
    }

    /** Converts a {@link BytesRefBlock} at a given {@code position} into a {@link Component2D} array. */
    static Component2D[] asLuceneComponent2Ds(BinarySpatialFunction.SpatialCrsType type, BytesRefBlock valueBlock, int position) {
        return asLuceneComponent2Ds(type, asGeometry(valueBlock, position));
    }

    /** Converts a {@link Expression} into a {@link GeometryDocValueReader} */
    static GeometryDocValueReader asGeometryDocValueReader(
        FoldContext ctx,
        BinarySpatialFunction.SpatialCrsType crsType,
        Expression expression
    ) throws IOException {
        Geometry geometry = makeGeometryFromLiteral(ctx, expression);
        if (crsType == BinarySpatialFunction.SpatialCrsType.GEO) {
            return asGeometryDocValueReader(
                CoordinateEncoder.GEO,
                new GeoShapeIndexer(Orientation.CCW, "SpatialRelatesFunction"),
                geometry
            );
        } else {
            return asGeometryDocValueReader(CoordinateEncoder.CARTESIAN, new CartesianShapeIndexer("SpatialRelatesFunction"), geometry);
        }

    }

    /** Converts a {@link Geometry} into a {@link GeometryDocValueReader} */
    static GeometryDocValueReader asGeometryDocValueReader(CoordinateEncoder encoder, ShapeIndexer shapeIndexer, Geometry geometry)
        throws IOException {
        GeometryDocValueReader reader = new GeometryDocValueReader();
        CentroidCalculator centroidCalculator = new CentroidCalculator();
        if (geometry instanceof Circle) {
            // Both the centroid calculator and the shape indexer do not support circles
            throw new IllegalArgumentException(ShapeType.CIRCLE + " geometry is not supported");
        }
        centroidCalculator.add(geometry);
        reader.reset(GeometryDocValueWriter.write(shapeIndexer.indexShape(geometry), encoder, centroidCalculator));
        return reader;
    }

    /** Converts a {@link LongBlock} at a give {@code position} into a {@link GeometryDocValueReader} */
    static GeometryDocValueReader asGeometryDocValueReader(
        CoordinateEncoder encoder,
        ShapeIndexer shapeIndexer,
        LongBlock valueBlock,
        int position,
        Function<Long, Point> decoder
    ) throws IOException {
        final int firstValueIndex = valueBlock.getFirstValueIndex(position);
        final int valueCount = valueBlock.getValueCount(position);
        if (valueCount == 1) {
            return asGeometryDocValueReader(encoder, shapeIndexer, decoder.apply(valueBlock.getLong(firstValueIndex)));
        }
        final List<Point> points = new ArrayList<>(valueCount);
        for (int i = 0; i < valueCount; i++) {
            points.add(decoder.apply(valueBlock.getLong(firstValueIndex + i)));
        }
        return asGeometryDocValueReader(encoder, shapeIndexer, new MultiPoint(points));
    }

    /** Converts a {@link BytesRefBlock} at a given {code position} into a {@link GeometryDocValueReader} */
    static GeometryDocValueReader asGeometryDocValueReader(
        CoordinateEncoder encoder,
        ShapeIndexer shapeIndexer,
        BytesRefBlock valueBlock,
        int position
    ) throws IOException {
        return asGeometryDocValueReader(encoder, shapeIndexer, asGeometry(valueBlock, position));
    }

    private static Geometry asGeometry(BytesRefBlock valueBlock, int position) {
        final BytesRef scratch = new BytesRef();
        final int firstValueIndex = valueBlock.getFirstValueIndex(position);
        final int valueCount = valueBlock.getValueCount(position);
        if (valueCount == 1) {
            return SpatialCoordinateTypes.UNSPECIFIED.wkbToGeometry(valueBlock.getBytesRef(firstValueIndex, scratch));
        }
        final List<Geometry> geometries = new ArrayList<>(valueCount);
        for (int i = 0; i < valueCount; i++) {
            geometries.add(SpatialCoordinateTypes.UNSPECIFIED.wkbToGeometry(valueBlock.getBytesRef(firstValueIndex + i, scratch)));
        }
        return new GeometryCollection<>(geometries);
    }

    /**
     * This function is used in two places, when evaluating a spatial constant in the SpatialRelatesFunction, as well as when
     * we do lucene-pushdown of spatial functions.
     */
    public static Geometry makeGeometryFromLiteral(FoldContext ctx, Expression expr) {
        return makeGeometryFromLiteralValue(valueOf(ctx, expr), expr.dataType());
    }

    private static Geometry makeGeometryFromLiteralValue(Object value, DataType dataType) {
        if (value instanceof BytesRef bytesRef) {
            // Single value expression
            return SpatialCoordinateTypes.UNSPECIFIED.wkbToGeometry(bytesRef);
        } else if (value instanceof List<?> bytesRefList) {
            // Multi-value expression
            ArrayList<Geometry> geometries = new ArrayList<>();
            for (Object obj : bytesRefList) {
                geometries.add(makeGeometryFromLiteralValue(obj, dataType));
            }
            return new GeometryCollection<>(geometries);
        } else {
            throw new IllegalArgumentException(
                "Unsupported combination of literal [" + value.getClass().getSimpleName() + "] of type [" + dataType + "]"
            );
        }
    }
}
