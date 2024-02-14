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
import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.index.mapper.ShapeIndexer;
import org.elasticsearch.lucene.spatial.CartesianShapeIndexer;
import org.elasticsearch.lucene.spatial.CentroidCalculator;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;
import org.elasticsearch.lucene.spatial.GeometryDocValueReader;
import org.elasticsearch.lucene.spatial.GeometryDocValueWriter;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.util.SpatialCoordinateTypes;

import java.io.IOException;

import static org.elasticsearch.xpack.ql.planner.ExpressionTranslators.valueOf;

public class SpatialRelatesUtils {

    /**
     * This function is used to convert a spatial constant to a lucene Component2D.
     * When both left and right sides are constants, we convert the left to a doc-values byte array and the right to a Component2D.
     */
    static Component2D asLuceneComponent2D(SpatialRelatesFunction.SpatialCrsType crsType, Expression expression) {
        return asLuceneComponent2D(crsType, makeGeometryFromLiteral(expression));
    }

    static Component2D asLuceneComponent2D(SpatialRelatesFunction.SpatialCrsType crsType, Geometry geometry) {
        if (crsType == SpatialRelatesFunction.SpatialCrsType.GEO) {
            var luceneGeometries = LuceneGeometriesUtils.toLatLonGeometry(geometry, true, t -> {});
            return LatLonGeometry.create(luceneGeometries);
        } else {
            var luceneGeometries = LuceneGeometriesUtils.toXYGeometry(geometry, t -> {});
            return XYGeometry.create(luceneGeometries);
        }
    }

    /**
     * This function is used to convert a spatial constant to a doc-values byte array.
     * When both left and right sides are constants, we convert the left to a doc-values byte array and the right to a Component2D.
     */
    static GeometryDocValueReader asGeometryDocValueReader(SpatialRelatesFunction.SpatialCrsType crsType, Expression expression)
        throws IOException {
        Geometry geometry = makeGeometryFromLiteral(expression);
        if (crsType == SpatialRelatesFunction.SpatialCrsType.GEO) {
            return asGeometryDocValueReader(
                CoordinateEncoder.GEO,
                new GeoShapeIndexer(Orientation.CCW, "SpatialRelatesFunction"),
                geometry
            );
        } else {
            return asGeometryDocValueReader(CoordinateEncoder.CARTESIAN, new CartesianShapeIndexer("SpatialRelatesFunction"), geometry);
        }

    }

    /**
     * Converting shapes into doc-values byte arrays is needed under two situations:
     * - If both left and right are constants, we convert the right to Component2D and the left to doc-values for comparison
     * - If the right is a constant and no lucene push-down was possible, we get WKB in the left and convert it to doc-values for comparison
     */
    static GeometryDocValueReader asGeometryDocValueReader(CoordinateEncoder encoder, ShapeIndexer shapeIndexer, Geometry geometry)
        throws IOException {
        GeometryDocValueReader reader = new GeometryDocValueReader();
        CentroidCalculator centroidCalculator = new CentroidCalculator();
        if (geometry instanceof Circle circle) {
            // TODO: How should we deal with circles?
            centroidCalculator.add(new Point(circle.getX(), circle.getY()));
        } else {
            centroidCalculator.add(geometry);
        }
        reader.reset(GeometryDocValueWriter.write(shapeIndexer.indexShape(geometry), encoder, centroidCalculator));
        return reader;
    }

    /**
     * This function is used in two places, when evaluating a spatial constant in the SpatialRelatesFunction, as well as when
     * we do lucene-pushdown of spatial functions.
     */
    public static Geometry makeGeometryFromLiteral(Expression expr) {
        Object value = valueOf(expr);

        if (value instanceof BytesRef bytesRef) {
            if (EsqlDataTypes.isSpatial(expr.dataType())) {
                return SpatialCoordinateTypes.UNSPECIFIED.wkbToGeometry(bytesRef);
            } else {
                return SpatialCoordinateTypes.UNSPECIFIED.wktToGeometry(bytesRef.utf8ToString());
            }
        } else if (value instanceof String string) {
            return SpatialCoordinateTypes.UNSPECIFIED.wktToGeometry(string);
        } else {
            throw new IllegalArgumentException(
                "Unsupported combination of literal [" + value.getClass().getSimpleName() + "] of type [" + expr.dataType() + "]"
            );
        }
    }
}
