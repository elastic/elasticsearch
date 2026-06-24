/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.index.mapper.ShapeIndexer;
import org.elasticsearch.lucene.spatial.CartesianShapeIndexer;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;
import org.elasticsearch.lucene.spatial.GeometryDocValueReader;
import org.elasticsearch.lucene.spatial.TriangleTreeVisitor;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.operation.union.CascadedPolygonUnion;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;

/**
 * A {@link GeometryOperator} that decomposes geometry A into individual triangles using the Lucene
 * triangle-tree representation, applies a per-triangle JTS operation against geometry B, and then
 * unions the accumulated results via {@link CascadedPolygonUnion}.
 *
 * <p>Both inputs and the return value are WKB-encoded ({@link BytesRef}). Geometry A is decoded
 * directly from WKB to the Elasticsearch geometry type (avoiding a redundant JTS round-trip).
 * Geometry B is decoded from WKB to JTS once before the per-triangle loop.
 *
 * <p>This approach can be more efficient than a direct JTS operation when geometry A is already
 * stored as doc-values (triangle-tree) on disk, because the decomposition avoids a full JTS parse
 * of A and can prune branches of the triangle tree early.
 *
 * <p>UNION and SYM_DIFFERENCE require decomposing both A and B, which is not yet implemented;
 * those operations fall back to JTS via the wrapped {@link GeometryOperator}.
 *
 * <p>Use the static factory methods to create instances:
 * {@link #intersection(BinarySpatialFunction.SpatialCrsType)},
 * {@link #union(BinarySpatialFunction.SpatialCrsType)},
 * {@link #difference(BinarySpatialFunction.SpatialCrsType)},
 * {@link #symDifference(BinarySpatialFunction.SpatialCrsType)}.
 */
public final class TriangleDecompositionOperator implements GeometryOperator {

    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

    private final CoordinateEncoder encoder;
    private final ShapeIndexer shapeIndexer;
    /**
     * The per-triangle JTS operation: takes a triangle polygon and the query geometry,
     * returns the result geometry (e.g. {@code Geometry::intersection}).
     */
    private final BiFunction<Geometry, Geometry, Geometry> triangleOperation;
    private final GeometryOperator fallback;

    private TriangleDecompositionOperator(
        CoordinateEncoder encoder,
        ShapeIndexer shapeIndexer,
        BiFunction<Geometry, Geometry, Geometry> triangleOperation,
        GeometryOperator fallback
    ) {
        this.encoder = encoder;
        this.shapeIndexer = shapeIndexer;
        this.triangleOperation = triangleOperation;
        this.fallback = fallback;
    }

    @Override
    public BytesRef apply(BytesRef wkbA, BytesRef wkbB) throws IOException {
        try {
            org.elasticsearch.geometry.Geometry esGeomA = UNSPECIFIED.wkbToGeometry(wkbA);
            GeometryDocValueReader reader = SpatialRelatesUtils.asGeometryDocValueReader(encoder, shapeIndexer, esGeomA);

            org.locationtech.jts.geom.Geometry jtsB;
            try {
                jtsB = UNSPECIFIED.wkbToJtsGeometry(wkbB);
            } catch (org.locationtech.jts.io.ParseException e) {
                throw new IOException("Failed to decode WKB for geometry B", e);
            }

            TriangleCollectingVisitor visitor = new TriangleCollectingVisitor(encoder, jtsB, triangleOperation);
            reader.visit(visitor);

            List<Geometry> parts = visitor.getResults();
            if (parts.isEmpty()) {
                return UNSPECIFIED.jtsGeometryToWkb(GEOMETRY_FACTORY.createGeometryCollection(new Geometry[0]));
            }
            return UNSPECIFIED.jtsGeometryToWkb(CascadedPolygonUnion.union(parts));
        } catch (IOException e) {
            throw e;
        } catch (Exception ignored) {
            // Fall through to JTS on any error
        }
        return fallback.apply(wkbA, wkbB);
    }

    /**
     * Creates a {@link TriangleDecompositionOperator} for ST_INTERSECTION.
     * Each triangle from A is intersected with B; results are unioned.
     */
    public static TriangleDecompositionOperator intersection(BinarySpatialFunction.SpatialCrsType crsType) {
        return new TriangleDecompositionOperator(
            encoderFor(crsType),
            indexerFor(crsType),
            Geometry::intersection,
            JtsGeometryOperator.INTERSECTION
        );
    }

    /**
     * Creates a {@link GeometryOperator} for ST_UNION.
     * Triangle decomposition of both A and B is not yet implemented; falls back to JTS directly.
     */
    public static GeometryOperator union(BinarySpatialFunction.SpatialCrsType crsType) {
        // UNION requires decomposing B as well, which is not yet implemented; fall back to JTS.
        return JtsGeometryOperator.UNION;
    }

    /**
     * Creates a {@link TriangleDecompositionOperator} for ST_DIFFERENCE.
     * Each triangle from A is differenced with B; results are unioned.
     */
    public static TriangleDecompositionOperator difference(BinarySpatialFunction.SpatialCrsType crsType) {
        return new TriangleDecompositionOperator(
            encoderFor(crsType),
            indexerFor(crsType),
            Geometry::difference,
            JtsGeometryOperator.DIFFERENCE
        );
    }

    /**
     * Creates a {@link GeometryOperator} for ST_SYMDIFFERENCE.
     * Triangle decomposition of both A and B is not yet implemented; falls back to JTS directly.
     */
    public static GeometryOperator symDifference(BinarySpatialFunction.SpatialCrsType crsType) {
        // SYM_DIFFERENCE requires decomposing B as well, which is not yet implemented; fall back to JTS.
        return JtsGeometryOperator.SYM_DIFFERENCE;
    }

    private static CoordinateEncoder encoderFor(BinarySpatialFunction.SpatialCrsType crsType) {
        return switch (crsType) {
            case GEO -> CoordinateEncoder.GEO;
            case CARTESIAN -> CoordinateEncoder.CARTESIAN;
            case UNSPECIFIED -> throw new IllegalArgumentException("UNSPECIFIED CRS type is not supported");
        };
    }

    private static ShapeIndexer indexerFor(BinarySpatialFunction.SpatialCrsType crsType) {
        return switch (crsType) {
            case GEO -> new GeoShapeIndexer(Orientation.CCW, "TriangleDecompositionOperator");
            case CARTESIAN -> new CartesianShapeIndexer("TriangleDecompositionOperator");
            case UNSPECIFIED -> throw new IllegalArgumentException("UNSPECIFIED CRS type is not supported");
        };
    }

    /**
     * A {@link TriangleTreeVisitor.TriangleTreeDecodedVisitor} that visits each decoded triangle,
     * applies a per-triangle JTS operation against a fixed query geometry, and accumulates
     * non-empty results.
     */
    private static class TriangleCollectingVisitor extends TriangleTreeVisitor.TriangleTreeDecodedVisitor {

        private final Geometry jtsB;
        private final BiFunction<Geometry, Geometry, Geometry> perTriangleOp;
        private final List<Geometry> results = new ArrayList<>();

        TriangleCollectingVisitor(CoordinateEncoder encoder, Geometry jtsB, BiFunction<Geometry, Geometry, Geometry> perTriangleOp) {
            super(encoder);
            this.jtsB = jtsB;
            this.perTriangleOp = perTriangleOp;
        }

        List<Geometry> getResults() {
            return results;
        }

        @Override
        protected void visitDecodedTriangle(double aX, double aY, double bX, double bY, double cX, double cY, byte metadata) {
            Coordinate[] coords = new Coordinate[] {
                new Coordinate(aX, aY),
                new Coordinate(bX, bY),
                new Coordinate(cX, cY),
                new Coordinate(aX, aY) };
            Geometry triangle = GEOMETRY_FACTORY.createPolygon(coords);
            Geometry result = perTriangleOp.apply(triangle, jtsB);
            if (result != null && result.isEmpty() == false) {
                results.add(result);
            }
        }

        @Override
        protected void visitDecodedLine(double aX, double aY, double bX, double bY, byte metadata) {
            // Lines have zero area; nothing to accumulate
        }

        @Override
        protected void visitDecodedPoint(double x, double y) {
            // Points have zero area; nothing to accumulate
        }

        @Override
        public boolean push() {
            return true;
        }

        @Override
        protected boolean pushDecodedX(double minX) {
            return true;
        }

        @Override
        protected boolean pushDecodedY(double minY) {
            return true;
        }

        @Override
        protected boolean pushDecoded(double maxX, double maxY) {
            return true;
        }

        @Override
        protected boolean pushDecoded(double minX, double minY, double maxX, double maxY) {
            return true;
        }
    }
}
