/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectortile.feature;

import com.wdtinc.mapbox_vector_tile.VectorTile;
import com.wdtinc.mapbox_vector_tile.adapt.jts.IUserDataConverter;
import com.wdtinc.mapbox_vector_tile.adapt.jts.JtsAdapter;
import com.wdtinc.mapbox_vector_tile.adapt.jts.UserDataIgnoreConverter;
import com.wdtinc.mapbox_vector_tile.build.MvtLayerProps;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.SimpleFeatureFactory;
import org.elasticsearch.common.geo.SphericalMercatorUtils;
import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.GeometryVisitor;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateSequenceFilter;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.IntersectionMatrix;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.TopologyException;
import org.locationtech.jts.simplify.TopologyPreservingSimplifier;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Transforms {@link Geometry} object in WGS84 into mvt features.
 */
public class FeatureFactory {

    private final IUserDataConverter userDataIgnoreConverter = new UserDataIgnoreConverter();
    private final MvtLayerProps layerProps = new MvtLayerProps();
    private final MVTGeometryBuilder mvtGeometryBuilder;
    // optimization for points and rectangles
    private final SimpleFeatureFactory simpleFeatureFactory;

    /**
     * The vector-tile feature factory will produce tiles as features based on the tile specification.
     * @param z - zoom level
     * @param x - x position of the tile at that zoom
     * @param y - y position of the tile at that zoom
     * @param extent - the full extent of entire area in pixels
     * @param padPixels - a buffer around each tile in pixels
     * The parameter padPixels is the size of the buffer in pixels for the clip envelope.
     * We choose a value that ensures we have values outside the tile for polygons crossing
     * the tile so the outline of the tile is not part of the final result. The default
     * value is set in SimpleVectorTileFormatter.DEFAULT_BUFFER_PIXELS (currently 5 pixels).
     */
    public FeatureFactory(int z, int x, int y, int extent, int padPixels) {
        // geometry factory
        final GeometryFactory geomFactory = new GeometryFactory();
        // pixel precision of the tile in the mercator projection.
        final double pixelPrecision = 2 * SphericalMercatorUtils.MERCATOR_BOUNDS / ((1L << z) * extent);
        final Rectangle r = SphericalMercatorUtils.recToSphericalMercator(GeoTileUtils.toBoundingBox(x, y, z));
        final Envelope tileEnvelope = new Envelope(r.getMinX(), r.getMaxX(), r.getMinY(), r.getMaxY());
        final Envelope clipEnvelope = new Envelope(tileEnvelope);
        // expand enough the clip envelope to prevent visual artefacts
        clipEnvelope.expandBy(padPixels * pixelPrecision, padPixels * pixelPrecision);
        // extent used for clipping
        final org.locationtech.jts.geom.Geometry clipTile = geomFactory.toGeometry(clipEnvelope);
        // transforms spherical mercator coordinates into the tile coordinates
        final CoordinateSequenceFilter sequenceFilter = new MvtCoordinateSequenceFilter(tileEnvelope, extent);
        this.mvtGeometryBuilder = new MVTGeometryBuilder(geomFactory, clipTile, pixelPrecision, sequenceFilter);
        this.simpleFeatureFactory = new SimpleFeatureFactory(z, x, y, extent);
    }

    /**
     * Returns a {@code byte[]} containing the mvt representation of the provided point
     */
    public byte[] point(double lon, double lat) throws IOException {
        return simpleFeatureFactory.point(lon, lat);
    }

    /**
     * Returns a {@code byte[]} containing the mvt representation of the provided rectangle
     */
    public byte[] box(double minLon, double maxLon, double minLat, double maxLat) throws IOException {
        return simpleFeatureFactory.box(minLon, maxLon, minLat, maxLat);
    }

    /**
     * Returns a {@code byte[]} containing the mvt representation of the provided points
     */
    public byte[] points(List<GeoPoint> multiPoint) {
        return simpleFeatureFactory.points(multiPoint);
    }

    /**
     * Returns a List {@code byte[]} containing the mvt representation of the provided geometry
     */
    public List<byte[]> getFeatures(Geometry geometry) {
        // Get geometry in pixel coordinates
        final org.locationtech.jts.geom.Geometry mvtGeometry = geometry.visit(mvtGeometryBuilder);
        if (mvtGeometry == null) {
            return List.of();
        }
        // MVT geometry to MVT feature
        final List<VectorTile.Tile.Feature> features = PatchedJtsAdapter.toFeatures(
            JtsAdapter.flatFeatureList(mvtGeometry),
            layerProps,
            userDataIgnoreConverter
        );
        final List<byte[]> byteFeatures = new ArrayList<>(features.size());
        features.forEach(f -> byteFeatures.add(f.toByteArray()));
        return byteFeatures;
    }

    private record MVTGeometryBuilder(
        GeometryFactory geomFactory,
        org.locationtech.jts.geom.Geometry clipTile,
        double pixelPrecision,
        CoordinateSequenceFilter sequenceFilter
    ) implements GeometryVisitor<org.locationtech.jts.geom.Geometry, IllegalArgumentException> {

        @Override
        public org.locationtech.jts.geom.Geometry visit(Circle circle) {
            throw new IllegalArgumentException("Circle is not supported");
        }

        @Override
        public org.locationtech.jts.geom.Geometry visit(GeometryCollection<?> collection) {
            return buildCollection(collection);
        }

        @Override
        public org.locationtech.jts.geom.Geometry visit(LinearRing ring) throws RuntimeException {
            throw new IllegalArgumentException("LinearRing is not supported");
        }

        @Override
        public org.locationtech.jts.geom.Geometry visit(Point point) throws RuntimeException {
            return toMVTGeometry(buildMercatorPoint(point));
        }

        @Override
        public org.locationtech.jts.geom.Geometry visit(MultiPoint multiPoint) throws RuntimeException {
            return toMVTGeometry(buildMercatorMultiPoint(multiPoint));
        }

        @Override
        public org.locationtech.jts.geom.Geometry visit(Line line) {
            return toMVTGeometry(buildMercatorLine(line));
        }

        @Override
        public org.locationtech.jts.geom.Geometry visit(MultiLine multiLine) throws RuntimeException {
            // Elasticsearch accepts lines that intersect but this might cause issues on JTS algorithms.
            // It is then important to transform each line individually, therefore we treat it as a collection.
            return buildCollection(multiLine);
        }

        @Override
        public org.locationtech.jts.geom.Geometry visit(Polygon polygon) throws RuntimeException {
            return toMVTGeometry(buildMercatorPolygon(polygon));
        }

        @Override
        public org.locationtech.jts.geom.Geometry visit(MultiPolygon multiPolygon) throws RuntimeException {
            // Elasticsearch accepts polygons that overlap but this causes issues on JTS algorithms.
            // It is then important to transform each polygon individually, therefore we treat it as a collection.
            return buildCollection(multiPolygon);
        }

        @Override
        public org.locationtech.jts.geom.Geometry visit(Rectangle rectangle) throws RuntimeException {
            return toMVTGeometry(buildMercatorRectangle(rectangle));
        }

        private org.locationtech.jts.geom.Geometry toMVTGeometry(org.locationtech.jts.geom.Geometry geometry) {
            // clip geometry
            geometry = clipGeometry(clipTile, geometry);
            if (geometry == null) {
                return null;
            }
            // simplify it
            geometry = TopologyPreservingSimplifier.simplify(geometry, pixelPrecision);
            // convert coordinates to MVT geometry
            geometry.apply(sequenceFilter);
            return geometry;
        }

        private org.locationtech.jts.geom.Geometry buildCollection(GeometryCollection<?> collection) {
            final List<org.locationtech.jts.geom.Geometry> geoms = new ArrayList<>(collection.size());
            for (int i = 0; i < collection.size(); i++) {
                final org.locationtech.jts.geom.Geometry geometry = collection.get(i).visit(this);
                if (geometry != null) {
                    // Simplification can transform simple shapes into multi-shapes.
                    // We flatten them out here so multi-polygons and multi-lines are not transformed to geometry collections.
                    for (int j = 0; j < geometry.getNumGeometries(); j++) {
                        geoms.add(geometry.getGeometryN(j));
                    }
                }
            }
            return geomFactory.buildGeometry(geoms);
        }

        private org.locationtech.jts.geom.Polygon buildMercatorPolygon(Polygon polygon) {
            final org.locationtech.jts.geom.LinearRing outerShell = buildMercatorLinearRing(polygon.getPolygon());
            if (polygon.getNumberOfHoles() == 0) {
                return geomFactory.createPolygon(outerShell);
            }
            final org.locationtech.jts.geom.LinearRing[] holes = new org.locationtech.jts.geom.LinearRing[polygon.getNumberOfHoles()];
            for (int i = 0; i < polygon.getNumberOfHoles(); i++) {
                holes[i] = buildMercatorLinearRing(polygon.getHole(i));
            }
            return geomFactory.createPolygon(outerShell, holes);
        }

        private org.locationtech.jts.geom.LinearRing buildMercatorLinearRing(LinearRing ring) {
            return geomFactory.createLinearRing(buildMercatorCoordinates(ring));
        }

        private LineString buildMercatorLine(Line line) {
            return geomFactory.createLineString(buildMercatorCoordinates(line));
        }

        private static Coordinate[] buildMercatorCoordinates(Line line) {
            final Coordinate[] coordinates = new Coordinate[line.length()];
            for (int i = 0; i < line.length(); i++) {
                final double x = SphericalMercatorUtils.lonToSphericalMercator(line.getX(i));
                final double y = SphericalMercatorUtils.latToSphericalMercator(line.getY(i));
                coordinates[i] = new Coordinate(x, y);
            }
            return coordinates;
        }

        private org.locationtech.jts.geom.Geometry buildMercatorRectangle(Rectangle rectangle) {
            final double xMin = SphericalMercatorUtils.lonToSphericalMercator(rectangle.getMinX());
            final double yMin = SphericalMercatorUtils.latToSphericalMercator(rectangle.getMinY());
            final double xMax = SphericalMercatorUtils.lonToSphericalMercator(rectangle.getMaxX());
            final double yMax = SphericalMercatorUtils.latToSphericalMercator(rectangle.getMaxY());
            final org.locationtech.jts.geom.Geometry geometry;
            if (rectangle.getMinX() > rectangle.getMaxX()) {
                // crosses dateline
                final Envelope westEnvelope = new Envelope(-SphericalMercatorUtils.MERCATOR_BOUNDS, xMax, yMin, yMax);
                final Envelope eastEnvelope = new Envelope(xMin, SphericalMercatorUtils.MERCATOR_BOUNDS, yMin, yMax);
                geometry = geomFactory.buildGeometry(List.of(geomFactory.toGeometry(westEnvelope), geomFactory.toGeometry(eastEnvelope)));
            } else {
                final Envelope envelope = new Envelope(xMin, xMax, yMin, yMax);
                geometry = geomFactory.toGeometry(envelope);
            }
            return geometry;
        }

        private org.locationtech.jts.geom.Point buildMercatorPoint(Point point) {
            final double x = SphericalMercatorUtils.lonToSphericalMercator(point.getX());
            final double y = SphericalMercatorUtils.latToSphericalMercator(point.getY());
            return geomFactory.createPoint(new Coordinate(x, y));
        }

        private org.locationtech.jts.geom.MultiPoint buildMercatorMultiPoint(MultiPoint multiPoint) {
            final org.locationtech.jts.geom.Point[] points = new org.locationtech.jts.geom.Point[multiPoint.size()];
            for (int i = 0; i < multiPoint.size(); i++) {
                points[i] = buildMercatorPoint(multiPoint.get(i));
            }
            Arrays.sort(
                points,
                Comparator.comparingDouble(org.locationtech.jts.geom.Point::getX).thenComparingDouble(org.locationtech.jts.geom.Point::getY)
            );
            return geomFactory.createMultiPoint(points);
        }

        private static org.locationtech.jts.geom.Geometry clipGeometry(
            org.locationtech.jts.geom.Geometry tile,
            org.locationtech.jts.geom.Geometry geometry
        ) {
            final Envelope tileEnvelope = tile.getEnvelopeInternal();
            final Envelope geometryEnvelope = geometry.getEnvelopeInternal();
            if (tileEnvelope.intersects(geometryEnvelope) == false) {
                return null; // disjoint
            } else if (tileEnvelope.contains(geometryEnvelope)) {
                return geometry; // geometry within the tile
            } else {
                try {
                    final IntersectionMatrix matrix = tile.relate(geometry);
                    if (matrix.isWithin()) {
                        // the clipped geometry is the envelope
                        return tile.copy();
                    } else if (matrix.isIntersects()) {
                        // clip it (clone envelope as coordinates are copied by reference)
                        return tile.copy().intersection(geometry);
                    } else {
                        // disjoint
                        assert tile.copy().intersection(geometry).isEmpty();
                        return null;
                    }
                } catch (TopologyException ex) {
                    // we should never get here but just to be super safe because a TopologyException will kill the node
                    throw new IllegalArgumentException(ex);
                }
            }
        }
    }

    private static class MvtCoordinateSequenceFilter implements CoordinateSequenceFilter {

        private final int extent;
        private final double pointXScale, pointYScale, pointXTranslate, pointYTranslate;

        private MvtCoordinateSequenceFilter(Envelope tileEnvelope, int extent) {
            this.extent = extent;
            this.pointXScale = (double) extent / tileEnvelope.getWidth();
            this.pointYScale = (double) -extent / tileEnvelope.getHeight();
            this.pointXTranslate = -pointXScale * tileEnvelope.getMinX();
            this.pointYTranslate = -pointYScale * tileEnvelope.getMinY();
        }

        @Override
        public void filter(CoordinateSequence seq, int i) {
            seq.setOrdinate(i, 0, lon(seq.getOrdinate(i, 0)));
            seq.setOrdinate(i, 1, lat(seq.getOrdinate(i, 1)));
        }

        @Override
        public boolean isDone() {
            return false;
        }

        @Override
        public boolean isGeometryChanged() {
            return true;
        }

        private int lat(double lat) {
            return (int) Math.round(pointYScale * lat + pointYTranslate) + extent;
        }

        private int lon(double lon) {
            return (int) Math.round(pointXScale * lon + pointXTranslate);
        }
    }
}
