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
    private final JTSGeometryBuilder builder;
    // extent used for clipping
    private final org.locationtech.jts.geom.Geometry clipTile;
    // transforms spherical mercator coordinates into the tile coordinates
    private final CoordinateSequenceFilter sequenceFilter;
    // pixel precision of the tile in the mercator projection.
    private final double pixelPrecision;
    // size of the buffer in pixels for the clip envelope. we choose a values that makes sure
    // we have values outside the tile for polygon crossing the tile so the outline of the
    // tile is not part of the final result.
    // TODO: consider exposing this parameter so users have control of the buffer's size.
    private static final int BUFFER_SIZE_PIXELS = 5;

    public FeatureFactory(int z, int x, int y, int extent) {
        this.pixelPrecision = 2 * SphericalMercatorUtils.MERCATOR_BOUNDS / ((1L << z) * extent);
        final Rectangle r = SphericalMercatorUtils.recToSphericalMercator(GeoTileUtils.toBoundingBox(x, y, z));
        final Envelope tileEnvelope = new Envelope(r.getMinX(), r.getMaxX(), r.getMinY(), r.getMaxY());
        final Envelope clipEnvelope = new Envelope(tileEnvelope);
        // expand enough the clip envelope to prevent visual artefacts
        clipEnvelope.expandBy(BUFFER_SIZE_PIXELS * this.pixelPrecision, BUFFER_SIZE_PIXELS * this.pixelPrecision);
        final GeometryFactory geomFactory = new GeometryFactory();
        this.builder = new JTSGeometryBuilder(geomFactory);
        this.clipTile = geomFactory.toGeometry(clipEnvelope);
        this.sequenceFilter = new MvtCoordinateSequenceFilter(tileEnvelope, extent);
    }

    public List<byte[]> getFeatures(Geometry geometry) {
        // Get geometry in spherical mercator
        final org.locationtech.jts.geom.Geometry jtsGeometry = geometry.visit(builder);
        // clip the geometry to the tile
        final List<org.locationtech.jts.geom.Geometry> flatGeometries = clipGeometries(
            clipTile.copy(),
            JtsAdapter.flatFeatureList(jtsGeometry)
        );
        // simplify geometry using the pixel precision
        simplifyGeometry(flatGeometries, pixelPrecision);
        // convert coordinates to MVT geometry
        convertToMvtGeometry(flatGeometries, sequenceFilter);
        // MVT geometry to MVT feature
        final List<VectorTile.Tile.Feature> features = PatchedJtsAdapter.toFeatures(flatGeometries, layerProps, userDataIgnoreConverter);
        final List<byte[]> byteFeatures = new ArrayList<>(features.size());
        features.forEach(f -> byteFeatures.add(f.toByteArray()));
        return byteFeatures;
    }

    private static List<org.locationtech.jts.geom.Geometry> clipGeometries(
        org.locationtech.jts.geom.Geometry envelope,
        List<org.locationtech.jts.geom.Geometry> geometries
    ) {
        final List<org.locationtech.jts.geom.Geometry> intersected = new ArrayList<>(geometries.size());
        for (org.locationtech.jts.geom.Geometry geometry : geometries) {
            try {
                final IntersectionMatrix matrix = envelope.relate(geometry);
                if (matrix.isContains()) {
                    // no need to clip
                    intersected.add(geometry);
                } else if (matrix.isWithin()) {
                    // the clipped geometry is the envelope
                    intersected.add(envelope);
                } else if (matrix.isIntersects()) {
                    // clip it
                    intersected.add(envelope.intersection(geometry));
                } else {
                    // disjoint
                    assert envelope.intersection(geometry).isEmpty();
                }
            } catch (TopologyException e) {
                // ignore
            }
        }
        return intersected;
    }

    private static void simplifyGeometry(List<org.locationtech.jts.geom.Geometry> geometries, double precision) {
        for (int i = 0; i < geometries.size(); i++) {
            geometries.set(i, TopologyPreservingSimplifier.simplify(geometries.get(i), precision));
        }
    }

    private static void convertToMvtGeometry(List<org.locationtech.jts.geom.Geometry> geometries, CoordinateSequenceFilter sequenceFilter) {
        for (org.locationtech.jts.geom.Geometry geometry : geometries) {
            geometry.apply(sequenceFilter);
        }
    }

    private static class JTSGeometryBuilder implements GeometryVisitor<org.locationtech.jts.geom.Geometry, IllegalArgumentException> {

        private final GeometryFactory geomFactory;

        JTSGeometryBuilder(GeometryFactory geomFactory) {
            this.geomFactory = geomFactory;
        }

        @Override
        public org.locationtech.jts.geom.Geometry visit(Circle circle) {
            throw new IllegalArgumentException("Circle is not supported");
        }

        @Override
        public org.locationtech.jts.geom.Geometry visit(GeometryCollection<?> collection) {
            final org.locationtech.jts.geom.Geometry[] geometries = new org.locationtech.jts.geom.Geometry[collection.size()];
            for (int i = 0; i < collection.size(); i++) {
                geometries[i] = collection.get(i).visit(this);
            }
            return geomFactory.createGeometryCollection(geometries);
        }

        @Override
        public org.locationtech.jts.geom.Geometry visit(LinearRing ring) throws RuntimeException {
            throw new IllegalArgumentException("LinearRing is not supported");
        }

        @Override
        public org.locationtech.jts.geom.Geometry visit(Point point) throws RuntimeException {
            return buildPoint(point);
        }

        @Override
        public org.locationtech.jts.geom.Geometry visit(MultiPoint multiPoint) throws RuntimeException {
            final org.locationtech.jts.geom.Point[] points = new org.locationtech.jts.geom.Point[multiPoint.size()];
            for (int i = 0; i < multiPoint.size(); i++) {
                points[i] = buildPoint(multiPoint.get(i));
            }
            Arrays.sort(
                points,
                Comparator.comparingDouble(org.locationtech.jts.geom.Point::getX).thenComparingDouble(org.locationtech.jts.geom.Point::getY)
            );
            return geomFactory.createMultiPoint(points);
        }

        private org.locationtech.jts.geom.Point buildPoint(Point point) {
            final double x = SphericalMercatorUtils.lonToSphericalMercator(point.getX());
            final double y = SphericalMercatorUtils.latToSphericalMercator(point.getY());
            return geomFactory.createPoint(new Coordinate(x, y));
        }

        @Override
        public org.locationtech.jts.geom.Geometry visit(Line line) {
            return buildLine(line);
        }

        @Override
        public org.locationtech.jts.geom.Geometry visit(MultiLine multiLine) throws RuntimeException {
            final LineString[] lineStrings = new LineString[multiLine.size()];
            for (int i = 0; i < multiLine.size(); i++) {
                lineStrings[i] = buildLine(multiLine.get(i));
            }
            return geomFactory.createMultiLineString(lineStrings);
        }

        private LineString buildLine(Line line) {
            final Coordinate[] coordinates = new Coordinate[line.length()];
            for (int i = 0; i < line.length(); i++) {
                final double x = SphericalMercatorUtils.lonToSphericalMercator(line.getX(i));
                final double y = SphericalMercatorUtils.latToSphericalMercator(line.getY(i));
                coordinates[i] = new Coordinate(x, y);
            }
            return geomFactory.createLineString(coordinates);
        }

        @Override
        public org.locationtech.jts.geom.Geometry visit(Polygon polygon) throws RuntimeException {
            return buildPolygon(polygon);
        }

        @Override
        public org.locationtech.jts.geom.Geometry visit(MultiPolygon multiPolygon) throws RuntimeException {
            final org.locationtech.jts.geom.Polygon[] polygons = new org.locationtech.jts.geom.Polygon[multiPolygon.size()];
            for (int i = 0; i < multiPolygon.size(); i++) {
                polygons[i] = buildPolygon(multiPolygon.get(i));
            }
            return geomFactory.createMultiPolygon(polygons);
        }

        private org.locationtech.jts.geom.Polygon buildPolygon(Polygon polygon) {
            final org.locationtech.jts.geom.LinearRing outerShell = buildLinearRing(polygon.getPolygon());
            if (polygon.getNumberOfHoles() == 0) {
                return geomFactory.createPolygon(outerShell);
            }
            final org.locationtech.jts.geom.LinearRing[] holes = new org.locationtech.jts.geom.LinearRing[polygon.getNumberOfHoles()];
            for (int i = 0; i < polygon.getNumberOfHoles(); i++) {
                holes[i] = buildLinearRing(polygon.getHole(i));
            }
            return geomFactory.createPolygon(outerShell, holes);
        }

        private org.locationtech.jts.geom.LinearRing buildLinearRing(LinearRing ring) throws RuntimeException {
            final Coordinate[] coordinates = new Coordinate[ring.length()];
            for (int i = 0; i < ring.length(); i++) {
                final double x = SphericalMercatorUtils.lonToSphericalMercator(ring.getX(i));
                final double y = SphericalMercatorUtils.latToSphericalMercator(ring.getY(i));
                coordinates[i] = new Coordinate(x, y);
            }
            return geomFactory.createLinearRing(coordinates);
        }

        @Override
        public org.locationtech.jts.geom.Geometry visit(Rectangle rectangle) throws RuntimeException {
            final double xMin = SphericalMercatorUtils.lonToSphericalMercator(rectangle.getMinX());
            final double yMin = SphericalMercatorUtils.latToSphericalMercator(rectangle.getMinY());
            final double xMax = SphericalMercatorUtils.lonToSphericalMercator(rectangle.getMaxX());
            final double yMax = SphericalMercatorUtils.latToSphericalMercator(rectangle.getMaxY());
            final Envelope envelope = new Envelope(xMin, xMax, yMin, yMax);
            return geomFactory.toGeometry(envelope);
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
