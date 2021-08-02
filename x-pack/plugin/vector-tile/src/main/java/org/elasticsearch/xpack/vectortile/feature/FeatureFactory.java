/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectortile.feature;

import com.wdtinc.mapbox_vector_tile.VectorTile;
import com.wdtinc.mapbox_vector_tile.adapt.jts.IGeometryFilter;
import com.wdtinc.mapbox_vector_tile.adapt.jts.IUserDataConverter;
import com.wdtinc.mapbox_vector_tile.adapt.jts.JtsAdapter;
import com.wdtinc.mapbox_vector_tile.adapt.jts.TileGeomResult;
import com.wdtinc.mapbox_vector_tile.adapt.jts.UserDataIgnoreConverter;
import com.wdtinc.mapbox_vector_tile.build.MvtLayerParams;
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
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.simplify.TopologyPreservingSimplifier;

import java.util.ArrayList;
import java.util.List;

/**
 * Transforms {@link Geometry} object in WGS84 into mvt features.
 */
public class FeatureFactory {

    private final IGeometryFilter acceptAllGeomFilter = geometry -> true;
    private final IUserDataConverter userDataIgnoreConverter = new UserDataIgnoreConverter();
    private final MvtLayerParams layerParams;
    private final GeometryFactory geomFactory = new GeometryFactory();
    private final MvtLayerProps layerProps = new MvtLayerProps();
    private final JTSGeometryBuilder builder;

    private final Envelope tileEnvelope;
    private final Envelope clipEnvelope;

    public FeatureFactory(int z, int x, int y, int extent) {
        final Rectangle r = SphericalMercatorUtils.recToSphericalMercator(GeoTileUtils.toBoundingBox(x, y, z));
        this.tileEnvelope = new Envelope(r.getMinX(), r.getMaxX(), r.getMinY(), r.getMaxY());
        this.clipEnvelope = new Envelope(tileEnvelope);
        // pixel precision of the tile in the mercator projection.
        final double pixelPrecision = 2 * SphericalMercatorUtils.MERCATOR_BOUNDS / ((1L << z) * extent);
        this.clipEnvelope.expandBy(pixelPrecision, pixelPrecision);
        this.builder = new JTSGeometryBuilder(geomFactory, geomFactory.toGeometry(tileEnvelope), pixelPrecision);
        // TODO: Not sure what is the difference between extent and tile size?
        this.layerParams = new MvtLayerParams(extent, extent);
    }

    public List<byte[]> getFeatures(Geometry geometry) {
        final org.locationtech.jts.geom.Geometry jtsGeometry = geometry.visit(builder);
        if (jtsGeometry.isValid() == false) {
            return List.of();
        }
        final TileGeomResult tileGeom = JtsAdapter.createTileGeom(
            JtsAdapter.flatFeatureList(jtsGeometry),
            tileEnvelope,
            clipEnvelope,
            geomFactory,
            layerParams,
            acceptAllGeomFilter
        );
        // MVT tile geometry to MVT features
        final List<VectorTile.Tile.Feature> features = JtsAdapter.toFeatures(tileGeom.mvtGeoms, layerProps, userDataIgnoreConverter);
        final List<byte[]> byteFeatures = new ArrayList<>(features.size());
        features.forEach(f -> byteFeatures.add(f.toByteArray()));
        return byteFeatures;
    }

    private static class JTSGeometryBuilder implements GeometryVisitor<org.locationtech.jts.geom.Geometry, IllegalArgumentException> {

        private final GeometryFactory geomFactory;
        private final org.locationtech.jts.geom.Geometry tile;
        private final double pixelPrecision;

        JTSGeometryBuilder(GeometryFactory geomFactory, org.locationtech.jts.geom.Geometry tile, double pixelPrecision) {
            this.pixelPrecision = pixelPrecision;
            this.tile = tile;
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
            return geomFactory.createMultiPoint(points);
        }

        private org.locationtech.jts.geom.Point buildPoint(Point point) {
            final double x = SphericalMercatorUtils.lonToSphericalMercator(point.getX());
            final double y = SphericalMercatorUtils.latToSphericalMercator(point.getY());
            return geomFactory.createPoint(new Coordinate(x, y));
        }

        @Override
        public org.locationtech.jts.geom.Geometry visit(Line line) {
            return TopologyPreservingSimplifier.simplify(buildLine(line), pixelPrecision);
        }

        @Override
        public org.locationtech.jts.geom.Geometry visit(MultiLine multiLine) throws RuntimeException {
            final LineString[] lineStrings = new LineString[multiLine.size()];
            for (int i = 0; i < multiLine.size(); i++) {
                lineStrings[i] = buildLine(multiLine.get(i));
            }
            return TopologyPreservingSimplifier.simplify(geomFactory.createMultiLineString(lineStrings), pixelPrecision);
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
            final org.locationtech.jts.geom.Polygon jtsPolygon = buildPolygon(polygon);
            if (jtsPolygon.contains(tile)) {
                // shortcut, we return the tile
                return tile;
            }
            return TopologyPreservingSimplifier.simplify(jtsPolygon, pixelPrecision);
        }

        @Override
        public org.locationtech.jts.geom.Geometry visit(MultiPolygon multiPolygon) throws RuntimeException {
            final org.locationtech.jts.geom.Polygon[] polygons = new org.locationtech.jts.geom.Polygon[multiPolygon.size()];
            for (int i = 0; i < multiPolygon.size(); i++) {
                final org.locationtech.jts.geom.Polygon jtsPolygon = buildPolygon(multiPolygon.get(i));
                if (jtsPolygon.contains(tile)) {
                    // shortcut, we return the tile
                    return tile;
                }
                polygons[i] = jtsPolygon;
            }
            return TopologyPreservingSimplifier.simplify(geomFactory.createMultiPolygon(polygons), pixelPrecision);
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
            // TODO: handle degenerated rectangles?
            final double xMin = SphericalMercatorUtils.lonToSphericalMercator(rectangle.getMinX());
            final double yMin = SphericalMercatorUtils.latToSphericalMercator(rectangle.getMinY());
            final double xMax = SphericalMercatorUtils.lonToSphericalMercator(rectangle.getMaxX());
            final double yMax = SphericalMercatorUtils.latToSphericalMercator(rectangle.getMaxY());
            final Coordinate[] coordinates = new Coordinate[5];
            coordinates[0] = new Coordinate(xMin, yMin);
            coordinates[1] = new Coordinate(xMax, yMin);
            coordinates[2] = new Coordinate(xMax, yMax);
            coordinates[3] = new Coordinate(xMin, yMax);
            coordinates[4] = new Coordinate(xMin, yMin);
            return geomFactory.createPolygon(coordinates);
        }
    }
}
