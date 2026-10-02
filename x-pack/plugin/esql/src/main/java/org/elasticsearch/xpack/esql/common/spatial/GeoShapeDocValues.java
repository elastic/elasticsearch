/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.common.spatial;

import org.apache.lucene.document.ShapeField;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.lucene.spatial.CentroidCalculator;
import org.elasticsearch.lucene.spatial.Component2DVisitor;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;
import org.elasticsearch.lucene.spatial.DimensionalShapeType;
import org.elasticsearch.lucene.spatial.GeometryDocValueReader;
import org.elasticsearch.lucene.spatial.GeometryDocValueWriter;
import org.elasticsearch.lucene.spatial.TriangleTreeVisitor;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;

/**
 * Wraps a geo_shape WKB as a triangle-tree for intersection testing against grid cells.
 * All types are from server/Lucene — no dependency on the spatial plugin.
 */
public class GeoShapeDocValues {
    private final GeometryDocValueReader reader;
    private final double minLon;
    private final double maxLon;
    private final double minLat;
    private final double maxLat;
    private final boolean singlePoint;

    private GeoShapeDocValues(
        GeometryDocValueReader reader,
        double minLon,
        double maxLon,
        double minLat,
        double maxLat,
        boolean singlePoint
    ) {
        this.reader = reader;
        this.minLon = minLon;
        this.maxLon = maxLon;
        this.minLat = minLat;
        this.maxLat = maxLat;
        this.singlePoint = singlePoint;
    }

    public double minLon() {
        return minLon;
    }

    public double maxLon() {
        return maxLon;
    }

    public double minLat() {
        return minLat;
    }

    public double maxLat() {
        return maxLat;
    }

    /** Visits the triangle tree, for example with a {@code GeoHexVisitor} to relate it to an H3 cell. */
    public void visit(TriangleTreeVisitor visitor) throws IOException {
        reader.visit(visitor);
    }

    /**
     * Whether the geometry is a single point, which the grid functions encode directly instead of tiling.
     * A multi-point has the same dimensional type but a non-degenerate extent, unless all its points coincide,
     * in which case encoding the one location is equivalent.
     */
    public boolean isSinglePoint() {
        return singlePoint;
    }

    /** Longitude of the centroid, which for a {@link #isSinglePoint() single point} is the point itself. */
    public double centroidLon() throws IOException {
        return CoordinateEncoder.GEO.decodeX(reader.getCentroidX());
    }

    /** Latitude of the centroid, which for a {@link #isSinglePoint() single point} is the point itself. */
    public double centroidLat() throws IOException {
        return CoordinateEncoder.GEO.decodeY(reader.getCentroidY());
    }

    /**
     * Parses a WKB-encoded geometry into a triangle-tree representation suitable for
     * intersection testing. The bounding box is extracted from the encoded extent.
     */
    public static GeoShapeDocValues from(BytesRef wkb, GeoShapeIndexer indexer) throws IOException {
        Geometry geometry = GEO.wkbToGeometry(wkb);
        CentroidCalculator centroidCalculator = new CentroidCalculator();
        centroidCalculator.add(geometry);
        return fromDocValue(GeometryDocValueWriter.write(indexer.indexShape(geometry), CoordinateEncoder.GEO, centroidCalculator));
    }

    /**
     * Wraps a triangle tree as stored in the doc values of a {@code geo_shape} field, or as produced by
     * {@link #from(BytesRef, GeoShapeIndexer)} from WKB. Both paths therefore compute cells on the same representation.
     */
    public static GeoShapeDocValues fromDocValue(BytesRef encoded) throws IOException {
        GeometryDocValueReader reader = new GeometryDocValueReader();
        reader.reset(encoded);
        var extent = reader.getExtent();
        double minLon = CoordinateEncoder.GEO.decodeX(extent.minX());
        double maxLon = CoordinateEncoder.GEO.decodeX(extent.maxX());
        double minLat = CoordinateEncoder.GEO.decodeY(extent.minY());
        double maxLat = CoordinateEncoder.GEO.decodeY(extent.maxY());
        boolean singlePoint = reader.getDimensionalShapeType() == DimensionalShapeType.POINT
            && extent.minX() == extent.maxX()
            && extent.minY() == extent.maxY();
        return new GeoShapeDocValues(reader, minLon, maxLon, minLat, maxLat, singlePoint);
    }

    /**
     * Tests whether this geometry intersects the given {@link Component2D}.
     */
    public boolean intersects(Component2D component) throws IOException {
        Component2DVisitor visitor = Component2DVisitor.getVisitor(component, ShapeField.QueryRelation.INTERSECTS, CoordinateEncoder.GEO);
        visitor.reset();
        reader.visit(visitor);
        return visitor.matches();
    }
}
