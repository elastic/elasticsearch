/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.common.spatial;

import org.apache.lucene.geo.Component2D;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.lucene.spatial.CentroidCalculator;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;
import org.elasticsearch.lucene.spatial.DimensionalShapeType;
import org.elasticsearch.lucene.spatial.Extent;
import org.elasticsearch.lucene.spatial.GeometryDocValueWriter;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;

/**
 * Wraps a geo_shape doc value (a serialized triangle tree) for intersection testing against grid cells.
 * <p>
 * The doc-value header (centroid, shape type, extent) is decoded here, and the triangle tree is traversed with
 * ES|QL's own {@link TriangleTreeReader} and {@link TriangleTreeVisitor} rather than the server's equivalents:
 * see {@link TriangleTreeVisitor} for why that duplication is deliberate. The binary format itself is owned by
 * the server's {@code GeometryDocValueWriter}/{@code TriangleTreeWriter}, which also write the values this class
 * reads on the WKB path in {@link #from}.
 */
public class GeoShapeDocValues {
    private final ByteArrayStreamInput input;
    private final int treeOffset;
    private final Extent extent;
    private final int encodedCentroidX;
    private final int encodedCentroidY;
    private final double minLon;
    private final double maxLon;
    private final double minLat;
    private final double maxLat;
    private final boolean singlePoint;

    private GeoShapeDocValues(
        ByteArrayStreamInput input,
        int treeOffset,
        Extent extent,
        DimensionalShapeType shapeType,
        int encodedCentroidX,
        int encodedCentroidY
    ) {
        this.input = input;
        this.treeOffset = treeOffset;
        this.extent = extent;
        this.encodedCentroidX = encodedCentroidX;
        this.encodedCentroidY = encodedCentroidY;
        this.minLon = CoordinateEncoder.GEO.decodeX(extent.minX());
        this.maxLon = CoordinateEncoder.GEO.decodeX(extent.maxX());
        this.minLat = CoordinateEncoder.GEO.decodeY(extent.minY());
        this.maxLat = CoordinateEncoder.GEO.decodeY(extent.maxY());
        this.singlePoint = shapeType == DimensionalShapeType.POINT && extent.minX() == extent.maxX() && extent.minY() == extent.maxY();
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
        if (visitor.push(extent.minX(), extent.minY(), extent.maxX(), extent.maxY())) {
            input.setPosition(treeOffset);
            TriangleTreeReader.visit(input, visitor, extent.maxX(), extent.maxY());
        }
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
    public double centroidLon() {
        return CoordinateEncoder.GEO.decodeX(encodedCentroidX);
    }

    /** Latitude of the centroid, which for a {@link #isSinglePoint() single point} is the point itself. */
    public double centroidLat() {
        return CoordinateEncoder.GEO.decodeY(encodedCentroidY);
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
        ByteArrayStreamInput input = new ByteArrayStreamInput();
        input.reset(encoded.bytes, encoded.offset, encoded.length);
        int encodedCentroidX = input.readInt();
        int encodedCentroidY = input.readInt();
        DimensionalShapeType shapeType = DimensionalShapeType.readFrom(input);
        input.readVLong(); // sum of centroid weights, not needed for grid functions
        Extent extent = new Extent();
        Extent.readFromCompressed(input, extent);
        int treeOffset = input.getPosition();
        return new GeoShapeDocValues(input, treeOffset, extent, shapeType, encodedCentroidX, encodedCentroidY);
    }

    /**
     * Tests whether this geometry intersects the given {@link Component2D}.
     */
    public boolean intersects(Component2D component) throws IOException {
        Component2DIntersectsVisitor visitor = new Component2DIntersectsVisitor(component, CoordinateEncoder.GEO);
        visitor.reset();
        visit(visitor);
        return visitor.matches();
    }
}
