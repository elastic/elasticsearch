/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.apache.lucene.document.ShapeField;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.lucene.spatial.CentroidCalculator;
import org.elasticsearch.lucene.spatial.Component2DVisitor;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;
import org.elasticsearch.lucene.spatial.GeometryDocValueReader;
import org.elasticsearch.lucene.spatial.GeometryDocValueWriter;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;

/**
 * Wraps a geo_shape WKB as a triangle-tree for intersection testing against grid cells.
 * All types are from server/Lucene — no dependency on the spatial plugin.
 */
class GeoShapeDocValues {
    private final GeometryDocValueReader reader;
    final double minLon;
    final double maxLon;
    final double minLat;
    final double maxLat;

    private GeoShapeDocValues(GeometryDocValueReader reader, double minLon, double maxLon, double minLat, double maxLat) {
        this.reader = reader;
        this.minLon = minLon;
        this.maxLon = maxLon;
        this.minLat = minLat;
        this.maxLat = maxLat;
    }

    /**
     * Parses a WKB-encoded geometry into a triangle-tree representation suitable for
     * intersection testing. The bounding box is extracted from the encoded extent.
     */
    static GeoShapeDocValues from(BytesRef wkb, GeoShapeIndexer indexer) throws IOException {
        Geometry geometry = GEO.wkbToGeometry(wkb);
        CentroidCalculator centroidCalculator = new CentroidCalculator();
        centroidCalculator.add(geometry);
        BytesRef triangleBytes = GeometryDocValueWriter.write(indexer.indexShape(geometry), CoordinateEncoder.GEO, centroidCalculator);
        GeometryDocValueReader reader = new GeometryDocValueReader();
        reader.reset(triangleBytes);
        var extent = reader.getExtent();
        double minLon = CoordinateEncoder.GEO.decodeX(extent.minX());
        double maxLon = CoordinateEncoder.GEO.decodeX(extent.maxX());
        double minLat = CoordinateEncoder.GEO.decodeY(extent.minY());
        double maxLat = CoordinateEncoder.GEO.decodeY(extent.maxY());
        return new GeoShapeDocValues(reader, minLon, maxLon, minLat, maxLat);
    }

    /**
     * Tests whether this geometry intersects the given {@link Component2D}.
     */
    boolean intersects(Component2D component) throws IOException {
        Component2DVisitor visitor = Component2DVisitor.getVisitor(component, ShapeField.QueryRelation.INTERSECTS, CoordinateEncoder.GEO);
        visitor.reset();
        reader.visit(visitor);
        return visitor.matches();
    }
}
