/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.vectortile.collector;

import com.wdtinc.mapbox_vector_tile.VectorTile;
import com.wdtinc.mapbox_vector_tile.encoding.GeomCmd;
import com.wdtinc.mapbox_vector_tile.encoding.GeomCmdHdr;
import com.wdtinc.mapbox_vector_tile.encoding.ZigZag;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.xpack.spatial.index.fielddata.DimensionalShapeType;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;
import org.elasticsearch.xpack.spatial.index.fielddata.LeafGeoShapeFieldData;
import org.elasticsearch.xpack.spatial.index.fielddata.TriangleTreeReader;
import org.elasticsearch.xpack.spatial.index.fielddata.plain.AbstractLatLonShapeIndexFieldData;
import org.elasticsearch.xpack.spatial.vectortile.VectorTileUtils;
import org.locationtech.jts.geom.Envelope;

import java.util.ArrayList;
import java.util.List;

public class ShapeDocValuesVectorTileCollector extends AbstractVectorTileCollector {

    final AbstractLatLonShapeIndexFieldData.LatLonShapeIndexFieldData shapes;
    private static int extent = 256;
    Rectangle rectangle;
    double height;
    double width;

    public ShapeDocValuesVectorTileCollector(final AbstractLatLonShapeIndexFieldData.LatLonShapeIndexFieldData shapes,
                                      Envelope tileEnvelope,
                                      Rectangle rectangle,
                                      String field) {
        super(tileEnvelope, field, extent);
        this.shapes = shapes;
        this.rectangle = rectangle;
        this.height = (rectangle.maxLat - rectangle.minLat) / extent;
        this.width = (rectangle.maxLon - rectangle.minLon) / extent;
    }

    @Override
    public VectorTileLeafCollector getVectorTileLeafCollector(LeafReaderContext context) {
        Visitor visitor = new Visitor();
        LeafGeoShapeFieldData data = shapes.load(context);
        GeoShapeValues values = data.getGeoShapeValues();
        return (docID -> {
            values.advanceExact(docID);
            GeoShapeValues.GeoShapeValue shapeValue = values.value();
            boolean skip = false;
            if (shapeValue.dimensionalShapeType() != DimensionalShapeType.POINT) {
                double width = shapeValue.boundingBox().maxX() - shapeValue.boundingBox().minX();
                double heigth = shapeValue.boundingBox().maxY() - shapeValue.boundingBox().minY();
                skip = width <= this.width || heigth <= this.height;
            }
            if (skip == false) {
                visitor.reset();
                shapeValue.visit(visitor);
                return visitor.getFeature();
            } else {
                return null;
            }
        });
    }

    private class Visitor implements TriangleTreeReader.Visitor {
        List<Integer> commands = new ArrayList<>();
        final double xScale = 1d / (tileEnvelope.getWidth() / (double) extent);
        final double yScale = -1d / (tileEnvelope.getHeight() / (double) extent);
        final VectorTile.Tile.Feature.Builder featureBuilder = VectorTile.Tile.Feature.newBuilder();

        void reset() {
            commands.clear();
            featureBuilder.clear();
            lastX  = Integer.MIN_VALUE;
            lastY = Integer.MIN_VALUE;
        }

        VectorTile.Tile.Feature.Builder getFeature() {
            featureBuilder.addAllGeometry(commands);
            return featureBuilder;

        }

        @Override
        public void visitPoint(int x, int y) {
            featureBuilder.setType(VectorTile.Tile.GeomType.POINT);
            commands.add(GeomCmdHdr.cmdHdr(GeomCmd.MoveTo, 1));
            add(x, y);
        }

        @Override
        public void visitLine(int aX, int aY, int bX, int bY, byte metadata) {
            featureBuilder.setType(VectorTile.Tile.GeomType.LINESTRING);
            commands.add(GeomCmdHdr.cmdHdr(GeomCmd.MoveTo, 1));
            add(aX, aY);
            commands.add(GeomCmdHdr.cmdHdr(GeomCmd.LineTo, 1));
            add(bX, bY);
        }

        int lastX  = Integer.MIN_VALUE;
        int lastY = Integer.MIN_VALUE;


        @Override
        public void visitTriangle(int aX, int aY, int bX, int bY, int cX, int cY, byte metadata) {
            int aX3 = (int) (xScale *
                (VectorTileUtils.lonToSphericalMercator(GeoEncodingUtils.decodeLongitude(aX))
                    - tileEnvelope.getMinX()));
            int ay3 = (int) (yScale *
                (VectorTileUtils.latToSphericalMercator(GeoEncodingUtils.decodeLatitude(aY))
                    - tileEnvelope.getMinY())) + extent;
            commands.add(GeomCmdHdr.cmdHdr(GeomCmd.MoveTo, 1));
            if (lastX == Integer.MIN_VALUE) {
                featureBuilder.setType(VectorTile.Tile.GeomType.POLYGON);
                commands.add(ZigZag.encode(aX3));
                commands.add(ZigZag.encode(ay3));
            } else {
                commands.add(ZigZag.encode(aX3 - lastX));
                commands.add(ZigZag.encode(ay3 - lastY));
            }
            commands.add(GeomCmdHdr.cmdHdr(GeomCmd.LineTo, 2));
            int bX3 = (int) (xScale *
                (VectorTileUtils.lonToSphericalMercator(GeoEncodingUtils.decodeLongitude(bX))
                    - tileEnvelope.getMinX()));
            int by3 = (int) (yScale *
                (VectorTileUtils.latToSphericalMercator(GeoEncodingUtils.decodeLatitude(bY))
                    - tileEnvelope.getMinY())) + extent;
            commands.add(ZigZag.encode(bX3 - aX3));
            commands.add(ZigZag.encode(by3 - ay3));
            int cX3 = (int) (xScale *
                (VectorTileUtils.lonToSphericalMercator(GeoEncodingUtils.decodeLongitude(cX))
                    - tileEnvelope.getMinX()));
            int cy3 = (int) (yScale *
                (VectorTileUtils.latToSphericalMercator(GeoEncodingUtils.decodeLatitude(cY))
                    - tileEnvelope.getMinY())) + extent;
            commands.add(ZigZag.encode(cX3 - bX3));
            commands.add(ZigZag.encode(cy3 - by3));
            commands.add(GeomCmdHdr.closePathCmdHdr());
            lastX = cX3;
            lastY = cy3;
        }

        private void add(int x, int y) {
            int aX3 = (int) (xScale *
                (VectorTileUtils.lonToSphericalMercator(GeoEncodingUtils.decodeLongitude(x))
                    - tileEnvelope.getMinX()));
            int ay3 = (int) (yScale *
                (VectorTileUtils.latToSphericalMercator(GeoEncodingUtils.decodeLatitude(y))
                    - tileEnvelope.getMinY())) + extent;
            commands.add(ZigZag.encode(aX3));
            commands.add(ZigZag.encode(ay3));
        }

        @Override
        public boolean push() {
            return true;
        }

        @Override
        public boolean pushX(int minX) {
            return true;
        }

        @Override
        public boolean pushY(int minY) {
            return true;
        }

        @Override
        public boolean push(int maxX, int maxY) {
            return true;
        }

        @Override
        public boolean push(int minX, int minY, int maxX, int maxY) {
            return true;
        }
    }
}
