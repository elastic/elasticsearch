/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.vectortile.collector;

import com.wdtinc.mapbox_vector_tile.encoding.ZigZag;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.SimpleWKTShapeParser;
import org.apache.lucene.geo.XYPolygon;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.index.fieldvisitor.SingleFieldsVisitor;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.xpack.spatial.proto.VectorTile;
import org.elasticsearch.xpack.spatial.vectortile.VectorTileUtils;
import org.locationtech.jts.geom.Envelope;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ShapeVectorTileCollector extends AbstractVectorTileCollector {

    final MappedFieldType sourceField;
    private static int extent = 256;

    ShapeVectorTileCollector(final MappedFieldType sourceField, Envelope tileEnvelope, String field) {
        super(tileEnvelope, field, extent);
        this.sourceField = sourceField;
    }

    @Override
    public VectorTileLeafCollector getVectorTileLeafCollector(LeafReaderContext context) {

        final double xScale = 1d / (tileEnvelope.getWidth() / (double) extent);
        final double yScale = -1d / (tileEnvelope.getHeight() / (double) extent);
        final VectorTile.Tile.Feature.Builder featureBuilder = VectorTile.Tile.Feature.newBuilder();
        List<Integer> commands = new ArrayList<>();
        return (docID -> {
            featureBuilder.clear();
            commands.clear();
            featureBuilder.setType(VectorTile.Tile.GeomType.POINT);
            final List<Object> values = new ArrayList<>();
            final SingleFieldsVisitor visitor = new SingleFieldsVisitor(sourceField, values);
            context.reader().document(docID, visitor);
            final SourceLookup lookup = new SourceLookup();
            lookup.setSource(new BytesArray((BytesRef) values.get(0)));
            try {
                Object o = SimpleWKTShapeParser.parse((String) lookup.get(field));
                Polygon px;
                if (o instanceof Polygon) {
                    px = (Polygon) o;
                } else {
                    px = ((Polygon[]) o)[0];
                }
                XYPolygon p = toMVTPol(px, tileEnvelope, xScale, yScale);
                if ( p.maxX - p.minX <= 1 && p.maxY - p.minY <= 1) {
                    return null;
                }
                featureBuilder.setType(VectorTile.Tile.GeomType.POLYGON);
                commands.add(com.wdtinc.mapbox_vector_tile.encoding.GeomCmdHdr.cmdHdr
                    (com.wdtinc.mapbox_vector_tile.encoding.GeomCmd.MoveTo, 1));
                commands.add(ZigZag.encode((int) p.getPolyX(0)));
                commands.add(ZigZag.encode((int) p.getPolyY(0)));
                commands.add(com.wdtinc.mapbox_vector_tile.encoding.GeomCmdHdr.cmdHdr
                    (com.wdtinc.mapbox_vector_tile.encoding.GeomCmd.LineTo, p.numPoints() - 2));
                for (int i = 1; i < p.numPoints() - 1; i++) {
                    if ((int)p.getPolyX(i) != (int)p.getPolyX(i - 1) || (int)p.getPolyY(i) != (int)p.getPolyY(i - 1)) {
                        commands.add(ZigZag.encode((int) p.getPolyX(i) - (int) p.getPolyX(i - 1)));
                        commands.add(ZigZag.encode((int) p.getPolyY(i) - (int) p.getPolyY(i - 1)));
                    }
                }
                //closePath
                commands.add(com.wdtinc.mapbox_vector_tile.encoding.GeomCmdHdr.closePathCmdHdr());
                featureBuilder.addAllGeometry(commands);
                layerBuilder.addFeatures(featureBuilder);
            } catch (Exception p) {
                throw new IOException(p);
            }
            return featureBuilder;
        });
    }

    public static XYPolygon toMVTPol(Polygon p, Envelope tile, double xScale, double yScale) {
        // Transform Setup: Scale X and Y to tile extent values, flip Y values
        float[] xs = new float[p.numPoints()];
        float[] ys = new float[p.numPoints()];
        for (int i = 0; i < p.numPoints(); i++) {
            xs[p.numPoints() - 1 - i] =
                (float) (xScale * (VectorTileUtils.lonToSphericalMercator(p.getPolyLon(i)) - tile.getMinX()));
            ys[p.numPoints() - 1 - i] =
                (float) (yScale * (VectorTileUtils.latToSphericalMercator(p.getPolyLat(i)) - tile.getMinY())) + extent;
        }
        XYPolygon[] holes = new XYPolygon[p.numHoles()];
        for (int i = 0; i < p.numHoles(); i++) {
            holes[i] = toMVTPol(p.getHoles()[i], tile, xScale, yScale);
        }
        return new XYPolygon(xs, ys, holes);
    }
}
