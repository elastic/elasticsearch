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
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.geo.SimpleWKTShapeParser;
import org.apache.lucene.geo.XYPolygon;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.index.fieldvisitor.SingleFieldsVisitor;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.xpack.spatial.index.fielddata.DimensionalShapeType;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;
import org.elasticsearch.xpack.spatial.index.fielddata.LeafGeoShapeFieldData;
import org.elasticsearch.xpack.spatial.index.fielddata.plain.AbstractLatLonShapeIndexFieldData;
import org.elasticsearch.xpack.spatial.vectortile.VectorTileUtils;
import org.locationtech.jts.geom.Envelope;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

public class ShapeDocValuesVectorTileCollector extends AbstractVectorTileCollector {

    final AbstractLatLonShapeIndexFieldData.LatLonShapeIndexFieldData shapes;
    final MappedFieldType sourceField;
    private static int extent = 4096;
    Rectangle rectangle;
    double height;
    double width;

    public ShapeDocValuesVectorTileCollector(final AbstractLatLonShapeIndexFieldData.LatLonShapeIndexFieldData shapes,
                                      Envelope tileEnvelope,
                                      Rectangle rectangle,
                                      String field,
                                      MappedFieldType sourceField) {
        super(tileEnvelope, field, extent);
        this.shapes = shapes;
        this.rectangle = rectangle;
        this.height = 3 * (rectangle.maxLat - rectangle.minLat) / extent;
        this.width = 3 * (rectangle.maxLon - rectangle.minLon) / extent;
        this.sourceField = sourceField;
    }

    @Override
    public VectorTileLeafCollector getVectorTileLeafCollector(LeafReaderContext context) {
        final double xScale = 1d / (tileEnvelope.getWidth() / (double) extent);
        final double yScale = -1d / (tileEnvelope.getHeight() / (double) extent);
        final VectorTile.Tile.Feature.Builder featureBuilder = VectorTile.Tile.Feature.newBuilder();
        final List<Integer> commands = new ArrayList<>();
        final List<Object> fieldVisitorCollector = new ArrayList<>();
        final SingleFieldsVisitor visitor = new SingleFieldsVisitor(sourceField, fieldVisitorCollector);
        final LeafGeoShapeFieldData data = shapes.load(context);
        final GeoShapeValues values = data.getGeoShapeValues();
        return (docID -> {
            values.advanceExact(docID);
            GeoShapeValues.GeoShapeValue shapeValue = values.value();
            boolean skip = skip(shapeValue);
            if (skip) {
                return null;
            } else {
                featureBuilder.clear();
                commands.clear();
                fieldVisitorCollector.clear();
                context.reader().document(docID, visitor);
                XYPolygon p = toMVTPol(getPolygon((BytesRef) fieldVisitorCollector.get(0)), tileEnvelope, xScale, yScale);
                featureBuilder.setType(VectorTile.Tile.GeomType.POLYGON);
                commands.add(GeomCmdHdr.cmdHdr(GeomCmd.MoveTo, 1));
                commands.add(ZigZag.encode((int) p.getPolyX(0)));
                commands.add(ZigZag.encode((int) p.getPolyY(0)));
                commands.add(0); // placeHolder
                int numPoints = 0;
                for (int i = 1; i < p.numPoints() - 1; i++) {
                    if ((int) p.getPolyX(i) != (int) p.getPolyX(i - 1) || (int) p.getPolyY(i) != (int) p.getPolyY(i - 1)) {
                        commands.add(ZigZag.encode((int) p.getPolyX(i) - (int) p.getPolyX(i - 1)));
                        commands.add(ZigZag.encode((int) p.getPolyY(i) - (int) p.getPolyY(i - 1)));
                        numPoints++;
                    }
                }
                commands.set(3, GeomCmdHdr.cmdHdr(GeomCmd.LineTo, numPoints));
                //closePath
                commands.add(GeomCmdHdr.closePathCmdHdr());
                featureBuilder.addAllGeometry(commands);
                layerBuilder.addFeatures(featureBuilder);
                return featureBuilder;
            }
        });
    }

    private Polygon getPolygon(BytesRef bytes) throws IOException {
        final SourceLookup lookup = new SourceLookup();
        lookup.setSource(new BytesArray(bytes));
        final Object o;
        try {
            o = SimpleWKTShapeParser.parse((String) lookup.get(field));
        } catch (ParseException p) {
            throw new IOException(p);
        }
        // we only support single polygons
        if (o instanceof Polygon) {
            return (Polygon) o;
        } else if (o instanceof Polygon[]) {
            return ((Polygon[]) o)[0];
        } else {
            throw new IllegalArgumentException("Unsupported shape");
        }
    }

    private boolean skip(GeoShapeValues.GeoShapeValue shapeValue) {
        if (shapeValue.dimensionalShapeType() != DimensionalShapeType.POINT) {
            double width = shapeValue.boundingBox().maxX() - shapeValue.boundingBox().minX();
            double height = shapeValue.boundingBox().maxY() - shapeValue.boundingBox().minY();
            return width <= this.width || height <= this.height;
        }
        return false;
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
