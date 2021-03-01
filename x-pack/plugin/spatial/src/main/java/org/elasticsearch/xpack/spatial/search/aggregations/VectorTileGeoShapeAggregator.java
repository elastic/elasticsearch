/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.geo.GeometryParser;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.fieldvisitor.SingleFieldsVisitor;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;
import org.elasticsearch.xpack.spatial.search.aggregations.support.GeoShapeValuesSource;
import org.elasticsearch.xpack.spatial.vectortile.FeatureFactory;
import org.elasticsearch.xpack.spatial.vectortile.PointFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Aggregator over a geo shape field. It skips shapes where bounding box is smaller
 * than the distance between two pixels. It reads Geometry from source which is slow.
 */
public class VectorTileGeoShapeAggregator extends AbstractVectorTileAggregator {

    private final String fieldName;
    private final double latPolygonPrecision;
    private final double lonPolygonPrecision;
    private final double latLinePrecision;
    private final double lonLinePrecision;
    private final MappedFieldType sourceField;
    private final GeometryParser parser = new GeometryParser(true, false, false);

    public VectorTileGeoShapeAggregator(
        String name,
        ValuesSourceConfig valuesSourceConfig,
        int z,
        int x,
        int y,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, valuesSourceConfig, z, x, y, context, parent, metadata);
        this.sourceField = context.getFieldType(SourceFieldMapper.NAME);
        this.fieldName = valuesSourceConfig.fieldType().name();
        Rectangle rectangle = GeoTileUtils.toBoundingBox(x, y, z);
        // TODO: Reason the logic for when we should skip a complex geometry
        this.latPolygonPrecision = 5 * (rectangle.getMaxLat() - rectangle.getMinLat()) / POLYGON_EXTENT;
        this.lonPolygonPrecision = 5 * (rectangle.getMaxLon() - rectangle.getMinLon()) / POLYGON_EXTENT;
        this.latLinePrecision = 5 * (rectangle.getMaxLat() - rectangle.getMinLat()) / LINE_EXTENT;
        this.lonLinePrecision = 5 * (rectangle.getMaxLon() - rectangle.getMinLon()) / LINE_EXTENT;

    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, final LeafBucketCollector sub) {
        final GeoShapeValues values = ((GeoShapeValuesSource) valuesSource).geoShapeValues(ctx);
        final FeatureFactory featureFactory = new FeatureFactory(z, x, y, POLYGON_EXTENT);
        final PointFactory pointFactory = new PointFactory();
        final List<Object> fieldVisitorCollector = new ArrayList<>();
        final SingleFieldsVisitor visitor = new SingleFieldsVisitor(sourceField, fieldVisitorCollector);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (values.advanceExact(doc)) {
                    GeoShapeValues.GeoShapeValue shapeValue = values.value();
                        switch (shapeValue.dimensionalShapeType()) {
                            case POINT:
                                List<Point> points = pointFactory.getPoints(getGeometry(ctx, doc, fieldVisitorCollector, visitor));
                                for(Point point : points) {
                                    addPoint(point.getLat(), point.getLon());
                                }
                                break;
                            case LINE:
                                if (skipLine(shapeValue) == false) {
                                    addLineFeatures(featureFactory.getFeatures(getGeometry(ctx, doc, fieldVisitorCollector, visitor)));
                                }
                                break;
                            case POLYGON:
                                if (skipPolygon(shapeValue) == false) {
                                    addPolygonFeatures(featureFactory.getFeatures(getGeometry(ctx, doc, fieldVisitorCollector, visitor)));
                                }
                                break;
                        }
                }
            }
        };
    }

    private boolean skipPolygon(GeoShapeValues.GeoShapeValue shapeValue) {
        final double width = shapeValue.boundingBox().maxX() - shapeValue.boundingBox().minX();
        final double height = shapeValue.boundingBox().maxY() - shapeValue.boundingBox().minY();
        return width <= this.lonPolygonPrecision && height <= this.latPolygonPrecision;
    }

    private boolean skipLine(GeoShapeValues.GeoShapeValue shapeValue) {
        final double width = shapeValue.boundingBox().maxX() - shapeValue.boundingBox().minX();
        final double height = shapeValue.boundingBox().maxY() - shapeValue.boundingBox().minY();
        return width <= this.lonLinePrecision && height <= this.latLinePrecision;
    }

    private Geometry getGeometry(LeafReaderContext ctx,
                                  int doc,
                                  List<Object> fieldVisitorCollector,
                                  SingleFieldsVisitor visitor) throws IOException {
        // TODO: Incomplete as we should vbe calling prepareForIndexing.
        // Already pretty slow, can we add this info to the doc value?
        fieldVisitorCollector.clear();
        ctx.reader().document(doc, visitor);
        final SourceLookup lookup = new SourceLookup();
        lookup.setSource(new BytesArray((BytesRef) fieldVisitorCollector.get(0)));
        return parser.parseGeometry(lookup.get(fieldName));
    }

}
