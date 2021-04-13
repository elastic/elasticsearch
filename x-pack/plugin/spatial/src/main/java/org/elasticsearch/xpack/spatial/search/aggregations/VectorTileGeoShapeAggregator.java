/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations;

import com.wdtinc.mapbox_vector_tile.adapt.jts.IUserDataConverter;
import com.wdtinc.mapbox_vector_tile.adapt.jts.UserDataIgnoreConverter;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.geo.GeometryParser;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.fieldvisitor.CustomFieldsVisitor;
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
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Aggregator over a geo shape field. It skips shapes where bounding box is smaller
 * than the distance between two pixels. It reads Geometry from source which is slow.
 */
public class VectorTileGeoShapeAggregator extends AbstractVectorTileAggregator {

    private final String fieldName;
    private final double latPointPrecision;
    private final double lonPointPrecision;
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
        this.latPointPrecision = 2 * (rectangle.getMaxLat() - rectangle.getMinLat()) / POINT_EXTENT;
        this.lonPointPrecision = 2 * (rectangle.getMaxLon() - rectangle.getMinLon()) / POINT_EXTENT;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, final LeafBucketCollector sub) {
        final GeoShapeValues values = ((GeoShapeValuesSource) valuesSource).geoShapeValues(ctx);
        final FeatureFactory featureFactory = new FeatureFactory(z, x, y, POLYGON_EXTENT);
        final PointFactory pointFactory = new PointFactory();
        final CustomFieldsVisitor visitor = new CustomFieldsVisitor(Set.of(), true);
        IUserDataConverter ignoreData = new UserDataIgnoreConverter();
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (values.advanceExact(doc)) {
                    GeoShapeValues.GeoShapeValue shapeValue = values.value();
                        switch (shapeValue.dimensionalShapeType()) {
                            case POINT: {
                                visitor.reset();
                                ctx.reader().document(doc, visitor);
                                final SourceLookup lookup = new SourceLookup();
                                lookup.setSource(visitor.source());
                                final Object pointObject = lookup.get(fieldName);
                                if (pointObject != null) {
                                    final List<Point> points = pointFactory.getPoints(parser.parseGeometry(pointObject));
                                    for (Point point : points) {
                                        addPoint(point.getLat(), point.getLon());
                                    }
                                }
                                break;
                            }
                            case LINE: {
                                if (skip(shapeValue)) {
                                    addPoint(shapeValue.lat(), shapeValue.lon());
                                } else {
                                    visitor.reset();
                                    ctx.reader().document(doc, visitor);
                                    final SourceLookup lookup = new SourceLookup();
                                    lookup.setSource(visitor.source());
                                    final Object lines = lookup.get(fieldName);
                                    if (lines != null) {
                                        addLineFeatures(visitor.id(),
                                            featureFactory.getFeatures(parser.parseGeometry(lines), ignoreData));
                                    }
                                }
                                break;
                            }
                            case POLYGON: {
                                if (skip(shapeValue)) {
                                    addPoint(shapeValue.lat(), shapeValue.lon());
                                } else {
                                    visitor.reset();
                                    ctx.reader().document(doc, visitor);
                                    final SourceLookup lookup = new SourceLookup();
                                    lookup.setSource(visitor.source());
                                    final Object polygons = lookup.get(fieldName);
                                    if (polygons != null) {
                                        addPolygonFeatures(visitor.id(),
                                            featureFactory.getFeatures(parser.parseGeometry(polygons), ignoreData));
                                    }
                                }
                                break;
                            }
                        }
                }
            }
        };
    }

    private boolean skip(GeoShapeValues.GeoShapeValue shapeValue) {
        final double width = shapeValue.boundingBox().maxX() - shapeValue.boundingBox().minX();
        final double height = shapeValue.boundingBox().maxY() - shapeValue.boundingBox().minY();
        return width <= this.lonPointPrecision && height <= this.latPointPrecision;
    }
}
