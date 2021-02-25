/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations;

import com.wdtinc.mapbox_vector_tile.VectorTile;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.geo.GeometryParser;
import org.elasticsearch.geometry.Geometry;
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
import org.elasticsearch.xpack.spatial.index.fielddata.DimensionalShapeType;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;
import org.elasticsearch.xpack.spatial.search.aggregations.support.GeoShapeValuesSource;
import org.elasticsearch.xpack.spatial.vectortile.FeatureFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class VectorTileGeoShapeAggregator extends AbstractVectorTileAggregator {

    private static final int extent = 4096;
    private final String fieldName;
    private final double latPrecision;
    private final double lonPrecision;
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
        super(name, valuesSourceConfig, z, x, y, extent, context, parent, metadata);
        Rectangle rectangle = GeoTileUtils.toBoundingBox(x, y, z);
        this.latPrecision = 2 * (rectangle.getMaxLat() - rectangle.getMinLat()) / extent;
        this.lonPrecision = 2 * (rectangle.getMaxLon() - rectangle.getMinLon()) / extent;
        this.sourceField = context.getFieldType(SourceFieldMapper.NAME);
        this.fieldName = valuesSourceConfig.fieldType().name();
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, final LeafBucketCollector sub) {
        final GeoShapeValues values = ((GeoShapeValuesSource) valuesSource).geoShapeValues(ctx);
        final FeatureFactory featureFactory = new FeatureFactory(z, x, y, extent);
        final List<Object> fieldVisitorCollector = new ArrayList<>();
        final SingleFieldsVisitor visitor = new SingleFieldsVisitor(sourceField, fieldVisitorCollector);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (values.advanceExact(doc)) {
                    GeoShapeValues.GeoShapeValue shapeValue = values.value();
                    boolean skip = skip(shapeValue);
                    if (skip == false) {
                        fieldVisitorCollector.clear();
                        ctx.reader().document(doc, visitor);
                        Geometry geometry = getGeometry((BytesRef) fieldVisitorCollector.get(0));
                        List<VectorTile.Tile.Feature> features = featureFactory.getFeatures(geometry);
                        for (VectorTile.Tile.Feature feature : features) {
                            layerBuilder.addFeatures(feature);
                        }
                    }
                }
            }
        };
    }

    private boolean skip(GeoShapeValues.GeoShapeValue shapeValue) {
        if (shapeValue.dimensionalShapeType() != DimensionalShapeType.POINT) {
            final double width = shapeValue.boundingBox().maxX() - shapeValue.boundingBox().minX();
            final double height = shapeValue.boundingBox().maxY() - shapeValue.boundingBox().minY();
            return width <= this.lonPrecision && height <= this.latPrecision;
        }
        return false;
    }

    private Geometry getGeometry(BytesRef bytes) {
        final SourceLookup lookup = new SourceLookup();
        lookup.setSource(new BytesArray(bytes));
        return parser.parseGeometry(lookup.get(fieldName));
    }
}
