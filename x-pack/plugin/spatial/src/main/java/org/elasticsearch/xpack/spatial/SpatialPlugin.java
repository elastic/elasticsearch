/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.geo.GeoPlugin;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregator;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregatorSupplier;
import org.elasticsearch.search.aggregations.metrics.GeoBoundsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.GeoBoundsAggregatorSupplier;
import org.elasticsearch.search.aggregations.metrics.GeoCentroidAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.GeoCentroidAggregatorSupplier;
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregator;
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregatorSupplier;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.spatial.aggregations.metrics.GeoShapeCentroidAggregator;
import org.elasticsearch.xpack.spatial.index.mapper.GeoShapeWithDocValuesFieldMapper;
import org.elasticsearch.xpack.spatial.index.mapper.PointFieldMapper;
import org.elasticsearch.xpack.spatial.index.mapper.ShapeFieldMapper;
import org.elasticsearch.xpack.spatial.index.query.ShapeQueryBuilder;
import org.elasticsearch.xpack.spatial.ingest.CircleProcessor;
import org.elasticsearch.xpack.spatial.search.aggregations.metrics.GeoShapeBoundsAggregator;
import org.elasticsearch.xpack.spatial.search.aggregations.support.GeoShapeValuesSource;
import org.elasticsearch.xpack.spatial.search.aggregations.support.GeoShapeValuesSourceType;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static java.util.Collections.singletonList;

public class SpatialPlugin extends GeoPlugin implements ActionPlugin, MapperPlugin, SearchPlugin, IngestPlugin {

    // to be overriden by tests
    protected XPackLicenseState getLicenseState() {
        return XPackPlugin.getSharedLicenseState();
    }

    @Override
    public List<ActionPlugin.ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(
            new ActionPlugin.ActionHandler<>(XPackUsageFeatureAction.SPATIAL, SpatialUsageTransportAction.class),
            new ActionPlugin.ActionHandler<>(XPackInfoFeatureAction.SPATIAL, SpatialInfoTransportAction.class));
    }

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        Map<String, Mapper.TypeParser> mappers = new HashMap<>(super.getMappers());
        mappers.put(ShapeFieldMapper.CONTENT_TYPE, new ShapeFieldMapper.TypeParser());
        mappers.put(PointFieldMapper.CONTENT_TYPE, new PointFieldMapper.TypeParser());
        mappers.put(GeoShapeWithDocValuesFieldMapper.CONTENT_TYPE, new GeoShapeWithDocValuesFieldMapper.TypeParser());
        return Collections.unmodifiableMap(mappers);
    }

    @Override
    public List<QuerySpec<?>> getQueries() {
        return singletonList(new QuerySpec<>(ShapeQueryBuilder.NAME, ShapeQueryBuilder::new, ShapeQueryBuilder::fromXContent));
    }

    @Override
    public List<Consumer<ValuesSourceRegistry.Builder>> getAggregationExtentions() {
        return List.of(
            this::registerGeoShapeBoundsAggregator,
            this::registerGeoShapeCentroidAggregator,
            SpatialPlugin::registerValueCountAggregator,
            SpatialPlugin::registerCardinalityAggregator
        );
    }

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        return Map.of(CircleProcessor.TYPE, new CircleProcessor.Factory());
    }

    public void registerGeoShapeBoundsAggregator(ValuesSourceRegistry.Builder builder) {
        builder.register(GeoBoundsAggregationBuilder.NAME, GeoShapeValuesSourceType.instance(),
            (GeoBoundsAggregatorSupplier) (name, aggregationContext, parent, valuesSource, wrapLongitude, metadata)
                -> new GeoShapeBoundsAggregator(name, aggregationContext, parent, (GeoShapeValuesSource) valuesSource,
                wrapLongitude, metadata));
    }

    public void registerGeoShapeCentroidAggregator(ValuesSourceRegistry.Builder builder) {
        builder.register(GeoCentroidAggregationBuilder.NAME, GeoShapeValuesSourceType.instance(),
            (GeoCentroidAggregatorSupplier) (name, aggregationContext, parent, valuesSource, metadata)
                -> {
                if (getLicenseState().isAllowed(XPackLicenseState.Feature.SPATIAL_GEO_CENTROID)) {
                    return new GeoShapeCentroidAggregator(name, aggregationContext, parent, (GeoShapeValuesSource) valuesSource, metadata);
                }
                throw LicenseUtils.newComplianceException("geo_centroid aggregation on geo_shape fields");
            });
    }

    public static void registerValueCountAggregator(ValuesSourceRegistry.Builder builder) {
        builder.register(ValueCountAggregationBuilder.NAME, GeoShapeValuesSourceType.instance(),
            (ValueCountAggregatorSupplier) ValueCountAggregator::new
        );
    }

    public static void registerCardinalityAggregator(ValuesSourceRegistry.Builder builder) {
        builder.register(CardinalityAggregationBuilder.NAME, GeoShapeValuesSourceType.instance(),
            (CardinalityAggregatorSupplier) CardinalityAggregator::new);
    }
}
