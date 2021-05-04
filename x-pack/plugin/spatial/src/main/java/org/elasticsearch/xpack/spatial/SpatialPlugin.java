/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial;

import org.elasticsearch.Build;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.ContextParser;
import org.elasticsearch.geo.GeoPlugin;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileGridAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregator;
import org.elasticsearch.search.aggregations.metrics.GeoBoundsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.GeoCentroidAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.spatial.action.SpatialStatsAction;
import org.elasticsearch.xpack.spatial.action.SpatialInfoTransportAction;
import org.elasticsearch.xpack.spatial.action.SpatialStatsTransportAction;
import org.elasticsearch.xpack.spatial.action.SpatialUsageTransportAction;
import org.elasticsearch.xpack.spatial.index.mapper.GeoShapeWithDocValuesFieldMapper;
import org.elasticsearch.xpack.spatial.index.mapper.PointFieldMapper;
import org.elasticsearch.xpack.spatial.index.mapper.ShapeFieldMapper;
import org.elasticsearch.xpack.spatial.index.query.ShapeQueryBuilder;
import org.elasticsearch.xpack.spatial.ingest.CircleProcessor;
import org.elasticsearch.xpack.spatial.search.aggregations.GeoLineAggregationBuilder;
import org.elasticsearch.xpack.spatial.search.aggregations.InternalGeoLine;
import org.elasticsearch.xpack.spatial.search.aggregations.InternalVectorTile;
import org.elasticsearch.xpack.spatial.search.aggregations.VectorTileAggregationBuilder;
import org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid.BoundedGeoHashGridTiler;
import org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid.BoundedGeoTileGridTiler;
import org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid.GeoGridTiler;
import org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid.GeoShapeCellIdSource;
import org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid.GeoShapeHashGridAggregator;
import org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid.GeoShapeTileGridAggregator;
import org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid.UnBoundedGeoTileGridTiler;
import org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid.UnboundedGeoHashGridTiler;
import org.elasticsearch.xpack.spatial.search.aggregations.metrics.GeoShapeBoundsAggregator;
import org.elasticsearch.xpack.spatial.search.aggregations.metrics.GeoShapeCentroidAggregator;
import org.elasticsearch.xpack.spatial.search.aggregations.support.GeoShapeValuesSource;
import org.elasticsearch.xpack.spatial.search.aggregations.support.GeoShapeValuesSourceType;
import org.elasticsearch.xpack.spatial.vectortile.RestVectorTileAction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;

public class SpatialPlugin extends GeoPlugin implements ActionPlugin, MapperPlugin, SearchPlugin, IngestPlugin {
    private final SpatialUsage usage = new SpatialUsage();

    // to be overriden by tests
    protected XPackLicenseState getLicenseState() {
        return XPackPlugin.getSharedLicenseState();
    }

    private static final Boolean VECTOR_TILE_FEATURE_FLAG_REGISTERED;

    static {
        final String property = System.getProperty("es.vector_tile_feature_flag_registered");
        if (Build.CURRENT.isSnapshot() && property != null) {
            throw new IllegalArgumentException("es.vector_tile_feature_flag_registered is only supported in non-snapshot builds");
        }
        if ("true".equals(property)) {
            VECTOR_TILE_FEATURE_FLAG_REGISTERED = true;
        } else if ("false".equals(property)) {
            VECTOR_TILE_FEATURE_FLAG_REGISTERED = false;
        } else if (property == null) {
            VECTOR_TILE_FEATURE_FLAG_REGISTERED = null;
        } else {
            throw new IllegalArgumentException(
                "expected es.vector_tile_feature_flag_registered to be unset or [true|false] but was [" + property + "]"
            );
        }
    }

    public boolean isVectorTileEnabled() {
        return Build.CURRENT.isSnapshot() || (VECTOR_TILE_FEATURE_FLAG_REGISTERED != null && VECTOR_TILE_FEATURE_FLAG_REGISTERED);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(
            new ActionHandler<>(XPackUsageFeatureAction.SPATIAL, SpatialUsageTransportAction.class),
            new ActionHandler<>(XPackInfoFeatureAction.SPATIAL, SpatialInfoTransportAction.class),
            new ActionHandler<>(SpatialStatsAction.INSTANCE, SpatialStatsTransportAction.class));
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
                                             IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
                                             IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<DiscoveryNodes> nodesInCluster) {
        if (isVectorTileEnabled()) {
            return List.of(new RestVectorTileAction());
        } else {
            return List.of();
        }
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        FixedExecutorBuilder indexing =
            new FixedExecutorBuilder(settings, "vector_tile_generation", 1, -1, "thread_pool.vectortile", false);
        return Collections.singletonList(indexing);
    }

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        Map<String, Mapper.TypeParser> mappers = new HashMap<>(super.getMappers());
        mappers.put(ShapeFieldMapper.CONTENT_TYPE, ShapeFieldMapper.PARSER);
        mappers.put(PointFieldMapper.CONTENT_TYPE, PointFieldMapper.PARSER);
        mappers.put(GeoShapeWithDocValuesFieldMapper.CONTENT_TYPE, GeoShapeWithDocValuesFieldMapper.PARSER);
        return Collections.unmodifiableMap(mappers);
    }

    @Override
    public List<QuerySpec<?>> getQueries() {
        return singletonList(new QuerySpec<>(ShapeQueryBuilder.NAME, ShapeQueryBuilder::new, ShapeQueryBuilder::fromXContent));
    }

    @Override
    public List<Consumer<ValuesSourceRegistry.Builder>> getAggregationExtentions() {
        return List.of(
            this::registerGeoShapeCentroidAggregator,
            this::registerGeoShapeGridAggregators,
            SpatialPlugin::registerGeoShapeBoundsAggregator,
            SpatialPlugin::registerValueCountAggregator,
            SpatialPlugin::registerCardinalityAggregator
        );
    }

    @Override
    public List<AggregationSpec> getAggregations() {
        List<AggregationSpec> aggSpecs = new ArrayList<>();
        aggSpecs.add(
            new AggregationSpec(
                GeoLineAggregationBuilder.NAME,
                GeoLineAggregationBuilder::new,
                usage.track(SpatialStatsAction.Item.GEOLINE,
                    checkLicense(GeoLineAggregationBuilder.PARSER, XPackLicenseState.Feature.SPATIAL_GEO_LINE)))
                .addResultReader(InternalGeoLine::new)
                .setAggregatorRegistrar(GeoLineAggregationBuilder::registerUsage)

        );
        if (isVectorTileEnabled()) {
            aggSpecs.add(new AggregationSpec(VectorTileAggregationBuilder.NAME,
                VectorTileAggregationBuilder::new,
                VectorTileAggregationBuilder.PARSER)
                //  usage.track(SpatialStatsAction.Item.GEOLINE,
                //      checkLicense(VectorTileAggregationBuilder.PARSER, XPackLicenseState.Feature.SPATIAL_GEO_LINE)))
                .addResultReader(InternalVectorTile::new).setAggregatorRegistrar(VectorTileAggregationBuilder::registerAggregators));
        }
        return aggSpecs;
    }

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        return Map.of(CircleProcessor.TYPE, new CircleProcessor.Factory());
    }

    private static void registerGeoShapeBoundsAggregator(ValuesSourceRegistry.Builder builder) {
        builder.register(
            GeoBoundsAggregationBuilder.REGISTRY_KEY,
            GeoShapeValuesSourceType.instance(),
            GeoShapeBoundsAggregator::new,
            true
        );
    }

    private void registerGeoShapeCentroidAggregator(ValuesSourceRegistry.Builder builder) {
        builder.register(GeoCentroidAggregationBuilder.REGISTRY_KEY, GeoShapeValuesSourceType.instance(),
            (name, valuesSourceConfig, context, parent, metadata)
                -> {
                if (getLicenseState().checkFeature(XPackLicenseState.Feature.SPATIAL_GEO_CENTROID)) {
                    return new GeoShapeCentroidAggregator(name, context, parent, valuesSourceConfig, metadata);
                }
                throw LicenseUtils.newComplianceException("geo_centroid aggregation on geo_shape fields");
            },
            true
        );
    }

    private void registerGeoShapeGridAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(GeoHashGridAggregationBuilder.REGISTRY_KEY, GeoShapeValuesSourceType.instance(),
            (name, factories, valuesSource, precision, geoBoundingBox, requiredSize, shardSize,
             aggregationContext, parent, collectsFromSingleBucket, metadata) -> {
                if (getLicenseState().checkFeature(XPackLicenseState.Feature.SPATIAL_GEO_GRID)) {
                    final GeoGridTiler tiler;
                    if (geoBoundingBox.isUnbounded()) {
                        tiler = new UnboundedGeoHashGridTiler(precision);
                    } else {
                        tiler = new BoundedGeoHashGridTiler(precision, geoBoundingBox);
                    }
                    GeoShapeCellIdSource cellIdSource = new GeoShapeCellIdSource((GeoShapeValuesSource) valuesSource, tiler);
                    GeoShapeHashGridAggregator agg = new GeoShapeHashGridAggregator(name, factories, cellIdSource, requiredSize, shardSize,
                        aggregationContext, parent, collectsFromSingleBucket, metadata);
                    // this would ideally be something set in an immutable way on the ValuesSource
                    cellIdSource.setCircuitBreakerConsumer(agg::addRequestBytes);
                    return agg;
                }
                throw LicenseUtils.newComplianceException("geohash_grid aggregation on geo_shape fields");
            },
            true
        );

        builder.register(GeoTileGridAggregationBuilder.REGISTRY_KEY, GeoShapeValuesSourceType.instance(),
            (name, factories, valuesSource, precision, geoBoundingBox, requiredSize, shardSize,
             context, parent, collectsFromSingleBucket, metadata) -> {
                if (getLicenseState().checkFeature(XPackLicenseState.Feature.SPATIAL_GEO_GRID)) {
                    final GeoGridTiler tiler;
                    if (geoBoundingBox.isUnbounded()) {
                        tiler = new UnBoundedGeoTileGridTiler(precision);
                    } else {
                        tiler = new BoundedGeoTileGridTiler(precision, geoBoundingBox);
                    }
                    GeoShapeCellIdSource cellIdSource = new GeoShapeCellIdSource((GeoShapeValuesSource) valuesSource, tiler);
                    GeoShapeTileGridAggregator agg = new GeoShapeTileGridAggregator(name, factories, cellIdSource, requiredSize, shardSize,
                        context, parent, collectsFromSingleBucket, metadata);
                    // this would ideally be something set in an immutable way on the ValuesSource
                    cellIdSource.setCircuitBreakerConsumer(agg::addRequestBytes);
                    return agg;
                }
                throw LicenseUtils.newComplianceException("geotile_grid aggregation on geo_shape fields");
            },
            true
        );
    }

    private static void registerValueCountAggregator(ValuesSourceRegistry.Builder builder) {
        builder.register(ValueCountAggregationBuilder.REGISTRY_KEY, GeoShapeValuesSourceType.instance(), ValueCountAggregator::new, true);
    }

    private static void registerCardinalityAggregator(ValuesSourceRegistry.Builder builder) {
        builder.register(CardinalityAggregationBuilder.REGISTRY_KEY, GeoShapeValuesSourceType.instance(), CardinalityAggregator::new, true);
    }

    private <T> ContextParser<String, T> checkLicense(ContextParser<String, T> realParser, XPackLicenseState.Feature feature) {
        return (parser, name) -> {
            if (getLicenseState().checkFeature(feature) == false) {
                throw LicenseUtils.newComplianceException(feature.name());
            }
            return realParser.parse(parser, name);
        };
    }
}
