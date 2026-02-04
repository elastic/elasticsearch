/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.geo.GeoFormatterFactory;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.RuntimeField;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGridAggregator;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileGridAggregator;
import org.elasticsearch.search.aggregations.metrics.GeoBoundsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.GeoCentroidAggregationBuilder;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.xcontent.ContextParser;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.spatial.action.SpatialStatsAction;
import org.elasticsearch.xpack.spatial.action.SpatialInfoTransportAction;
import org.elasticsearch.xpack.spatial.action.SpatialStatsTransportAction;
import org.elasticsearch.xpack.spatial.action.SpatialUsageTransportAction;
import org.elasticsearch.xpack.spatial.common.CartesianBoundingBox;
import org.elasticsearch.xpack.spatial.index.fielddata.CartesianShapeValues;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;
import org.elasticsearch.xpack.spatial.index.mapper.GeoShapeScriptFieldType;
import org.elasticsearch.xpack.spatial.index.mapper.GeoShapeWithDocValuesFieldMapper;
import org.elasticsearch.xpack.spatial.index.mapper.PointFieldMapper;
import org.elasticsearch.xpack.spatial.index.mapper.ShapeFieldMapper;
import org.elasticsearch.xpack.spatial.index.query.GeoGridQueryBuilder;
import org.elasticsearch.xpack.spatial.index.query.ShapeQueryBuilder;
import org.elasticsearch.xpack.spatial.ingest.CircleProcessor;
import org.elasticsearch.xpack.spatial.ingest.GeoGridProcessor;
import org.elasticsearch.xpack.spatial.search.aggregations.GeoLineAggregationBuilder;
import org.elasticsearch.xpack.spatial.search.aggregations.InternalGeoLine;
import org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid.GeoGridTiler;
import org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid.GeoHashGridTiler;
import org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid.GeoHexCellIdSource;
import org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid.GeoHexGridAggregationBuilder;
import org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid.GeoHexGridAggregator;
import org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid.GeoHexGridTiler;
import org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid.GeoShapeCellIdSource;
import org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid.GeoTileGridTiler;
import org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid.InternalGeoHexGrid;
import org.elasticsearch.xpack.spatial.search.aggregations.metrics.CartesianBoundsAggregationBuilder;
import org.elasticsearch.xpack.spatial.search.aggregations.metrics.CartesianBoundsAggregator;
import org.elasticsearch.xpack.spatial.search.aggregations.metrics.CartesianCentroidAggregationBuilder;
import org.elasticsearch.xpack.spatial.search.aggregations.metrics.CartesianCentroidAggregator;
import org.elasticsearch.xpack.spatial.search.aggregations.metrics.CartesianShapeBoundsAggregator;
import org.elasticsearch.xpack.spatial.search.aggregations.metrics.CartesianShapeCentroidAggregator;
import org.elasticsearch.xpack.spatial.search.aggregations.metrics.GeoShapeBoundsAggregator;
import org.elasticsearch.xpack.spatial.search.aggregations.metrics.GeoShapeCentroidAggregator;
import org.elasticsearch.xpack.spatial.search.aggregations.metrics.InternalCartesianBounds;
import org.elasticsearch.xpack.spatial.search.aggregations.metrics.InternalCartesianCentroid;
import org.elasticsearch.xpack.spatial.search.aggregations.support.CartesianPointValuesSourceType;
import org.elasticsearch.xpack.spatial.search.aggregations.support.CartesianShapeValuesSourceType;
import org.elasticsearch.xpack.spatial.search.aggregations.support.GeoShapeValuesSource;
import org.elasticsearch.xpack.spatial.search.aggregations.support.GeoShapeValuesSourceType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class SpatialPlugin extends Plugin implements ActionPlugin, MapperPlugin, SearchPlugin, IngestPlugin, ExtensiblePlugin {
    private final SpatialUsage usage = new SpatialUsage();

    private final LicensedFeature.Momentary GEO_CENTROID_AGG_FEATURE = LicensedFeature.momentary(
        "spatial",
        "geo-centroid-agg",
        License.OperationMode.GOLD
    );
    private final LicensedFeature.Momentary CARTESIAN_CENTROID_AGG_FEATURE = LicensedFeature.momentary(
        "spatial",
        "cartesian-centroid-agg",
        License.OperationMode.GOLD
    );
    private final LicensedFeature.Momentary GEO_GRID_AGG_FEATURE = LicensedFeature.momentary(
        "spatial",
        "geo-grid-agg",
        License.OperationMode.GOLD
    );
    private final LicensedFeature.Momentary GEO_LINE_AGG_FEATURE = LicensedFeature.momentary(
        "spatial",
        "geo-line-agg",
        License.OperationMode.GOLD
    );

    private final LicensedFeature.Momentary GEO_HEX_AGG_FEATURE = LicensedFeature.momentary(
        "spatial",
        "geo-hex-agg",
        License.OperationMode.GOLD
    );

    // to be overriden by tests
    protected XPackLicenseState getLicenseState() {
        return XPackPlugin.getSharedLicenseState();
    }

    // register the vector tile factory from a different module
    private final SetOnce<GeoFormatterFactory<Geometry>> geoFormatterFactory = new SetOnce<>();

    @Override
    public List<ActionPlugin.ActionHandler> getActions() {
        return List.of(
            new ActionPlugin.ActionHandler(XPackUsageFeatureAction.SPATIAL, SpatialUsageTransportAction.class),
            new ActionPlugin.ActionHandler(XPackInfoFeatureAction.SPATIAL, SpatialInfoTransportAction.class),
            new ActionPlugin.ActionHandler(SpatialStatsAction.INSTANCE, SpatialStatsTransportAction.class)
        );
    }

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return Map.of(
            ShapeFieldMapper.CONTENT_TYPE,
            ShapeFieldMapper.PARSER,
            PointFieldMapper.CONTENT_TYPE,
            PointFieldMapper.PARSER,
            GeoShapeWithDocValuesFieldMapper.CONTENT_TYPE,
            new GeoShapeWithDocValuesFieldMapper.TypeParser(geoFormatterFactory.get())
        );
    }

    @Override
    public Map<String, RuntimeField.Parser> getRuntimeFields() {
        return Map.of(GeoShapeWithDocValuesFieldMapper.CONTENT_TYPE, GeoShapeScriptFieldType.typeParser(geoFormatterFactory.get()));
    }

    @Override
    public List<QuerySpec<?>> getQueries() {
        return List.of(
            new QuerySpec<>(ShapeQueryBuilder.NAME, ShapeQueryBuilder::new, ShapeQueryBuilder::fromXContent),
            new QuerySpec<>(GeoGridQueryBuilder.NAME, GeoGridQueryBuilder::new, GeoGridQueryBuilder::fromXContent)
        );
    }

    @Override
    public List<Consumer<ValuesSourceRegistry.Builder>> getAggregationExtentions() {
        return List.of(
            this::registerGeoShapeCentroidAggregator,
            this::registerGeoShapeGridAggregators,
            SpatialPlugin::registerGeoShapeBoundsAggregator
        );
    }

    @Override
    public List<AggregationSpec> getAggregations() {
        return List.of(
            new AggregationSpec(
                GeoLineAggregationBuilder.NAME,
                GeoLineAggregationBuilder::new,
                usage.track(SpatialStatsAction.Item.GEOLINE, checkLicense(GeoLineAggregationBuilder.PARSER, GEO_LINE_AGG_FEATURE))
            ).addResultReader(InternalGeoLine::new).setAggregatorRegistrar(GeoLineAggregationBuilder::registerUsage),
            new AggregationSpec(
                GeoHexGridAggregationBuilder.NAME,
                GeoHexGridAggregationBuilder::new,
                usage.track(SpatialStatsAction.Item.GEOHEX, checkLicense(GeoHexGridAggregationBuilder.PARSER, GEO_HEX_AGG_FEATURE))
            ).addResultReader(InternalGeoHexGrid::new).setAggregatorRegistrar(this::registerGeoHexGridAggregator),
            new AggregationSpec(
                CartesianCentroidAggregationBuilder.NAME,
                CartesianCentroidAggregationBuilder::new,
                usage.track(SpatialStatsAction.Item.CARTESIANCENTROID, CartesianCentroidAggregationBuilder.PARSER)
            ).addResultReader(InternalCartesianCentroid::new).setAggregatorRegistrar(this::registerCartesianCentroidAggregator),
            new AggregationSpec(
                CartesianBoundsAggregationBuilder.NAME,
                CartesianBoundsAggregationBuilder::new,
                usage.track(SpatialStatsAction.Item.CARTESIANBOUNDS, CartesianBoundsAggregationBuilder.PARSER)
            ).addResultReader(InternalCartesianBounds::new).setAggregatorRegistrar(SpatialPlugin::registerCartesianBoundsAggregators)
        );
    }

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        return Map.of(CircleProcessor.TYPE, new CircleProcessor.Factory(), GeoGridProcessor.TYPE, new GeoGridProcessor.Factory());
    }

    @Override
    public List<GenericNamedWriteableSpec> getGenericNamedWriteables() {
        return List.of(
            new GenericNamedWriteableSpec("CartesianBoundingBox", CartesianBoundingBox::new),
            new GenericNamedWriteableSpec("GeoShapeValue", GeoShapeValues.GeoShapeValue::new),
            new GenericNamedWriteableSpec("CartesianShapeValue", CartesianShapeValues.CartesianShapeValue::new)
        );
    }

    private static void registerGeoShapeBoundsAggregator(ValuesSourceRegistry.Builder builder) {
        builder.register(
            GeoBoundsAggregationBuilder.REGISTRY_KEY,
            GeoShapeValuesSourceType.instance(),
            GeoShapeBoundsAggregator::new,
            true
        );
    }

    private static void registerCartesianBoundsAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(
            CartesianBoundsAggregationBuilder.REGISTRY_KEY,
            CartesianShapeValuesSourceType.instance(),
            CartesianShapeBoundsAggregator::new,
            true
        );
        builder.register(
            CartesianBoundsAggregationBuilder.REGISTRY_KEY,
            CartesianPointValuesSourceType.instance(),
            CartesianBoundsAggregator::new,
            true
        );
    }

    private void registerGeoShapeCentroidAggregator(ValuesSourceRegistry.Builder builder) {
        builder.register(
            GeoCentroidAggregationBuilder.REGISTRY_KEY,
            GeoShapeValuesSourceType.instance(),
            (name, valuesSourceConfig, context, parent, metadata) -> {
                if (GEO_CENTROID_AGG_FEATURE.check(getLicenseState())) {
                    return new GeoShapeCentroidAggregator(name, context, parent, valuesSourceConfig, metadata);
                }
                throw LicenseUtils.newComplianceException(GeoCentroidAggregationBuilder.NAME + " aggregation on geo_shape fields");
            },
            true
        );
    }

    private void registerCartesianCentroidAggregator(ValuesSourceRegistry.Builder builder) {
        // Only aggregations over the shape type are licensed at Gold/Platinum level
        builder.register(
            CartesianCentroidAggregationBuilder.REGISTRY_KEY,
            CartesianShapeValuesSourceType.instance(),
            (name, valuesSourceConfig, context, parent, metadata) -> {
                if (CARTESIAN_CENTROID_AGG_FEATURE.check(getLicenseState())) {
                    return new CartesianShapeCentroidAggregator(name, context, parent, valuesSourceConfig, metadata);
                }
                throw LicenseUtils.newComplianceException(CartesianCentroidAggregationBuilder.NAME + " aggregation on shape fields");
            },
            true
        );
        // Points are licensed at the default level (Basic)
        builder.register(
            CartesianCentroidAggregationBuilder.REGISTRY_KEY,
            CartesianPointValuesSourceType.instance(),
            CartesianCentroidAggregator::new,
            true
        );
    }

    private void registerGeoHexGridAggregator(ValuesSourceRegistry.Builder builder) {
        builder.register(
            GeoHexGridAggregationBuilder.REGISTRY_KEY,
            CoreValuesSourceType.GEOPOINT,
            (
                name,
                factories,
                valuesSource,
                precision,
                geoBoundingBox,
                requiredSize,
                shardSize,
                aggregationContext,
                parent,
                cardinality,
                metadata) -> {
                if (GEO_HEX_AGG_FEATURE.check(getLicenseState())) {
                    return new GeoHexGridAggregator(
                        name,
                        factories,
                        cb -> new GeoHexCellIdSource((ValuesSource.GeoPoint) valuesSource, precision, geoBoundingBox, cb),
                        requiredSize,
                        shardSize,
                        aggregationContext,
                        parent,
                        cardinality,
                        metadata
                    );
                }

                throw LicenseUtils.newComplianceException("geohex_grid aggregation on geo_point fields");
            },
            true
        );
    }

    private void registerGeoShapeGridAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(
            GeoHashGridAggregationBuilder.REGISTRY_KEY,
            GeoShapeValuesSourceType.instance(),
            (
                name,
                factories,
                valuesSource,
                precision,
                geoBoundingBox,
                requiredSize,
                shardSize,
                aggregationContext,
                parent,
                collectsFromSingleBucket,
                metadata) -> {
                if (GEO_GRID_AGG_FEATURE.check(getLicenseState())) {
                    final GeoGridTiler tiler = GeoHashGridTiler.makeGridTiler(precision, geoBoundingBox);
                    return new GeoHashGridAggregator(
                        name,
                        factories,
                        cb -> new GeoShapeCellIdSource((GeoShapeValuesSource) valuesSource, tiler, cb),
                        requiredSize,
                        shardSize,
                        aggregationContext,
                        parent,
                        collectsFromSingleBucket,
                        metadata
                    );
                }
                throw LicenseUtils.newComplianceException("geohash_grid aggregation on geo_shape fields");
            },
            true
        );

        builder.register(
            GeoTileGridAggregationBuilder.REGISTRY_KEY,
            GeoShapeValuesSourceType.instance(),
            (
                name,
                factories,
                valuesSource,
                precision,
                geoBoundingBox,
                requiredSize,
                shardSize,
                context,
                parent,
                collectsFromSingleBucket,
                metadata) -> {
                if (GEO_GRID_AGG_FEATURE.check(getLicenseState())) {
                    final GeoGridTiler tiler = GeoTileGridTiler.makeGridTiler(precision, geoBoundingBox);
                    return new GeoTileGridAggregator(
                        name,
                        factories,
                        cb -> new GeoShapeCellIdSource((GeoShapeValuesSource) valuesSource, tiler, cb),
                        requiredSize,
                        shardSize,
                        context,
                        parent,
                        collectsFromSingleBucket,
                        metadata
                    );
                }
                throw LicenseUtils.newComplianceException("geotile_grid aggregation on geo_shape fields");
            },
            true
        );

        builder.register(
            GeoHexGridAggregationBuilder.REGISTRY_KEY,
            GeoShapeValuesSourceType.instance(),
            (
                name,
                factories,
                valuesSource,
                precision,
                geoBoundingBox,
                requiredSize,
                shardSize,
                context,
                parent,
                collectsFromSingleBucket,
                metadata) -> {
                if (GEO_GRID_AGG_FEATURE.check(getLicenseState())) {
                    final GeoGridTiler tiler = GeoHexGridTiler.makeGridTiler(precision, geoBoundingBox);
                    return new GeoHexGridAggregator(
                        name,
                        factories,
                        cb -> new GeoShapeCellIdSource((GeoShapeValuesSource) valuesSource, tiler, cb),
                        requiredSize,
                        shardSize,
                        context,
                        parent,
                        collectsFromSingleBucket,
                        metadata
                    );
                }
                throw LicenseUtils.newComplianceException("geohex_grid aggregation on geo_shape fields");
            },
            true
        );
    }

    private <T> ContextParser<String, T> checkLicense(ContextParser<String, T> realParser, LicensedFeature.Momentary feature) {
        return (parser, name) -> {
            if (feature.check(getLicenseState()) == false) {
                throw LicenseUtils.newComplianceException(feature.getName());
            }
            return realParser.parse(parser, name);
        };
    }

    @Override
    public void loadExtensions(ExtensionLoader loader) {
        // we only expect one vector tile extension that comes from the vector tile module.
        List<GeoFormatterFactory.FormatterFactory<Geometry>> formatterFactories = new ArrayList<>();
        loader.loadExtensions(GeometryFormatterExtension.class)
            .stream()
            .map(GeometryFormatterExtension::getGeometryFormatterFactories)
            .forEach(formatterFactories::addAll);
        geoFormatterFactory.set(new GeoFormatterFactory<>(formatterFactories));
    }
}
