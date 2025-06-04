/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial;

import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.common.io.stream.GenericNamedWriteable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.License;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileGridAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.GeoCentroidAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.GeoGridAggregatorSupplier;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid.GeoHexGridAggregationBuilder;
import org.elasticsearch.xpack.spatial.search.aggregations.metrics.CartesianBoundsAggregationBuilder;
import org.elasticsearch.xpack.spatial.search.aggregations.metrics.CartesianCentroidAggregationBuilder;
import org.elasticsearch.xpack.spatial.search.aggregations.support.CartesianPointValuesSourceType;
import org.elasticsearch.xpack.spatial.search.aggregations.support.CartesianShapeValuesSourceType;
import org.elasticsearch.xpack.spatial.search.aggregations.support.GeoShapeValuesSourceType;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SpatialPluginTests extends ESTestCase {

    public void testGeoShapeCentroidLicenseCheck() {
        checkLicenseRequired(GeoShapeValuesSourceType.instance(), GeoCentroidAggregationBuilder.REGISTRY_KEY, (agg) -> {
            try {
                agg.build(null, null, null, null, null);
            } catch (IOException e) {
                fail("Unexpected exception: " + e.getMessage());
            }
        }, "geo_centroid", "geo_shape");
    }

    public void testGeoHexLicenseCheck() {
        checkLicenseRequired(CoreValuesSourceType.GEOPOINT, GeoHexGridAggregationBuilder.REGISTRY_KEY, (agg) -> {
            try {
                agg.build(null, AggregatorFactories.EMPTY, null, 0, null, 0, 0, null, null, CardinalityUpperBound.NONE, null);
            } catch (IOException e) {
                fail("Unexpected exception: " + e.getMessage());
            }
        }, "geohex_grid", "geo_point");
    }

    public void testGeoShapeHexLicenseCheck() {
        checkLicenseRequired(GeoShapeValuesSourceType.instance(), GeoHexGridAggregationBuilder.REGISTRY_KEY, (agg) -> {
            try {
                agg.build(null, AggregatorFactories.EMPTY, null, 0, null, 0, 0, null, null, CardinalityUpperBound.NONE, null);
            } catch (IOException e) {
                fail("Unexpected exception: " + e.getMessage());
            }
        }, "geohex_grid", "geo_shape");
    }

    public void testGeoGridLicenseCheck() {
        for (ValuesSourceRegistry.RegistryKey<GeoGridAggregatorSupplier> registryKey : Arrays.asList(
            GeoHashGridAggregationBuilder.REGISTRY_KEY,
            GeoTileGridAggregationBuilder.REGISTRY_KEY
        )) {
            checkLicenseRequired(GeoShapeValuesSourceType.instance(), registryKey, (agg) -> {
                try {
                    agg.build(null, AggregatorFactories.EMPTY, null, 0, null, 0, 0, null, null, CardinalityUpperBound.NONE, null);
                } catch (IOException e) {
                    fail("Unexpected exception: " + e.getMessage());
                }
            }, registryKey.getName(), "geo_shape");
        }
    }

    public void testCartesianShapeCentroidLicenseCheck() {
        checkLicenseRequired(CartesianShapeValuesSourceType.instance(), CartesianCentroidAggregationBuilder.REGISTRY_KEY, (agg) -> {
            try {
                agg.build(null, null, null, null, null);
            } catch (IOException e) {
                fail("Unexpected exception: " + e.getMessage());
            }
        }, "cartesian_centroid", "shape");
    }

    public void testCartesianPointCentroidLicenseCheck() {
        checkLicenseNotRequired(CartesianPointValuesSourceType.instance(), CartesianCentroidAggregationBuilder.REGISTRY_KEY, (agg) -> {
            try {
                agg.build(null, null, null, null, null);
            } catch (IOException e) {
                fail("Unexpected exception: " + e.getMessage());
            }
        }, "cartesian_centroid", "point");
    }

    public void testCartesianPointBoundsLicenseCheck() {
        CartesianPointValuesSourceType sourceType = CartesianPointValuesSourceType.instance();
        TestValuesSourceConfig sourceConfig = new TestValuesSourceConfig(sourceType);
        checkLicenseNotRequired(sourceType, CartesianBoundsAggregationBuilder.REGISTRY_KEY, (agg) -> {
            try {
                agg.build(null, null, null, sourceConfig, null);
            } catch (IOException e) {
                fail("Unexpected exception: " + e.getMessage());
            }
        }, "cartesian_bounds", "point");
    }

    public void testCartesianShapeBoundsLicenseCheck() {
        CartesianShapeValuesSourceType sourceType = CartesianShapeValuesSourceType.instance();
        TestValuesSourceConfig sourceConfig = new TestValuesSourceConfig(sourceType);
        checkLicenseNotRequired(sourceType, CartesianBoundsAggregationBuilder.REGISTRY_KEY, (agg) -> {
            try {
                agg.build(null, null, null, sourceConfig, null);
            } catch (IOException e) {
                fail("Unexpected exception: " + e.getMessage());
            }
        }, "cartesian_bounds", "shape");
    }

    public void testGenericNamedWriteables() {
        SearchModule module = new SearchModule(Settings.EMPTY, List.of(new SpatialPlugin()));
        Set<String> names = module.getNamedWriteables()
            .stream()
            .filter(e -> e.categoryClass.equals(GenericNamedWriteable.class))
            .map(e -> e.name)
            .collect(Collectors.toSet());
        assertThat(
            "Expect both Geo and Cartesian BoundingBox and ShapeValue",
            names,
            equalTo(Set.of("GeoBoundingBox", "CartesianBoundingBox", "GeoShapeValue", "CartesianShapeValue"))
        );
    }

    private SpatialPlugin getPluginWithOperationMode(License.OperationMode operationMode) {
        return new SpatialPlugin() {
            protected XPackLicenseState getLicenseState() {
                TestUtils.UpdatableLicenseState licenseState = new TestUtils.UpdatableLicenseState();
                licenseState.update(new XPackLicenseStatus(operationMode, true, null));
                return licenseState;
            }
        };
    }

    private <T> void checkLicenseNotRequired(
        ValuesSourceType sourceType,
        ValuesSourceRegistry.RegistryKey<T> registryKey,
        Consumer<T> builder,
        String aggName,
        String fieldTypeName
    ) {
        for (License.OperationMode operationMode : License.OperationMode.values()) {
            SpatialPlugin plugin = getPluginWithOperationMode(operationMode);
            ValuesSourceRegistry.Builder registryBuilder = new ValuesSourceRegistry.Builder();
            List<Consumer<ValuesSourceRegistry.Builder>> registrar = plugin.getAggregationExtentions();
            registrar.forEach(c -> c.accept(registryBuilder));
            List<SearchPlugin.AggregationSpec> specs = plugin.getAggregations();
            specs.forEach(c -> c.getAggregatorRegistrar().accept(registryBuilder));
            ValuesSourceRegistry registry = registryBuilder.build();
            T aggregator = registry.getAggregator(
                registryKey,
                new ValuesSourceConfig(sourceType, null, true, null, null, null, null, null)
            );
            NullPointerException exception = expectThrows(NullPointerException.class, () -> builder.accept(aggregator));
            assertThat(
                "Incorrect exception testing " + aggName + " on field " + fieldTypeName,
                exception.getMessage(),
                containsString("because \"context\" is null")
            );
        }
    }

    private <T> void checkLicenseRequired(
        ValuesSourceType sourceType,
        ValuesSourceRegistry.RegistryKey<T> registryKey,
        Consumer<T> builder,
        String aggName,
        String fieldTypeName
    ) {
        for (License.OperationMode operationMode : License.OperationMode.values()) {
            SpatialPlugin plugin = getPluginWithOperationMode(operationMode);
            ValuesSourceRegistry.Builder registryBuilder = new ValuesSourceRegistry.Builder();
            List<Consumer<ValuesSourceRegistry.Builder>> registrar = plugin.getAggregationExtentions();
            registrar.forEach(c -> c.accept(registryBuilder));
            List<SearchPlugin.AggregationSpec> specs = plugin.getAggregations();
            specs.forEach(c -> c.getAggregatorRegistrar().accept(registryBuilder));
            ValuesSourceRegistry registry = registryBuilder.build();
            T aggregator = registry.getAggregator(
                registryKey,
                new ValuesSourceConfig(sourceType, null, true, null, null, null, null, null)
            );
            if (License.OperationMode.TRIAL != operationMode
                && License.OperationMode.compare(operationMode, License.OperationMode.GOLD) < 0) {
                ElasticsearchSecurityException exception = expectThrows(
                    ElasticsearchSecurityException.class,
                    () -> builder.accept(aggregator)
                );
                assertThat(
                    exception.getMessage(),
                    equalTo("current license is non-compliant for [" + aggName + " aggregation on " + fieldTypeName + " fields]")
                );
            } else {
                try {
                    builder.accept(aggregator);
                } catch (NullPointerException e) {
                    // Expected exception from passing null aggregation context
                } catch (Exception e) {
                    fail("Unexpected exception testing " + aggName + " at license level " + operationMode + ": " + e.getMessage());
                }
            }
        }
    }

    private static class TestValuesSourceConfig extends ValuesSourceConfig {
        private TestValuesSourceConfig(ValuesSourceType sourceType) {
            super(sourceType, null, true, null, null, null, null, null);
        }
    }
}
