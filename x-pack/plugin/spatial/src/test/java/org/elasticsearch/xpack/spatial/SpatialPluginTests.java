/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial;

import org.elasticsearch.license.License;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.search.aggregations.metrics.GeoBoundsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.GeoBoundsAggregatorSupplier;
import org.elasticsearch.search.aggregations.metrics.GeoCentroidAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.GeoCentroidAggregatorSupplier;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xpack.spatial.search.aggregations.support.GeoShapeValuesSourceType;

import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.instanceOf;

public class SpatialPluginTests extends ESTestCase {

    public void testGeoAggregationsByLicense() {
        for (License.OperationMode operationMode : License.OperationMode.values()) {
            SpatialPlugin plugin = getPluginWithOperationMode(operationMode);
            ValuesSourceRegistry.Builder registryBuilder = new ValuesSourceRegistry.Builder();
            List<Consumer<ValuesSourceRegistry.Builder>> registrar = plugin.getAggregationExtentions();
            registrar.forEach(c -> c.accept(registryBuilder));
            ValuesSourceRegistry registry = registryBuilder.build();
            switch (operationMode) {
                case STANDARD:
                case BASIC:
                case MISSING:
                    assertThat(registry.getAggregator(GeoShapeValuesSourceType.instance(), GeoBoundsAggregationBuilder.NAME),
                        instanceOf(GeoBoundsAggregatorSupplier.class));
                    break;
                case ENTERPRISE:
                case PLATINUM:
                case GOLD:
                case TRIAL:
                    assertThat(registry.getAggregator(GeoShapeValuesSourceType.instance(), GeoCentroidAggregationBuilder.NAME),
                        instanceOf(GeoCentroidAggregatorSupplier.class));
                    break;
                default:
                    throw new IllegalArgumentException("unchecked operation mode [" + operationMode + "]");
            }
        }
    }

    private SpatialPlugin getPluginWithOperationMode(License.OperationMode operationMode) {
        return new SpatialPlugin() {
            protected XPackLicenseState getLicenseState() {
                TestUtils.UpdatableLicenseState licenseState = new TestUtils.UpdatableLicenseState();
                licenseState.update(operationMode, true, VersionUtils.randomVersion(random()));
                return licenseState;
            }
        };
    }
}
