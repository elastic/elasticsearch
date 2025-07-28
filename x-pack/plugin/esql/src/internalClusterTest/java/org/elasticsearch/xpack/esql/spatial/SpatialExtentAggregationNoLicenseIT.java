/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.spatial;

import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.spatial.SpatialPlugin;

import java.util.Collection;
import java.util.List;

public class SpatialExtentAggregationNoLicenseIT extends SpatialExtentAggregationTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestSpatialPlugin.class, TestEsqlPlugin.class);
    }

    @Override
    public void testStExtentAggregationWithShapes() {
        assertStExtentFailsWith("index_geo_shape");
    }

    private static XPackLicenseState getLicenseState() {
        License.OperationMode operationMode;
        boolean active;
        if (randomBoolean()) {
            operationMode = randomFrom(
                License.OperationMode.GOLD,
                License.OperationMode.BASIC,
                License.OperationMode.MISSING,
                License.OperationMode.STANDARD
            );
            active = true;
        } else {
            operationMode = randomFrom(License.OperationMode.PLATINUM, License.OperationMode.ENTERPRISE, License.OperationMode.TRIAL);
            active = false;  // expired
        }

        return new XPackLicenseState(
            () -> System.currentTimeMillis(),
            new XPackLicenseStatus(operationMode, active, "Test license expired")
        );
    }

    public static class TestEsqlPlugin extends EsqlPlugin {
        protected XPackLicenseState getLicenseState() {
            return SpatialExtentAggregationNoLicenseIT.getLicenseState();
        }
    }

    public static class TestSpatialPlugin extends SpatialPlugin {
        protected XPackLicenseState getLicenseState() {
            return SpatialExtentAggregationNoLicenseIT.getLicenseState();
        }
    }
}
