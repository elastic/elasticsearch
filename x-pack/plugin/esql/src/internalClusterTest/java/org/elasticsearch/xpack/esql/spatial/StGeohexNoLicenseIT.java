/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.spatial;

import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.List;

public class StGeohexNoLicenseIT extends StGeohexLicenseIT {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(SpatialNoLicenseTestCase.TestSpatialPlugin.class, SpatialNoLicenseTestCase.TestEsqlPlugin.class);
    }

    /**
     * The ST_GEOHEX function is not available without a license, so we override the test to ensure it fails.
     */
    public void testGeoGridWithPoints() {
        assertGeoGridFailsWith("index_geo_point");
    }

    public void testGeoGridWithShapes() {
        assertGeoGridFailsWith("index_geo_shape");
    }
}
