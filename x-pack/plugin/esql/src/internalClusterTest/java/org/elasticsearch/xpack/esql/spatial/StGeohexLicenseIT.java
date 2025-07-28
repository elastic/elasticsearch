/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.spatial;

import org.elasticsearch.geometry.Point;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.action.EsqlPluginWithEnterpriseOrTrialLicense;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StGeohex;
import org.elasticsearch.xpack.spatial.SpatialPlugin;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StGeohexLicenseIT extends SpatialGridLicenseTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(SpatialPlugin.class, EsqlPluginWithEnterpriseOrTrialLicense.class);
    }

    @Override
    protected String gridFunction() {
        return "ST_GEOHEX";
    }

    @Override
    protected Map<Long, Long> expectedValues() {
        Map<Long, Long> expected = new HashMap<>();
        for (Point point : testData) {
            long gridId = StGeohex.unboundedGrid.calculateGridId(point, precision());
            expected.compute(gridId, (k, v) -> v == null ? 1 : v + 1);
        }
        return expected;
    }

    public void testGeoGridWithShapes() {
        assertGeoGridFromIndex("index_geo_shape");
    }
}
