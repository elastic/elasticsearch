/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;

import java.util.Collection;
import java.util.Collections;

public class CartesianShapeQueryTests extends CartesianShapeQueryTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(LocalStateSpatialPlugin.class);
    }

    @Override
    public void testQueryRandomGeoCollection() throws Exception {
        super.testQueryRandomGeoCollection();
    }
}
