/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.elasticsearch.Version;
import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Map;

/**
 * This class specifies features exposed by the Shutdown plugin.
 */
public class ShutdownFeatures implements FeatureSpecification {

    @Override
    public Map<NodeFeature, Version> getHistoricalFeatures() {
        return Map.of(ShutdownPlugin.SHUTDOWN_API_SUPPORTED, Version.V_7_15_0);
    }
}
