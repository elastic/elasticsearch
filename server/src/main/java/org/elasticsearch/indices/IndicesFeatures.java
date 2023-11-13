/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.elasticsearch.Version;
import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Map;

public class IndicesFeatures implements FeatureSpecification {
    @Override
    public Map<NodeFeature, Version> getHistoricalFeatures() {
        return Map.of(IndicesService.SUPPORTS_AUTO_PUT, Version.V_8_8_0);
    }
}
