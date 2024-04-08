/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.Version;
import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.xpack.security.support.SecuritySystemIndices;

import java.util.Map;

public class SecurityFeatures implements FeatureSpecification {
    @Override
    public Map<NodeFeature, Version> getHistoricalFeatures() {
        return Map.of(SecuritySystemIndices.SECURITY_PROFILE_ORIGIN_FEATURE, SecuritySystemIndices.VERSION_SECURITY_PROFILE_ORIGIN);
    }
}
