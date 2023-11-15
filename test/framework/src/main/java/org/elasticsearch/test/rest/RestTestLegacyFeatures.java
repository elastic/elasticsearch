/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.rest;

import org.elasticsearch.Version;
import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Map;

/**
 * This class groups historical features that have been removed from the production codebase, but are still used by the test
 * framework to support BwC tests. Rather than leaving them in the main src we group them here, so it's clear they are not used in
 * production code anymore.
 */
public class RestTestLegacyFeatures implements FeatureSpecification {
    public static final NodeFeature FEATURE_STATE_RESET_SUPPORTED = new NodeFeature("system_indices.feature_state_reset_supported");
    public static final NodeFeature SYSTEM_INDICES_REST_ACCESS_ENFORCED = new NodeFeature("system_indices.rest_access_enforced");

    @Override
    public Map<NodeFeature, Version> getHistoricalFeatures() {
        return Map.of(FEATURE_STATE_RESET_SUPPORTED, Version.V_7_13_0, SYSTEM_INDICES_REST_ACCESS_ENFORCED, Version.V_8_0_0);
    }
}
