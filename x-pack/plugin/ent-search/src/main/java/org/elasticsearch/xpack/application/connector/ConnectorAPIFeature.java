/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import org.elasticsearch.common.util.FeatureFlag;

/**
 * Connector API feature flag. When the feature is complete, this flag will be removed.
 */
public class ConnectorAPIFeature {

    private static final FeatureFlag CONNECTOR_API_FEATURE_FLAG = new FeatureFlag("connector_api");

    public static boolean isEnabled() {
        return CONNECTOR_API_FEATURE_FLAG.isEnabled();
    }
}
