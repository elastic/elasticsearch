/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.esql;

import org.elasticsearch.common.util.FeatureFlag;

/**
 * Shared ES|QL feature flags for use across x-pack modules.
 */
public class EsqlFeatureFlags {
    /**
     * A feature flag to enable ESQL views REST API functionality.
     */
    public static final FeatureFlag ESQL_VIEWS_FEATURE_FLAG = new FeatureFlag("esql_views");
}
