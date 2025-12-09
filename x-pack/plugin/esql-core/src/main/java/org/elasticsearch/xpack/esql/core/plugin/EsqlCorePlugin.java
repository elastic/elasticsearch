/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.plugin;

import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;

public class EsqlCorePlugin extends Plugin implements ExtensiblePlugin {

    public static final FeatureFlag EXPONENTIAL_HISTOGRAM_FEATURE_FLAG = new FeatureFlag("esql_exponential_histogram");

    // Note, there is also a feature flag for the field type in the analytics plugin, but for visibility reasons we need
    // another one here.
    public static final FeatureFlag T_DIGEST_ESQL_SUPPORT = new FeatureFlag("esql_t_digest_support");
}
