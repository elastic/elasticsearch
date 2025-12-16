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

    // Note, there is also a feature flag for the field type in the analytics plugin, but for visibility reasons we need
    // another one here.
    // this feature flag corresponds to the ES support for the dedicated t-digest field type,
    // The ESQL T-Digest type is supported even without it (e.g. via type conversions)
    public static final FeatureFlag T_DIGEST_FIELD_SUPPORT = new FeatureFlag("esql_t_digest_support");
}
