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
    public static final FeatureFlag DATE_NANOS_FEATURE_FLAG = new FeatureFlag("esql_date_nanos");

}
