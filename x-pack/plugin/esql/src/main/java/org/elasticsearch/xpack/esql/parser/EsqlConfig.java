/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.Build;
import org.elasticsearch.xpack.core.esql.EsqlFeatureFlags;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;

public class EsqlConfig {

    // versioning information
    private final boolean isDevVersion;
    private final EsqlFunctionRegistry functionRegistry;

    public EsqlConfig(boolean isDevVersion, EsqlFunctionRegistry functionRegistry) {
        this.isDevVersion = isDevVersion;
        this.functionRegistry = functionRegistry;
    }

    public EsqlConfig(EsqlFunctionRegistry functionRegistry) {
        this(Build.current().isSnapshot(), functionRegistry);
    }

    public boolean isDevVersion() {
        return isDevVersion;
    }

    /**
     * Whether the EXTERNAL command and external data source grammar are enabled (feature flag; on by default in snapshot builds).
     */
    public boolean isExternalDataSourcesEnabled() {
        return EsqlFeatureFlags.ESQL_EXTERNAL_DATASOURCES_FEATURE_FLAG.isEnabled();
    }

    public EsqlFunctionRegistry functionRegistry() {
        return functionRegistry;
    }
}
