/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.Build;
import org.elasticsearch.cluster.metadata.DataSourceMetadata;
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
     * Whether the EXTERNAL command and external data source grammar are enabled. This respects the
     * {@code esql_external_datasources} feature flag (on by default in snapshot builds). Snapshot test runs may also use
     * {@link #EsqlConfig(boolean, EsqlFunctionRegistry) EsqlConfig(false, ...)} to simulate production parsing; in that case
     * EXTERNAL is disabled even when the feature flag is on. Non-snapshot (release) builds ignore that simulation and rely on
     * the feature flag alone.
     */
    public boolean isExternalDataSourcesEnabled() {
        if (DataSourceMetadata.ESQL_EXTERNAL_DATASOURCES_FEATURE_FLAG.isEnabled() == false) {
            return false;
        }
        return isDevVersion || Build.current().isSnapshot() == false;
    }

    public EsqlFunctionRegistry functionRegistry() {
        return functionRegistry;
    }
}
