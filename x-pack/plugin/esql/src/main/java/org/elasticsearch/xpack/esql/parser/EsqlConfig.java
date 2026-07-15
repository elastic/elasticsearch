/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.Build;
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
     * Whether the EXTERNAL command and external data source grammar are enabled. Mirrors {@link #isDevVersion()}:
     * enabled in snapshot builds, disabled in release builds. Snapshot test runs may also use
     * {@link #EsqlConfig(boolean, EsqlFunctionRegistry) EsqlConfig(false, ...)} to simulate production parsing, in
     * which case EXTERNAL is disabled regardless of the actual build type.
     */
    public boolean isExternalDataSourcesEnabled() {
        return isDevVersion;
    }

    public EsqlFunctionRegistry functionRegistry() {
        return functionRegistry;
    }
}
