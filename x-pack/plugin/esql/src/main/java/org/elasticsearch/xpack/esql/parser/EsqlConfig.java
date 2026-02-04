/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.Build;

public class EsqlConfig {

    // versioning information
    private final boolean isDevVersion;

    public EsqlConfig(boolean isDevVersion) {
        this.isDevVersion = isDevVersion;
    }

    public EsqlConfig() {
        this(Build.current().isSnapshot());
    }

    public boolean isDevVersion() {
        return isDevVersion;
    }
}
