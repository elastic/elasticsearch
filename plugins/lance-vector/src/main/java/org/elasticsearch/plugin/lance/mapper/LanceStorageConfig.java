/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.lance.mapper;

import java.util.Objects;

/**
 * Minimal storage configuration for Phase 1 (external mount).
 * Only supports read-only, external URIs and is primarily consumed by the
 * fake S3 dataset loader used in tests.
 */
public class LanceStorageConfig {
    private final String type;
    private final String uri;
    private final String idColumn;
    private final String vectorColumn;
    private final String ossEndpoint;
    private final String ossAccessKeyId;
    private final String ossAccessKeySecret;

    public LanceStorageConfig(String type, String uri, String idColumn, String vectorColumn,
                              String ossEndpoint, String ossAccessKeyId, String ossAccessKeySecret) {
        this.type = Objects.requireNonNull(type);
        this.uri = Objects.requireNonNull(uri);
        this.idColumn = Objects.requireNonNull(idColumn);
        this.vectorColumn = Objects.requireNonNull(vectorColumn);
        this.ossEndpoint = ossEndpoint;
        this.ossAccessKeyId = ossAccessKeyId;
        this.ossAccessKeySecret = ossAccessKeySecret;
    }

    public String type() {
        return type;
    }

    public String uri() {
        return uri;
    }

    public String idColumn() {
        return idColumn;
    }

    public String vectorColumn() {
        return vectorColumn;
    }

    public String ossEndpoint() {
        return ossEndpoint;
    }

    public String ossAccessKeyId() {
        return ossAccessKeyId;
    }

    public String ossAccessKeySecret() {
        return ossAccessKeySecret;
    }
}
