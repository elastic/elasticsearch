/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasource.azure;

import org.elasticsearch.xpack.esql.datasources.spi.FileDataSourceConfiguration.AuthMode;
import org.elasticsearch.xpack.esql.datasources.spi.StorageIdentity;

/**
 * Identifies the Azure storage configuration (endpoint + credential identity) for use as the
 * {@code storageIdentity} component of a {@code FooterByteCache.Key}, so that two data sources with
 * different credentials never share cached footer bytes.
 * <p>
 * Secret settings ({@code connection_string}, {@code key}, {@code sas_token}) are held as SHA-256 digests
 * rather than plaintext: cache keys outlive the data source and records print their fields in {@code toString}.
 */
record AzureCredentialIdentity(
    AuthMode authMode,
    String endpoint,
    String account,
    String connectionStringDigest,
    String keyDigest,
    String sasTokenDigest,
    String tenantId,
    String clientId,
    String jwtAudience
) implements StorageIdentity {

    static AzureCredentialIdentity of(AzureConfiguration config) {
        return new AzureCredentialIdentity(
            config.resolveAuthMode(),
            config.endpoint(),
            config.account(),
            StorageIdentity.digestSecret(config.connectionString()),
            StorageIdentity.digestSecret(config.key()),
            StorageIdentity.digestSecret(config.sasToken()),
            config.tenantId(),
            config.clientId(),
            config.jwtAudience()
        );
    }
}
