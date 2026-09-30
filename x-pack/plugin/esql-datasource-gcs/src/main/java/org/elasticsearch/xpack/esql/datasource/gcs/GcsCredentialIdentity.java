/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasource.gcs;

import org.elasticsearch.xpack.esql.datasources.spi.FileDataSourceConfiguration.AuthMode;
import org.elasticsearch.xpack.esql.datasources.spi.StorageIdentity;

/**
 * Identifies the GCS storage configuration (endpoint + credential identity) for use as the
 * {@code storageIdentity} component of a {@code FooterByteCache.Key}, so that two data sources with
 * different credentials never share cached footer bytes.
 * <p>
 * Secret settings ({@code credentials}, {@code access_token}) are held as SHA-256 digests rather than
 * plaintext: cache keys outlive the data source and records print their fields in {@code toString}.
 */
record GcsCredentialIdentity(
    AuthMode authMode,
    String endpoint,
    String projectId,
    String tokenUri,
    String credentialsDigest,
    String accessTokenDigest,
    String jwtAudience,
    String stsAudience,
    String serviceAccountImpersonationUrl
) implements StorageIdentity {

    static GcsCredentialIdentity of(GcsConfiguration config) {
        return new GcsCredentialIdentity(
            config.resolveAuthMode(),
            config.endpoint(),
            config.projectId(),
            config.tokenUri(),
            StorageIdentity.digestSecret(config.serviceAccountCredentials()),
            StorageIdentity.digestSecret(config.accessToken()),
            config.jwtAudience(),
            config.stsAudience(),
            config.serviceAccountImpersonationUrl()
        );
    }
}
