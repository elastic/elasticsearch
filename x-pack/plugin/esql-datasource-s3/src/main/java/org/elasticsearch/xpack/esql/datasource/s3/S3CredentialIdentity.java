/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasource.s3;

import org.elasticsearch.xpack.esql.datasources.spi.FileDataSourceConfiguration.AuthMode;
import org.elasticsearch.xpack.esql.datasources.spi.StorageIdentity;

/**
 * Identifies the S3 storage configuration (endpoint + credential identity) for use as the
 * {@code storageIdentity} component of a {@code FooterByteCache.Key}. Record equality is used
 * directly — no string serialisation, no separator-collision risk.
 * <p>
 * Fields vary by auth mode:
 * <ul>
 *   <li>{@code static_credentials}: authMode + endpoint + accessKey + secretKey + sessionToken</li>
 *   <li>{@code federated_identity}: authMode + endpoint + roleArn + roleSessionName + jwtAudience + stsEndpoint</li>
 *   <li>{@code anonymous}: authMode + endpoint</li>
 *   <li>{@code managed_identity}: authMode + endpoint</li>
 * </ul>
 * The secret key must be part of the identity, not just the access key: an access key ID is not a
 * secret, so a data source pairing a known access key with a wrong secret would otherwise be served
 * footers cached by the legitimate one without S3 ever checking its signature. Secrets are held as
 * SHA-256 digests because cache keys outlive the data source and records print their fields in
 * {@code toString}.
 * <p>
 * The {@code authMode} field ensures that {@code anonymous} and {@code managed_identity} configs
 * at the same endpoint are never assigned the same identity, even though both carry no
 * per-datasource credential fields. The sentinel {@link #NONE} is used by test-only constructors
 * that have no config.
 */
record S3CredentialIdentity(
    AuthMode authMode,
    String endpoint,
    String accessKey,
    String secretKeyDigest,
    String sessionTokenDigest,
    String roleArn,
    String roleSessionName,
    String jwtAudience,
    String stsEndpoint
) implements StorageIdentity {

    /** Sentinel used by test-only provider constructors that have no {@link S3Configuration}. */
    static final S3CredentialIdentity NONE = new S3CredentialIdentity(null, null, null, null, null, null, null, null, null);

    /**
     * Builds an identity from the given config. Returns {@link #NONE} when {@code config} is
     * {@code null}.
     */
    static S3CredentialIdentity of(S3Configuration config) {
        if (config == null) {
            return NONE;
        }
        return new S3CredentialIdentity(
            config.resolveAuthMode(),
            config.endpoint(),
            config.accessKey(),
            StorageIdentity.digestSecret(config.secretKey()),
            StorageIdentity.digestSecret(config.sessionToken()),
            config.roleArn(),
            config.roleSessionName(),
            config.jwtAudience(),
            config.stsEndpoint()
        );
    }
}
