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
 * Identifies the S3 storage configuration (endpoint, region, credential identity) for use as the
 * {@code storageIdentity} component of a {@code FooterByteCache.Key}. Record equality is used
 * directly — no string serialisation, no separator-collision risk.
 * <p>
 * Fields vary by auth mode; every mode also carries endpoint and region:
 * <ul>
 *   <li>{@code static_credentials}: authMode + accessKey + secretKey + sessionToken</li>
 *   <li>{@code federated_identity}: authMode + roleArn + roleSessionName + jwtAudience + stsEndpoint</li>
 *   <li>{@code anonymous}: authMode</li>
 *   <li>{@code managed_identity}: authMode</li>
 * </ul>
 * Region is part of the identity because, with no endpoint override, it selects the AWS partition
 * ({@code aws}, {@code aws-cn}, {@code aws-us-gov}) and bucket names are only unique within one, so the
 * same {@code s3://bucket/key} can name different objects.
 * The secret key must be part of the identity, not just the access key: an access key ID is not a
 * secret, so a data source pairing a known access key with a wrong secret would otherwise be served
 * footers cached by the legitimate one without S3 ever checking its signature. Secrets are held as
 * SHA-256 digests because cache keys outlive the data source and records print their fields in
 * {@code toString}.
 * <p>
 * The {@code authMode} field ensures that {@code anonymous} and {@code managed_identity} configs
 * at the same endpoint are never assigned the same identity, even though both carry no
 * per-datasource credential fields.
 */
record S3CredentialIdentity(
    AuthMode authMode,
    String endpoint,
    String region,
    String accessKey,
    String secretKeyDigest,
    String sessionTokenDigest,
    String roleArn,
    String roleSessionName,
    String jwtAudience,
    String stsEndpoint
) implements StorageIdentity {

    /** Builds an identity from the given config. */
    static S3CredentialIdentity of(S3Configuration config) {
        return new S3CredentialIdentity(
            config.resolveAuthMode(),
            config.endpoint(),
            config.region(),
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
