/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasource.s3;

/**
 * Identifies the S3 storage configuration (endpoint + credential identity) for use as the
 * {@code storageIdentity} component of a {@code FooterByteCache.Key}. Record equality is used
 * directly — no string serialisation, no separator-collision risk.
 * <p>
 * Fields vary by auth mode:
 * <ul>
 *   <li>{@code static_credentials}: endpoint + accessKey</li>
 *   <li>{@code federated_identity}: endpoint + roleArn + stsEndpoint</li>
 *   <li>{@code anonymous} / {@code managed_identity}: endpoint only (both are node-level identities)</li>
 * </ul>
 * Unused fields are {@code null} in every case, so distinct auth modes with the same endpoint never
 * collide. The sentinel {@link #NONE} is used by test-only constructors that have no config.
 */
record S3CredentialIdentity(String endpoint, String accessKey, String roleArn, String stsEndpoint)
    implements
        org.elasticsearch.xpack.esql.datasources.spi.StorageIdentity {

    /** Sentinel used by test-only provider constructors that have no {@link S3Configuration}. */
    static final S3CredentialIdentity NONE = new S3CredentialIdentity(null, null, null, null);

    /**
     * Builds an identity from the given config. Returns {@link #NONE} when {@code config} is
     * {@code null}.
     */
    static S3CredentialIdentity of(S3Configuration config) {
        if (config == null) {
            return NONE;
        }
        return new S3CredentialIdentity(config.endpoint(), config.accessKey(), config.roleArn(), config.stsEndpoint());
    }
}
