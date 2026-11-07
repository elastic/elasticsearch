/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasource.s3;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StorageIdentityCoverage;

import java.util.Map;

/**
 * {@link S3CredentialIdentity} is the storage identity component of the footer byte cache key. Its sibling
 * {@code S3StorageIdentityTests} covers a different mechanism - the identity the schema, listing and file-metadata
 * keys are built from - so the two are not one class.
 */
public class S3CredentialIdentityTests extends ESTestCase {

    /**
     * The census, derived rather than listed. A per-field test names its field, so no set of them can fail when this
     * provider GAINS a setting that never reaches the identity - and two data sources differing only in that setting
     * would then share cached bytes. This asks the configuration which settings it declares.
     */

    public void testEverySettingReachesTheIdentity() {
        S3Configuration config = S3Configuration.fromFields("AKIA", "secret", "http://s3:1", "us-east-1");
        StorageIdentityCoverage.assertEverySettingReachesTheIdentity(
            config,
            S3CredentialIdentity.of(config),
            Map.of("auth", "authMode"),
            Map.of(
                "addressing_style",
                "it only changes the request URL shape for the same bucket, key and principal, so it cannot change "
                    + "which bytes are reachable"
            )
        );
    }
}
