/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Tests {@link S3StorageProvider#credentialsProvider(S3Configuration)}: the mapping from a parsed
 * {@link S3Configuration} to the AWS SDK credentials provider, including STS temporary credentials.
 */
public class S3CredentialsProviderTests extends ESTestCase {

    public void testSessionCredentials() {
        S3Configuration config = S3Configuration.fromFields("ak", "sk", "tok", null, null, null);
        AwsCredentials creds = S3StorageProvider.forTesting(null, null).credentialsProvider(config).resolveCredentials();
        assertThat(creds, instanceOf(AwsSessionCredentials.class));
        AwsSessionCredentials session = (AwsSessionCredentials) creds;
        assertEquals("ak", session.accessKeyId());
        assertEquals("sk", session.secretAccessKey());
        assertEquals("tok", session.sessionToken());
    }

    public void testBasicCredentialsWhenNoSessionToken() {
        S3Configuration config = S3Configuration.fromFields("ak", "sk", null, null);
        AwsCredentials creds = S3StorageProvider.forTesting(null, null).credentialsProvider(config).resolveCredentials();
        assertThat(creds, instanceOf(AwsBasicCredentials.class));
        assertEquals("ak", creds.accessKeyId());
        assertEquals("sk", creds.secretAccessKey());
    }

    public void testAnonymous() {
        S3Configuration config = S3Configuration.fromFields(null, null, "http://endpoint", "us-east-1", "none");
        assertThat(S3StorageProvider.forTesting(null, null).credentialsProvider(config), instanceOf(AnonymousCredentialsProvider.class));
    }

    public void testEmptySessionTokenTreatedAsAbsent() {
        // An empty session token is treated as absent: with keys present, this falls back to basic credentials
        // rather than building STS credentials with an empty token (which would fail later at request signing).
        S3Configuration config = S3Configuration.fromMap(Map.of("access_key", "ak", "secret_key", "sk", "session_token", ""));
        AwsCredentials creds = S3StorageProvider.forTesting(null, null).credentialsProvider(config).resolveCredentials();
        assertThat(creds, instanceOf(AwsBasicCredentials.class));
    }

    public void testSessionTokenWithoutKeysThrows() {
        S3Configuration config = S3Configuration.fromFields(null, null, "tok", null, null, null);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> S3StorageProvider.forTesting(null, null).credentialsProvider(config)
        );
        assertTrue(e.getMessage().contains("S3 session_token requires access_key and secret_key"));
    }

    public void testNoCredentialsThrows() {
        S3Configuration config = S3Configuration.fromFields(null, null, "http://endpoint", "us-east-1");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> S3StorageProvider.forTesting(null, null).credentialsProvider(config)
        );
        assertTrue(e.getMessage().contains("S3 data source requires credentials"));
    }

    /**
     * Without an IRSA singleton wired through, the workload-identity chain is just
     * ContainerCredentialsProvider followed by InstanceProfileCredentialsProvider — the v1
     * shape. Behavior on every non-EKS deployment.
     */
    public void testWorkloadIdentityChainExcludesIrsaWhenSingletonAbsent() {
        List<AwsCredentialsProvider> providers = S3StorageProvider.forTesting(null, null).workloadIdentityProviders();
        assertThat(providers, hasSize(2));
        assertThat(providers.get(0), instanceOf(ContainerCredentialsProvider.class));
        assertThat(providers.get(1), instanceOf(InstanceProfileCredentialsProvider.class));
    }

    /**
     * An inactive singleton (env var unset on the node) is treated the same as no singleton at
     * all: skipped from the chain so we don't attempt STS calls that have no chance of succeeding.
     */
    public void testWorkloadIdentityChainExcludesIrsaWhenSingletonInactive() {
        CustomWebIdentityTokenCredentialsProvider inactive = new CustomWebIdentityTokenCredentialsProvider(null, null, null, name -> null);
        assertFalse("test setup: provider must be inactive when env returns null for everything", inactive.isActive());
        List<AwsCredentialsProvider> providers = new S3StorageProvider(null, null, inactive).workloadIdentityProviders();
        assertThat(providers, hasSize(2));
        assertThat(providers.get(0), instanceOf(ContainerCredentialsProvider.class));
        assertThat(providers.get(1), instanceOf(InstanceProfileCredentialsProvider.class));
    }

    /**
     * Sanity check the v1 providers are listed in the documented order: container credentials
     * (ECS / Pod Identity) before EC2 instance profile.
     */
    public void testV1ChainOrder() {
        List<AwsCredentialsProvider> providers = S3StorageProvider.forTesting(null, null).workloadIdentityProviders();
        assertThat(providers, hasSize(2));
        assertThat(providers.get(0), instanceOf(ContainerCredentialsProvider.class));
        assertThat(providers.get(1), instanceOf(InstanceProfileCredentialsProvider.class));
    }
}
