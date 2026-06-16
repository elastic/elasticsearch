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
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.instanceOf;

/**
 * Tests {@link S3StorageProvider#credentialsProvider(S3Configuration)}: the mapping from a parsed
 * {@link S3Configuration} to the AWS SDK credentials provider, including STS temporary credentials.
 */
public class S3CredentialsProviderTests extends ESTestCase {

    public void testSessionCredentials() {
        S3Configuration config = S3Configuration.fromFields("ak", "sk", "tok", null, null, null);
        AwsCredentials creds = new S3StorageProvider(null, null).credentialsProvider(config).resolveCredentials();
        assertThat(creds, instanceOf(AwsSessionCredentials.class));
        AwsSessionCredentials session = (AwsSessionCredentials) creds;
        assertEquals("ak", session.accessKeyId());
        assertEquals("sk", session.secretAccessKey());
        assertEquals("tok", session.sessionToken());
    }

    public void testBasicCredentialsWhenNoSessionToken() {
        S3Configuration config = S3Configuration.fromFields("ak", "sk", null, null);
        AwsCredentials creds = new S3StorageProvider(null, null).credentialsProvider(config).resolveCredentials();
        assertThat(creds, instanceOf(AwsBasicCredentials.class));
        assertEquals("ak", creds.accessKeyId());
        assertEquals("sk", creds.secretAccessKey());
    }

    public void testAnonymous() {
        S3Configuration config = S3Configuration.fromFields(null, null, "http://endpoint", "us-east-1", "none");
        assertThat(new S3StorageProvider(null, null).credentialsProvider(config), instanceOf(AnonymousCredentialsProvider.class));
    }

    public void testEmptySessionTokenTreatedAsAbsent() {
        // An empty session token is treated as absent: with keys present, this falls back to basic credentials
        // rather than building STS credentials with an empty token (which would fail later at request signing).
        S3Configuration config = S3Configuration.fromMap(Map.of("access_key", "ak", "secret_key", "sk", "session_token", ""));
        AwsCredentials creds = new S3StorageProvider(null, null).credentialsProvider(config).resolveCredentials();
        assertThat(creds, instanceOf(AwsBasicCredentials.class));
    }

    public void testSessionTokenWithoutKeysThrows() {
        S3Configuration config = S3Configuration.fromFields(null, null, "tok", null, null, null);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new S3StorageProvider(null, null).credentialsProvider(config)
        );
        assertTrue(e.getMessage().contains("S3 session_token requires access_key and secret_key"));
    }

    public void testNoCredentialsThrows() {
        S3Configuration config = S3Configuration.fromFields(null, null, "http://endpoint", "us-east-1");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new S3StorageProvider(null, null).credentialsProvider(config)
        );
        assertTrue(e.getMessage().contains("S3 data source requires credentials"));
    }
}
