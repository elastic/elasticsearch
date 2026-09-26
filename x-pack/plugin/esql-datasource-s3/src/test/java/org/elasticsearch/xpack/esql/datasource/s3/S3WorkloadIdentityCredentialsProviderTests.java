/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.identity.spi.ResolveIdentityRequest;
import software.amazon.awssdk.services.sts.StsAsyncClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleWithWebIdentityRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleWithWebIdentityResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.workload.identity.aws.AsyncWebIdentityCredentialsProvider;
import org.elasticsearch.workloadidentity.spi.WorkloadIdentityIssuerClient;
import org.elasticsearch.workloadidentity.spi.WorkloadIdentityRegistry;
import org.junit.After;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;

/**
 * Tests the federated workload-identity wiring in {@link S3StorageProvider}: building the
 * {@link AsyncWebIdentityCredentialsProvider} and the enablement guard on construction.
 */
public class S3WorkloadIdentityCredentialsProviderTests extends ESTestCase {

    private static final String ROLE_ARN = "arn:aws:iam::123456789012:role/example";
    private static final String JWT_AUDIENCE = "arn:aws:iam::123456789012:role/example";

    @After
    public void resetWorkloadIdentityRegistry() {
        WorkloadIdentityRegistry.reset();
    }

    public void testResolvesCredentialsViaWebIdentityExchange() throws Exception {
        WorkloadIdentityIssuerClient issuerClient = (request, listener) -> listener.onResponse(
            new WorkloadIdentityIssuerClient.IssueTokenResponse("signed.jwt", Instant.now().plusSeconds(300))
        );
        AtomicReference<AssumeRoleWithWebIdentityRequest> captured = new AtomicReference<>();
        StubStsAsyncClient sts = new StubStsAsyncClient(captured, "AK-1");

        S3Configuration config = S3Configuration.fromFederatedFields(ROLE_ARN, null, JWT_AUDIENCE, null, null, null, null);
        AsyncWebIdentityCredentialsProvider provider = S3StorageProvider.buildWorkloadIdentityCredentialsProvider(
            config,
            issuerClient,
            sts
        );

        AwsCredentialsIdentity identity = provider.resolveIdentity(ResolveIdentityRequest.builder().build()).get();
        assertEquals("AK-1", identity.accessKeyId());

        // The minted token and role configuration flow through to the STS request.
        assertEquals(ROLE_ARN, captured.get().roleArn());
        assertEquals("signed.jwt", captured.get().webIdentityToken());
        assertEquals("elasticsearch-esql-datasource", captured.get().roleSessionName());
    }

    public void testUsesConfiguredRoleSessionName() throws Exception {
        WorkloadIdentityIssuerClient issuerClient = (request, listener) -> listener.onResponse(
            new WorkloadIdentityIssuerClient.IssueTokenResponse("signed.jwt", Instant.now().plusSeconds(300))
        );
        AtomicReference<AssumeRoleWithWebIdentityRequest> captured = new AtomicReference<>();
        StubStsAsyncClient sts = new StubStsAsyncClient(captured, "AK-1");

        S3Configuration config = S3Configuration.fromFederatedFields(ROLE_ARN, "custom-session", JWT_AUDIENCE, null, null, null, null);
        AsyncWebIdentityCredentialsProvider provider = S3StorageProvider.buildWorkloadIdentityCredentialsProvider(
            config,
            issuerClient,
            sts
        );

        provider.resolveIdentity(ResolveIdentityRequest.builder().build()).get();
        assertEquals("custom-session", captured.get().roleSessionName());
    }

    public void testUsesDefaultJwtAudienceWhenOmitted() throws Exception {
        AtomicReference<String> requestedAudience = new AtomicReference<>();
        WorkloadIdentityIssuerClient issuerClient = (request, listener) -> {
            requestedAudience.set(request.audience());
            listener.onResponse(new WorkloadIdentityIssuerClient.IssueTokenResponse("signed.jwt", Instant.now().plusSeconds(300)));
        };
        AtomicReference<AssumeRoleWithWebIdentityRequest> captured = new AtomicReference<>();
        StubStsAsyncClient sts = new StubStsAsyncClient(captured, "AK-1");

        S3Configuration config = S3Configuration.fromFederatedFields(ROLE_ARN, null, null, null, null, null, null);
        AsyncWebIdentityCredentialsProvider provider = S3StorageProvider.buildWorkloadIdentityCredentialsProvider(
            config,
            issuerClient,
            sts
        );

        provider.resolveIdentity(ResolveIdentityRequest.builder().build()).get();
        assertEquals("sts.amazonaws.com", requestedAudience.get());
        assertEquals("signed.jwt", captured.get().webIdentityToken());
    }

    public void testConstructionFailsWhenWorkloadIdentityDisabled() {
        WorkloadIdentityRegistry.setIssuerClient(new WorkloadIdentityIssuerClient() {
            @Override
            public boolean isEnabled() {
                return false;
            }

            @Override
            public void issueToken(IssueTokenRequest request, ActionListener<IssueTokenResponse> listener) {
                throw new UnsupportedOperationException("not expected");
            }
        });
        S3Configuration config = S3Configuration.fromFederatedFields(ROLE_ARN, null, JWT_AUDIENCE, null, null, null, null);
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> new S3StorageProvider(config));
        assertThat(e.getMessage(), containsString("workload-identity"));
    }

    public void testConstructionSucceedsWhenWorkloadIdentityEnabled() throws Exception {
        WorkloadIdentityRegistry.setIssuerClient((request, listener) -> fail("token request is not expected during client construction"));
        S3Configuration config = S3Configuration.fromFederatedFields(ROLE_ARN, null, JWT_AUDIENCE, null, null, null, "us-east-1");
        try (S3StorageProvider provider = new S3StorageProvider(config)) {
            assertNotNull(provider);
        }
    }

    /**
     * Minimal {@link StsAsyncClient} test double: AWS SDK service-client interfaces declare every operation as a
     * {@code default} method, so only {@code serviceName}/{@code close} and the operation under test are overridden.
     */
    private static final class StubStsAsyncClient implements StsAsyncClient {
        private final AtomicReference<AssumeRoleWithWebIdentityRequest> captured;
        private final String accessKeyId;

        StubStsAsyncClient(AtomicReference<AssumeRoleWithWebIdentityRequest> captured, String accessKeyId) {
            this.captured = captured;
            this.accessKeyId = accessKeyId;
        }

        @Override
        public CompletableFuture<AssumeRoleWithWebIdentityResponse> assumeRoleWithWebIdentity(AssumeRoleWithWebIdentityRequest request) {
            captured.set(request);
            AssumeRoleWithWebIdentityResponse response = AssumeRoleWithWebIdentityResponse.builder()
                .credentials(
                    Credentials.builder()
                        .accessKeyId(accessKeyId)
                        .secretAccessKey("secret")
                        .sessionToken("token")
                        .expiration(Instant.now().plus(Duration.ofMinutes(10)))
                        .build()
                )
                .build();
            return CompletableFuture.completedFuture(response);
        }

        @Override
        public String serviceName() {
            return "sts";
        }

        @Override
        public void close() {}
    }
}
