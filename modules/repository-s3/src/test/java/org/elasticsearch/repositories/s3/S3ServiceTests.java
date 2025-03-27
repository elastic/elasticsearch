/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.repositories.s3;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.retry.RetryPolicyContext;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;
import software.amazon.awssdk.services.s3.model.S3Exception;

import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.Mockito.mock;

public class S3ServiceTests extends ESTestCase {

    public void testCachedClientsAreReleased() throws IOException {
        final S3Service s3Service = new S3Service(mock(Environment.class), Settings.EMPTY, mock(ResourceWatcherService.class));
        final Settings settings = Settings.builder().put("endpoint", "http://first").build();
        final RepositoryMetadata metadata1 = new RepositoryMetadata("first", "s3", settings);
        final RepositoryMetadata metadata2 = new RepositoryMetadata("second", "s3", settings);
        final S3ClientSettings clientSettings = s3Service.settings(metadata2);
        final S3ClientSettings otherClientSettings = s3Service.settings(metadata2);
        assertSame(clientSettings, otherClientSettings);
        final AmazonS3Reference reference = s3Service.client(metadata1);
        reference.close();
        s3Service.close();
        final AmazonS3Reference referenceReloaded = s3Service.client(metadata1);
        assertNotSame(referenceReloaded, reference);
        referenceReloaded.close();
        s3Service.close();
        final S3ClientSettings clientSettingsReloaded = s3Service.settings(metadata1);
        assertNotSame(clientSettings, clientSettingsReloaded);
    }

    public void testRetryOn403RetryPolicy() {
        AwsErrorDetails awsErrorDetails = AwsErrorDetails.builder().errorCode("InvalidAccessKeyId").build();
        AwsServiceException s3Exception = S3Exception.builder()
            .awsErrorDetails(awsErrorDetails)
            .statusCode(RestStatus.FORBIDDEN.getStatus())
            .build();

        // AWS default retry condition does not retry on 403
        assertFalse(
            RetryCondition.defaultRetryCondition()
                .shouldRetry(RetryPolicyContext.builder().retriesAttempted(between(0, 9)).exception(s3Exception).build())
        );

        // The retryable 403 condition retries on 403 invalid access key id
        assertTrue(
            S3Service.RETRYABLE_403_RETRY_POLICY.shouldRetry(
                RetryPolicyContext.builder().retriesAttempted(between(0, 9)).exception(s3Exception).build()
            )
        );

        if (randomBoolean()) {
            // Random for another error status that is not 403
            var non403StatusCode = randomValueOtherThan(403, () -> between(0, 600));
            var non403Exception = S3Exception.builder().statusCode(non403StatusCode).awsErrorDetails(awsErrorDetails).build();
            var retryPolicyContext = RetryPolicyContext.builder().retriesAttempted(between(0, 9)).exception(non403Exception).build();
            // Retryable 403 condition delegates to the AWS default retry condition. Its result must be consistent with the decision
            // by the AWS default, e.g. some error status like 429 is retryable by default, the retryable 403 condition respects it.
            boolean actual = S3Service.RETRYABLE_403_RETRY_POLICY.shouldRetry(retryPolicyContext);
            boolean expected = RetryCondition.defaultRetryCondition().shouldRetry(retryPolicyContext);
            assertThat(actual, equalTo(expected));
        } else {
            // Not retry for 403 with error code that is not invalid access key id
            String errorCode = randomAlphaOfLength(10);
            var exception = S3Exception.builder().awsErrorDetails(AwsErrorDetails.builder().errorCode(errorCode).build()).build();
            var retryPolicyContext = RetryPolicyContext.builder().retriesAttempted(between(0, 9)).exception(exception).build();
            assertFalse(S3Service.RETRYABLE_403_RETRY_POLICY.shouldRetry(retryPolicyContext));
        }
    }
}
