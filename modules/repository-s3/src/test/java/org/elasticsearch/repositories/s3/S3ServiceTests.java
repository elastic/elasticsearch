/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.repositories.s3;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.services.s3.model.AmazonS3Exception;

import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.IOException;

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
        final AmazonS3Exception e = new AmazonS3Exception("error");
        e.setStatusCode(403);
        e.setErrorCode("InvalidAccessKeyId");

        // Retry on 403 invalid access key id
        assertTrue(
            S3Service.RETRYABLE_403_RETRY_POLICY.getRetryCondition().shouldRetry(mock(AmazonWebServiceRequest.class), e, between(0, 9))
        );

        // Not retry if not 403 or not invalid access key id
        if (randomBoolean()) {
            e.setStatusCode(randomValueOtherThan(403, () -> between(0, 600)));
        } else {
            e.setErrorCode(randomAlphaOfLength(10));
        }
        assertFalse(
            S3Service.RETRYABLE_403_RETRY_POLICY.getRetryCondition().shouldRetry(mock(AmazonWebServiceRequest.class), e, between(0, 9))
        );
    }
}
