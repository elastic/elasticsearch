/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories.s3;

import org.elasticsearch.cluster.metadata.RepositoryMetadata;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

public class S3ServiceTests extends ESTestCase {

    public void testCachedClientsAreReleased() {
        final S3Service s3Service = new S3Service();
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
}
