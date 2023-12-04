/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.s3.advancedstoragetiering;

import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.repositories.s3.S3StorageClassStrategy;
import org.elasticsearch.repositories.s3.SimpleS3StorageClassStrategyProvider;
import org.elasticsearch.test.ESIntegTestCase;

import static com.amazonaws.services.s3.model.StorageClass.OneZoneInfrequentAccess;
import static com.amazonaws.services.s3.model.StorageClass.Standard;
import static com.amazonaws.services.s3.model.StorageClass.StandardInfrequentAccess;
import static org.elasticsearch.repositories.s3.S3RepositoryPlugin.getStorageClassStrategyProvider;

public class S3AdvancedStorageTieringIT extends ESIntegTestCase {

    public void testDefaultStrategy() {
        final var defaultStrategy = getStrategy(Settings.builder());
        assertEquals("STANDARD", defaultStrategy.toString());
        for (final var purpose : OperationPurpose.values()) {
            assertEquals(purpose.toString(), Standard, defaultStrategy.getStorageClass(purpose));
        }
    }

    public void testConstantStrategy() {
        final var constantStrategy = getStrategy(
            Settings.builder().put(SimpleS3StorageClassStrategyProvider.STORAGE_CLASS_SETTING.getKey(), OneZoneInfrequentAccess.toString())
        );
        assertEquals("ONEZONE_IA", constantStrategy.toString());
        for (final var purpose : OperationPurpose.values()) {
            assertEquals(purpose.toString(), OneZoneInfrequentAccess, constantStrategy.getStorageClass(purpose));
        }
    }

    public void testAdvancedStrategy() {
        final var advancedStrategy = getStrategy(
            Settings.builder()
                .put(SimpleS3StorageClassStrategyProvider.STORAGE_CLASS_SETTING.getKey(), OneZoneInfrequentAccess.toString())
                .put(AdvancedS3StorageClassStrategyProvider.METADATA_STORAGE_CLASS_SETTING.getKey(), StandardInfrequentAccess.toString())
        );
        assertEquals("Advanced[data=ONEZONE_IA, metadata=STANDARD_IA]", advancedStrategy.toString());
        for (final var purpose : OperationPurpose.values()) {
            assertEquals(
                purpose.toString(),
                purpose == OperationPurpose.INDICES ? OneZoneInfrequentAccess : StandardInfrequentAccess,
                advancedStrategy.getStorageClass(purpose)
            );
        }
    }

    private static S3StorageClassStrategy getStrategy(Settings.Builder settings) {
        return asInstanceOf(
            AdvancedS3StorageClassStrategyProvider.class,
            getStorageClassStrategyProvider(internalCluster().getInstance(PluginsService.class))
        ).getS3StorageClassStrategy(settings.build());
    }
}
