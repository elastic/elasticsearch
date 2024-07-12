/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.s3.advancedstoragetiering;

import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicensingHelper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.s3.spi.S3StorageClassStrategy;
import org.elasticsearch.repositories.s3.spi.SimpleS3StorageClassStrategyProvider;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;

import static org.elasticsearch.repositories.s3.spi.S3StorageClass.ONEZONE_IA;
import static org.elasticsearch.repositories.s3.spi.S3StorageClass.STANDARD;
import static org.elasticsearch.repositories.s3.spi.S3StorageClass.STANDARD_IA;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;

public class S3AdvancedStorageTieringIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), LocalStateS3AdvancedStorageTieringPlugin.class);
    }

    public void testDefaultStrategy() {
        LicensingHelper.enableLicensing(internalCluster(), randomOperationMode());
        final var defaultStrategy = getStrategy(Settings.builder());
        assertEquals("STANDARD", defaultStrategy.toString());
        for (final var purpose : OperationPurpose.values()) {
            assertEquals(purpose.toString(), STANDARD, defaultStrategy.getStorageClass(purpose));
        }
    }

    public void testConstantStrategy() {
        LicensingHelper.enableLicensing(internalCluster(), randomOperationMode());
        final var constantStrategy = getStrategy(
            Settings.builder().put(SimpleS3StorageClassStrategyProvider.STORAGE_CLASS_SETTING.getKey(), ONEZONE_IA.toString())
        );
        assertEquals("ONEZONE_IA", constantStrategy.toString());
        for (final var purpose : OperationPurpose.values()) {
            assertEquals(purpose.toString(), ONEZONE_IA, constantStrategy.getStorageClass(purpose));
        }
    }

    public void testAdvancedStrategyWithValidLicense() {
        LicensingHelper.enableLicensing(internalCluster(), randomFrom(License.OperationMode.ENTERPRISE, License.OperationMode.TRIAL));
        final var advancedStrategy = getStrategy(
            Settings.builder()
                .put(SimpleS3StorageClassStrategyProvider.STORAGE_CLASS_SETTING.getKey(), ONEZONE_IA.toString())
                .put(AdvancedS3StorageClassStrategyProvider.METADATA_STORAGE_CLASS_SETTING.getKey(), STANDARD_IA.toString())
        );
        assertEquals("Advanced[data=ONEZONE_IA, metadata=STANDARD_IA]", advancedStrategy.toString());
        for (final var purpose : OperationPurpose.values()) {
            assertEquals(
                purpose.toString(),
                purpose == OperationPurpose.SNAPSHOT_DATA ? ONEZONE_IA : STANDARD_IA,
                advancedStrategy.getStorageClass(purpose)
            );
        }
    }

    public void testAdvancedStrategyWithInvalidLicense() {
        LicensingHelper.enableLicensing(
            internalCluster(),
            randomValueOtherThanMany(
                l -> l == License.OperationMode.ENTERPRISE || l == License.OperationMode.TRIAL,
                S3AdvancedStorageTieringIT::randomOperationMode
            )
        );
        final var advancedStrategy = getStrategy(
            Settings.builder()
                .put(SimpleS3StorageClassStrategyProvider.STORAGE_CLASS_SETTING.getKey(), ONEZONE_IA.toString())
                .put(AdvancedS3StorageClassStrategyProvider.METADATA_STORAGE_CLASS_SETTING.getKey(), STANDARD_IA.toString())
        );
        assertEquals("Advanced[data=ONEZONE_IA, metadata=STANDARD_IA]", advancedStrategy.toString());
        for (final var purpose : OperationPurpose.values()) {
            assertThat(
                purpose.toString(),
                expectThrows(IllegalArgumentException.class, () -> advancedStrategy.getStorageClass(purpose)).getMessage(),
                allOf(
                    containsString("advanced storage tiering is not available with the current license level"),
                    containsString("remove the [metadata_storage_class] repository setting")
                )
            );
        }
    }

    private static License.OperationMode randomOperationMode() {
        return randomFrom(License.OperationMode.values());
    }

    private static S3StorageClassStrategy getStrategy(Settings.Builder settings) {
        return new AdvancedS3StorageClassStrategyProvider().getS3StorageClassStrategy(settings.build());
    }
}
