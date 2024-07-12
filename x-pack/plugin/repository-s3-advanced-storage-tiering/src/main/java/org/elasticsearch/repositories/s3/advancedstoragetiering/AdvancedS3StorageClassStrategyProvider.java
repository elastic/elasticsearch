/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.s3.advancedstoragetiering;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.repositories.s3.spi.S3StorageClass;
import org.elasticsearch.repositories.s3.spi.S3StorageClassStrategy;
import org.elasticsearch.repositories.s3.spi.S3StorageClassStrategyProvider;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.util.function.Supplier;

import static org.elasticsearch.repositories.s3.advancedstoragetiering.S3AdvancedStorageTieringPlugin.S3_ADVANCED_STORAGE_TIERING_FEATURE;
import static org.elasticsearch.repositories.s3.spi.SimpleS3StorageClassStrategyProvider.STORAGE_CLASS_SETTING;

public class AdvancedS3StorageClassStrategyProvider implements S3StorageClassStrategyProvider {

    /**
     * Sets the S3 storage class type for metadata objects written to S3.
     */
    public static final Setting<S3StorageClass> METADATA_STORAGE_CLASS_SETTING = Setting.enumSetting(
        S3StorageClass.class,
        "metadata_storage_class",
        STORAGE_CLASS_SETTING,
        value -> {}
    );

    // mutable global state because yolo I guess?
    static Supplier<XPackLicenseState> licenseStateSupplier = XPackPlugin::getSharedLicenseState;

    private static final Logger logger = LogManager.getLogger(AdvancedS3StorageClassStrategyProvider.class);

    @Override
    public S3StorageClassStrategy getS3StorageClassStrategy(Settings repositorySettings) {
        return new S3StorageClassStrategy() {

            private final S3StorageClass dataStorageClass = STORAGE_CLASS_SETTING.get(repositorySettings);
            private final S3StorageClass metadataStorageClass = METADATA_STORAGE_CLASS_SETTING.get(repositorySettings);

            @Override
            public S3StorageClass getStorageClass(OperationPurpose operationPurpose) {

                logger.info("computing storage class for [{}]", operationPurpose);

                if (metadataStorageClass == dataStorageClass) {
                    return dataStorageClass;
                }

                final var licenseState = licenseStateSupplier.get();
                if (S3_ADVANCED_STORAGE_TIERING_FEATURE.check(licenseState) == false) {
                    throw new IllegalArgumentException(
                        Strings.format(
                            """
                                advanced storage tiering is not available with the current license level of [%s]; \
                                either install an appropriate license or remove the [%s] repository setting""",
                            licenseState.getOperationMode(),
                            METADATA_STORAGE_CLASS_SETTING.getKey()
                        )
                    );
                }

                if (operationPurpose == OperationPurpose.SNAPSHOT_DATA) {
                    return dataStorageClass;
                } else {
                    return metadataStorageClass;
                }
            }

            @Override
            public String toString() {
                if (dataStorageClass == metadataStorageClass) {
                    return dataStorageClass.toString();
                } else {
                    return "Advanced[data=" + dataStorageClass + ", metadata=" + metadataStorageClass + "]";
                }
            }
        };
    }
}
