/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.s3.spi;

import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

public class SimpleS3StorageClassStrategyProvider implements S3StorageClassStrategyProvider {

    public static final S3StorageClassStrategyProvider INSTANCE = new SimpleS3StorageClassStrategyProvider();

    private SimpleS3StorageClassStrategyProvider() {/* singleton */}

    /**
     * Sets the S3 storage class type for the objects written to S3.
     */
    public static final Setting<S3StorageClass> STORAGE_CLASS_SETTING = Setting.enumSetting(
        S3StorageClass.class,
        "storage_class",
        S3StorageClass.STANDARD
    );

    @Override
    public S3StorageClassStrategy getS3StorageClassStrategy(Settings repositorySettings) {
        return new S3StorageClassStrategy() {
            private final S3StorageClass storageClass = STORAGE_CLASS_SETTING.get(repositorySettings);

            @Override
            public S3StorageClass getStorageClass(OperationPurpose operationPurpose) {
                return storageClass;
            }

            @Override
            public String toString() {
                return storageClass.toString();
            }
        };
    }
}
