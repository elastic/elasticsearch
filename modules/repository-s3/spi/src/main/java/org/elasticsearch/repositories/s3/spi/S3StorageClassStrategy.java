/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.s3.spi;

import org.elasticsearch.common.blobstore.OperationPurpose;

/**
 * A strategy for computing the storage class for uploads to S3
 */
public interface S3StorageClassStrategy {
    /**
     * @return the storage class for an upload to S3 for the given purpose.
     */
    S3StorageClass getStorageClass(OperationPurpose operationPurpose);
}
