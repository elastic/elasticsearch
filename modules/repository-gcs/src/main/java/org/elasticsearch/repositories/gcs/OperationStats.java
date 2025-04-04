/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.gcs;

import org.elasticsearch.common.blobstore.OperationPurpose;

public class OperationStats {

    final OperationPurpose purpose;
    final StorageOperation operation;

    /**
     * total time taken for the operation
     */
    long totalDuration;

    /**
     * true if last request is completed successfully
     */
    boolean isLastReqSuccess;

    /**
     * request attempts including retires and multi part requests
     */
    int reqAtt;

    /**
     * request errors, all unsuccessful request attempts {@code reqErr<=reqAtt}
     */
    int reqErr;

    /**
     * request throttles (429),  {@code reqErrThrottle<=reqErr}
     */
    int reqErrThrottle;

    /**
     * request range not satisfied error(416), only applicable for GetObject operations, {@code reqErrRange<=reqErr}
     */
    int reqErrRange;

    OperationStats(OperationPurpose purpose, StorageOperation operation) {
        this.purpose = purpose;
        this.operation = operation;
    }

    @Override
    public String toString() {
        return "OperationStats{"
            + "purpose="
            + purpose
            + ", operation="
            + operation
            + ", totalDuration="
            + totalDuration
            + ", isLastReqSuccess="
            + isLastReqSuccess
            + ", reqAtt="
            + reqAtt
            + ", reqErr="
            + reqErr
            + ", reqErrThrottle="
            + reqErrThrottle
            + ", reqErrRange="
            + reqErrRange
            + '}';
    }
}
