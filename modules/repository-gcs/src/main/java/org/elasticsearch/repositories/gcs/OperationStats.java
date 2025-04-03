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
     * operation start time (system milliseconds)
     */
    long startTimeMs;

    /**
     * true if operation completed successfully
     */
    boolean isSuccess;

    /**
     * request attempts including retires and multi part requests
     */
    int reqAtt;

    /**
     * request errors, all unsuccessful request attempts {@code reqErr<=reqAtt}
     */
    int reqErr;

    /**
     * billable errors, such as 4xx, {@code reqBillableErr<=reqErr}
     */
    int reqBillableErr;

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
        this.startTimeMs = 0;
    }

    @Override
    public String toString() {
        return "OperationStats{"
            + "purpose="
            + purpose
            + ", operation="
            + operation
            + ", startTimeMs="
            + startTimeMs
            + ", isSuccess="
            + isSuccess
            + ", reqAtt="
            + reqAtt
            + ", reqErr="
            + reqErr
            + ", reqBillableErr="
            + reqBillableErr
            + ", reqErrThrottle="
            + reqErrThrottle
            + ", reqErrRange="
            + reqErrRange
            + '}';
    }
}
