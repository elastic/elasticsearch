/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.seqno;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.exception.ElasticsearchException;

import java.io.IOException;

public class RetentionLeaseInvalidRetainingSeqNoException extends ElasticsearchException {

    RetentionLeaseInvalidRetainingSeqNoException(
        String retentionLeaseId,
        String source,
        long retainingSequenceNumber,
        RetentionLease existingRetentionLease
    ) {
        super(
            "the current retention lease with ["
                + retentionLeaseId
                + "]"
                + " is retaining a higher sequence number ["
                + existingRetentionLease.retainingSequenceNumber()
                + "]"
                + " than the new retaining sequence number ["
                + retainingSequenceNumber
                + "] from ["
                + source
                + "]"
        );
    }

    public RetentionLeaseInvalidRetainingSeqNoException(StreamInput in) throws IOException {
        super(in);
    }
}
