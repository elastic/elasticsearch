/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * Exception indicating that an operation failed because the node stopped being the elected master.
 * Since this exception is thrown prior to the cluster state publication, it should only be used when the cluster state update
 * *definitely* did not happen, and there is no possibility the next master committed the cluster state update.
 *
 * This is different to the {@code FailedToCommitClusterStateException}
 *
 * This exception is retryable within {@code TransportNodeMasterAction}.
 */
public class NotMasterException extends ElasticsearchException {

    public NotMasterException(String msg) {
        super(msg);
    }

    public NotMasterException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public Throwable fillInStackTrace() {
        return this;
    }
}
