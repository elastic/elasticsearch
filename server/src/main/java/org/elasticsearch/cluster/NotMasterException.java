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
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.coordination.FailedToCommitClusterStateException;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * Exception indicating that a cluster state update operation failed because the node stopped being the elected master.
 * Since this exception is thrown prior to the cluster state publication, it should only be used when the cluster state update
 * <i>definitely</i> did not happen, and there is no possibility the next master committed the cluster state update.
 *
 * This is different from {@link FailedToCommitClusterStateException}.
 *
 * This exception is retryable within {@link TransportMasterNodeAction}.
 */
public class NotMasterException extends ElasticsearchException {

    public NotMasterException(String msg) {
        super(msg);
    }

    public NotMasterException(StreamInput in) throws IOException {
        super(in);
    }

    public NotMasterException(String msg, Object... args) {
        super(msg, args);
    }

    public NotMasterException(String msg, Throwable cause, Object... args) {
        super(msg, cause, args);
    }

    @Override
    public Throwable fillInStackTrace() {
        return this;
    }
}
