/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * Exception indicating a cluster state update failed prior to publication.
 * <p>
 * If this exception is thrown, then the cluster state update was <i>not</i> published to any node.
 * It is therefore impossible for the new master to have committed this state.
 * <p>
 * For exceptions thrown <i>after</i> publication, when the cluster state update may or may not have been committed,
 * use a {@link FailedToCommitClusterStateException}.
 * <p>
 * This is a retryable exception inside {@link TransportMasterNodeAction}
 */
public class FailedToPublishClusterStateException extends ElasticsearchException {

    public FailedToPublishClusterStateException(String msg) {
        super(msg);
    }

    public FailedToPublishClusterStateException(StreamInput in) throws IOException {
        super(in);
    }

    public FailedToPublishClusterStateException(String msg, Throwable cause, Object... args) {
        super(msg, cause, args);
    }

    @Override
    public Throwable fillInStackTrace() {
        return this;
    }
}
