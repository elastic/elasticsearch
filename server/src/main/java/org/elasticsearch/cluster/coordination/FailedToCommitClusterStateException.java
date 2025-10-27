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
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * Exception indicating a cluster state update was published and may or may not have been committed.
 * <p>
 * If this exception is thrown, then the cluster state update was published, but is not guaranteed
 * to be committed, including the next master node. This exception should only be thrown when there is
 * <i>ambiguity</i> whether a cluster state update has been committed.
 * <p>
 * For exceptions thrown prior to publication,
 * when the cluster update has <i>definitely</i> failed, use a different exception.
 * <p>
 * If during a cluster state update the node is no longer master, use a {@link NotMasterException}
 * <p>
 * This is a retryable exception inside {@link TransportMasterNodeAction}
 * <p>
 * See {@link ClusterStatePublisher} for more details.
 */
public class FailedToCommitClusterStateException extends ElasticsearchException {

    public FailedToCommitClusterStateException(StreamInput in) throws IOException {
        super(in);
    }

    public FailedToCommitClusterStateException(String msg, Object... args) {
        super(msg, args);
    }

    public FailedToCommitClusterStateException(String msg, Throwable cause, Object... args) {
        super(msg, cause, args);
    }
}
