/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.exception.ElasticsearchException;

import java.io.IOException;

/**
 * Thrown when a cluster state publication fails to commit the new cluster state. If publication fails then a new master is elected but the
 * update might or might not take effect, depending on whether or not the newly-elected master accepted the published state that failed to
 * be committed.
 *
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
