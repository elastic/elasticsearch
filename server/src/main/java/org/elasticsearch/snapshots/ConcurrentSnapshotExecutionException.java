/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 * Thrown when a user tries to multiple conflicting snapshot/restore operations at the same time.
 */
public class ConcurrentSnapshotExecutionException extends SnapshotException {

    public ConcurrentSnapshotExecutionException(final String repositoryName, final String snapshotName, final String msg) {
        super(repositoryName, snapshotName, msg);
    }

    public ConcurrentSnapshotExecutionException(final Snapshot snapshot, final String msg) {
        super(snapshot, msg);
    }

    public ConcurrentSnapshotExecutionException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public RestStatus status() {
        return RestStatus.SERVICE_UNAVAILABLE;
    }
}
