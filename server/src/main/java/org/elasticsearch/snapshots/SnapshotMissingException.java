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
 * Thrown if requested snapshot doesn't exist
 */
public class SnapshotMissingException extends SnapshotException {

    public SnapshotMissingException(final String repositoryName, final SnapshotId snapshotId, final Throwable cause) {
        super(repositoryName, snapshotId, "is missing", cause);
    }

    public SnapshotMissingException(final String repositoryName, final String snapshotName) {
        super(repositoryName, snapshotName, "is missing");
    }

    public SnapshotMissingException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public RestStatus status() {
        return RestStatus.NOT_FOUND;
    }
}
