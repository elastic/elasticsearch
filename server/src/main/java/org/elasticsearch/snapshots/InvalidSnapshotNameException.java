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
 * Thrown on the attempt to create a snapshot with invalid name
 */
public class InvalidSnapshotNameException extends SnapshotException {

    public InvalidSnapshotNameException(final String repositoryName, final String snapshotName, String desc) {
        super(repositoryName, snapshotName, "Invalid snapshot name [" + snapshotName + "], " + desc);
    }

    public InvalidSnapshotNameException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }
}
