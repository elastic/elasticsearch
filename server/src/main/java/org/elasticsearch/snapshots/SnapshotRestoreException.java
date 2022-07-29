/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * Snapshot restore exception
 */
public class SnapshotRestoreException extends SnapshotException {
    public SnapshotRestoreException(final String repositoryName, final String snapshotName, final String message) {
        super(repositoryName, snapshotName, message);
    }

    public SnapshotRestoreException(final Snapshot snapshot, final String message) {
        super(snapshot, message);
    }

    public SnapshotRestoreException(final Snapshot snapshot, final String message, final Throwable cause) {
        super(snapshot, message, cause);
    }

    public SnapshotRestoreException(StreamInput in) throws IOException {
        super(in);
    }
}
