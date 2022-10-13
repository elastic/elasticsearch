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
 * Thrown on the attempt to create a snapshot with a name that is taken by a snapshot in progress and a snapshot that already exists.
 */
public class SnapshotNameAlreadyInUseException extends InvalidSnapshotNameException {

    public SnapshotNameAlreadyInUseException(final String repositoryName, final String snapshotName, String desc) {
        super(repositoryName, snapshotName, desc);
    }

    public SnapshotNameAlreadyInUseException(StreamInput in) throws IOException {
        super(in);
    }
}
