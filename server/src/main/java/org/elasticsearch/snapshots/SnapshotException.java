/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;

import java.io.IOException;

/**
 * Generic snapshot exception
 */
public class SnapshotException extends ElasticsearchException {

    @Nullable
    private final String repositoryName;
    @Nullable
    private final String snapshotName;

    public SnapshotException(final Snapshot snapshot, final String msg) {
        this(snapshot, msg, null);
    }

    public SnapshotException(final Snapshot snapshot, final String msg, final Throwable cause) {
        super("[" + (snapshot == null ? "_na" : snapshot) + "] " + msg, cause);
        if (snapshot != null) {
            this.repositoryName = snapshot.getRepository();
            this.snapshotName = snapshot.getSnapshotId().getName();
        } else {
            this.repositoryName = null;
            this.snapshotName = null;
        }
    }

    public SnapshotException(final String repositoryName, final SnapshotId snapshotId, final String msg, final Throwable cause) {
        super("[" + repositoryName + ":" + snapshotId + "] " + msg, cause);
        this.repositoryName = repositoryName;
        this.snapshotName = snapshotId.getName();
    }

    public SnapshotException(final String repositoryName, final String snapshotName, final String msg) {
        super("[" + repositoryName + ":" + snapshotName + "] " + msg);
        this.repositoryName = repositoryName;
        this.snapshotName = snapshotName;
    }

    public SnapshotException(final StreamInput in) throws IOException {
        super(in);
        repositoryName = in.readOptionalString();
        snapshotName = in.readOptionalString();
    }

    @Override
    protected void writeTo(StreamOutput out, Writer<Throwable> nestedExceptionsWriter) throws IOException {
        super.writeTo(out, nestedExceptionsWriter);
        out.writeOptionalString(repositoryName);
        out.writeOptionalString(snapshotName);
    }

    @Nullable
    public String getRepositoryName() {
        return repositoryName;
    }

    @Nullable
    public String getSnapshotName() {
        return snapshotName;
    }

}
