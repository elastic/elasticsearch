/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

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
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
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
