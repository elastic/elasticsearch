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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

/**
 * Basic information about a snapshot - a SnapshotId and the repository that the snapshot belongs to.
 */
public final class Snapshot implements Writeable {

    private final String repository;
    private final SnapshotId snapshotId;
    private final int hashCode;

    /**
     * Constructs a snapshot.
     */
    public Snapshot(final String repository, final SnapshotId snapshotId) {
        this.repository = Objects.requireNonNull(repository);
        this.snapshotId = Objects.requireNonNull(snapshotId);
        this.hashCode = computeHashCode();
    }

    /**
     * Constructs a snapshot from the stream input.
     */
    public Snapshot(final StreamInput in) throws IOException {
        repository = in.readString();
        snapshotId = new SnapshotId(in);
        hashCode = computeHashCode();
    }

    /**
     * Gets the repository name for the snapshot.
     */
    public String getRepository() {
        return repository;
    }

    /**
     * Gets the snapshot id for the snapshot.
     */
    public SnapshotId getSnapshotId() {
        return snapshotId;
    }

    @Override
    public String toString() {
        return repository + ":" + snapshotId.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Snapshot that = (Snapshot) o;
        return repository.equals(that.repository) && snapshotId.equals(that.snapshotId);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    private int computeHashCode() {
        return Objects.hash(repository, snapshotId);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeString(repository);
        snapshotId.writeTo(out);
    }

}
