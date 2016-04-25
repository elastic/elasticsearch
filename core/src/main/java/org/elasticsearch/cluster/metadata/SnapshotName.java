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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

/**
 * Snapshot Name - repository name + snapshot name
 */
public class SnapshotName implements Writeable {

    private final String repository;

    private final String snapshot;

    // Caching hash code
    private int hashCode;

    private SnapshotName(final StreamInput in) throws IOException {
        this.repository = in.readString();
        this.snapshot = in.readString();
        this.hashCode = computeHashCode();
    }

    /**
     * Constructs new snapshot id
     *
     * @param repository repository name
     * @param snapshot   snapshot name
     */
    public SnapshotName(final String repository, final String snapshot) {
        Objects.requireNonNull(repository);
        Objects.requireNonNull(snapshot);
        this.repository = repository;
        this.snapshot = snapshot;
        this.hashCode = computeHashCode();
    }

    /**
     * Returns repository name
     *
     * @return repository name
     */
    public String getRepository() {
        return repository;
    }

    /**
     * Returns snapshot name
     *
     * @return snapshot name
     */
    public String getSnapshot() {
        return snapshot;
    }

    @Override
    public String toString() {
        return repository + ":" + snapshot;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SnapshotName snapshotName = (SnapshotName) o;
        return snapshot.equals(snapshotName.snapshot) && repository.equals(snapshotName.repository);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    private int computeHashCode() {
        return Objects.hash(repository, snapshot);
    }

    /**
     * Reads snapshot name from stream input
     *
     * @param in stream input
     * @return snapshot name
     */
    public static SnapshotName readSnapshotName(final StreamInput in) throws IOException {
        return new SnapshotName(in);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeString(repository);
        out.writeString(snapshot);
    }
}
