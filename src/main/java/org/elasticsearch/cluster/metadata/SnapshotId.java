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
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.io.Serializable;

/**
 * Snapshot ID - repository name + snapshot name
 */
public class SnapshotId implements Serializable, Streamable {

    private String repository;

    private String snapshot;

    // Caching hash code
    private int hashCode;

    private SnapshotId() {
    }

    /**
     * Constructs new snapshot id
     *
     * @param repository repository name
     * @param snapshot   snapshot name
     */
    public SnapshotId(String repository, String snapshot) {
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null) return false;
        SnapshotId snapshotId = (SnapshotId) o;
        return snapshot.equals(snapshotId.snapshot) && repository.equals(snapshotId.repository);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    private int computeHashCode() {
        int result = repository != null ? repository.hashCode() : 0;
        result = 31 * result + snapshot.hashCode();
        return result;
    }

    /**
     * Reads snapshot id from stream input
     *
     * @param in stream input
     * @return snapshot id
     * @throws IOException
     */
    public static SnapshotId readSnapshotId(StreamInput in) throws IOException {
        SnapshotId snapshot = new SnapshotId();
        snapshot.readFrom(in);
        return snapshot;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readFrom(StreamInput in) throws IOException {
        repository = in.readString();
        snapshot = in.readString();
        hashCode = computeHashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(repository);
        out.writeString(snapshot);
    }
}
