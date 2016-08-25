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

package org.elasticsearch.cluster.routing;

import org.elasticsearch.Version;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Represents snapshot and index from which a recovering index should be restored
 */
public class RestoreSource implements Streamable, ToXContent {

    private Snapshot snapshot;

    private String index;

    private Version version;

    RestoreSource() {
    }

    public RestoreSource(Snapshot snapshot, Version version, String index) {
        this.snapshot = Objects.requireNonNull(snapshot);
        this.version = Objects.requireNonNull(version);
        this.index = Objects.requireNonNull(index);
    }

    public Snapshot snapshot() {
        return snapshot;
    }

    public String index() {
        return index;
    }

    public Version version() {
        return version;
    }

    public static RestoreSource readOptionalRestoreSource(StreamInput in) throws IOException {
        return in.readOptionalStreamable(RestoreSource::new);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        snapshot = new Snapshot(in);
        version = Version.readVersion(in);
        index = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        snapshot.writeTo(out);
        Version.writeVersion(version, out);
        out.writeString(index);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
                .field("repository", snapshot.getRepository())
                .field("snapshot", snapshot.getSnapshotId().getName())
                .field("version", version.toString())
                .field("index", index)
                .endObject();
    }

    @Override
    public String toString() {
        return snapshot.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        @SuppressWarnings("unchecked") RestoreSource that = (RestoreSource) o;
        return snapshot.equals(that.snapshot) && index.equals(that.index);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshot, index);
    }
}
