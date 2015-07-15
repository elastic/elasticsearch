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
import org.elasticsearch.cluster.metadata.SnapshotId;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Represents snapshot and index from which a recovering index should be restored
 */
public class RestoreSource implements Streamable, ToXContent {

    private SnapshotId snapshotId;

    private String index;

    private Version version;

    RestoreSource() {
    }

    public RestoreSource(SnapshotId snapshotId, Version version, String index) {
        this.snapshotId = snapshotId;
        this.version = version;
        this.index = index;
    }

    public SnapshotId snapshotId() {
        return snapshotId;
    }

    public String index() {
        return index;
    }

    public Version version() {
        return version;
    }

    public static RestoreSource readRestoreSource(StreamInput in) throws IOException {
        RestoreSource restoreSource = new RestoreSource();
        restoreSource.readFrom(in);
        return restoreSource;
    }

    public static RestoreSource readOptionalRestoreSource(StreamInput in) throws IOException {
        return in.readOptionalStreamable(new RestoreSource());
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        snapshotId = SnapshotId.readSnapshotId(in);
        version = Version.readVersion(in);
        index = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        snapshotId.writeTo(out);
        Version.writeVersion(version, out);
        out.writeString(index);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
                .field("repository", snapshotId.getRepository())
                .field("snapshot", snapshotId.getSnapshot())
                .field("version", version.toString())
                .field("index", index)
                .endObject();
    }

    @Override
    public String toString() {
        return snapshotId.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RestoreSource that = (RestoreSource) o;

        if (!index.equals(that.index)) return false;
        if (!snapshotId.equals(that.snapshotId)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = snapshotId.hashCode();
        result = 31 * result + index.hashCode();
        return result;
    }
}
