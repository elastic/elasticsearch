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
import org.elasticsearch.cluster.metadata.SnapshotName;
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

    private SnapshotName snapshotName;

    private String index;

    private Version version;

    RestoreSource() {
    }

    public RestoreSource(SnapshotName snapshotName, Version version, String index) {
        this.snapshotName = snapshotName;
        this.version = version;
        this.index = index;
    }

    public SnapshotName snapshotName() {
        return snapshotName;
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
        return in.readOptionalStreamable(RestoreSource::new);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        snapshotName = SnapshotName.readSnapshotName(in);
        version = Version.readVersion(in);
        index = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        snapshotName.writeTo(out);
        Version.writeVersion(version, out);
        out.writeString(index);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
                .field("repository", snapshotName.getRepository())
                .field("snapshot", snapshotName.getSnapshot())
                .field("version", version.toString())
                .field("index", index)
                .endObject();
    }

    @Override
    public String toString() {
        return snapshotName.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RestoreSource that = (RestoreSource) o;

        if (!index.equals(that.index)) return false;
        if (!snapshotName.equals(that.snapshotName)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = snapshotName.hashCode();
        result = 31 * result + index.hashCode();
        return result;
    }
}
