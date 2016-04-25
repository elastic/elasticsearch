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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.cluster.metadata.SnapshotName;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * A unique identification for a snapshot.  Combines the {@link SnapshotName} with a UUID.
 */
public final class SnapshotId implements Writeable, ToXContent {

    private final SnapshotName snapshotName;

    private final String uuid;

    private SnapshotId(final StreamInput in) throws IOException {
        snapshotName = SnapshotName.readSnapshotName(in);
        uuid = in.readString();
    }

    private SnapshotId(final SnapshotName snapshotName, final String uuid) {
        Objects.requireNonNull(snapshotName);
        Objects.requireNonNull(uuid);
        this.snapshotName = snapshotName;
        this.uuid = uuid;
    }

    /**
     * Create a SnapshotId with a new UUID.
     */
    public static SnapshotId create(final SnapshotName snapshotName) {
        return new SnapshotId(snapshotName, UUIDs.randomBase64UUID());
    }

    /**
     * Get a SnapshotId from a snapshot name and a UUID.
     */
    public static SnapshotId get(final SnapshotName snapshotName, final String uuid) {
        return new SnapshotId(snapshotName, uuid);
    }

    /**
     * Returns the snapshot name.
     */
    public SnapshotName getSnapshotName() {
        return snapshotName;
    }

    /**
     * Returns the snapshot uuid.
     */
    public String getUUID() {
        return uuid;
    }

    /**
     * Returns the snapshot name.
     */
    public String getName() {
        return snapshotName.getSnapshot();
    }

    @Override
    public String toString() {
        return snapshotName.toString() + "/" + uuid;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        @SuppressWarnings("unchecked") SnapshotId snapshotTag = (SnapshotId) o;
        return snapshotName.equals(snapshotTag.snapshotName) && uuid.equals(snapshotTag.uuid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshotName, uuid);
    }

    /**
     * Reads snapshot tag from stream input
     */
    public static SnapshotId readSnapshotId(final StreamInput in) throws IOException {
        return new SnapshotId(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        snapshotName.writeTo(out);
        out.writeString(uuid);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject(Fields.SNAPSHOT);
        builder.field(Fields.REPOSITORY, snapshotName.getRepository());
        builder.field(Fields.NAME, snapshotName.getSnapshot());
        builder.field(Fields.UUID, uuid);
        return builder.endObject();
    }

    public static SnapshotId fromXContent(final XContentParser parser) throws IOException {
        assert parser.currentToken() == XContentParser.Token.START_OBJECT;
        XContentParser.Token token;
        String currentFieldName;
        String repository = null;
        String name = null;
        String uuid = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
                token = parser.nextToken();
                if (token.isValue()) {
                    if (Fields.NAME.equals(currentFieldName)) {
                        name = parser.text();
                    } else if (Fields.REPOSITORY.equals(currentFieldName)) {
                        repository = parser.text();
                    } else if (Fields.UUID.equals(currentFieldName)) {
                        uuid = parser.text();
                    }
                } else {
                    throw new ElasticsearchParseException("unexpected token  [" + token + "] on " + currentFieldName);
                }
            } else {
                throw new ElasticsearchParseException("unexpected token  [" + token + "]");
            }
        }
        return new SnapshotId(new SnapshotName(repository, name), uuid);
    }

    public static final class Fields {
        static final String SNAPSHOT = "snapshot_id";
        static final String REPOSITORY = "repository";
        public static final String NAME = "name";
        public static final String UUID = "uuid";
    }
}
