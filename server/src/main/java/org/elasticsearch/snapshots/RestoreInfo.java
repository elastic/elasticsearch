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

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ContextParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Information about successfully completed restore operation.
 * <p>
 * Returned as part of {@link org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse}
 */
public class RestoreInfo implements ToXContentObject, Streamable {

    private String name;

    private List<String> indices;

    private int totalShards;

    private int successfulShards;

    RestoreInfo() {

    }

    public RestoreInfo(String name, List<String> indices, int totalShards, int successfulShards) {
        this.name = name;
        this.indices = indices;
        this.totalShards = totalShards;
        this.successfulShards = successfulShards;
    }

    /**
     * Snapshot name
     *
     * @return snapshot name
     */
    public String name() {
        return name;
    }

    private void setName(String name) {
        this.name = name;
    }

    /**
     * List of restored indices
     *
     * @return list of restored indices
     */
    public List<String> indices() {
        return indices;
    }

    private void setIndices(List<String> indices) {
        this.indices = indices;
    }

    /**
     * Number of shards being restored
     *
     * @return number of being restored
     */
    public int totalShards() {
        return totalShards;
    }

    /**
     * Number of failed shards
     *
     * @return number of failed shards
     */
    public int failedShards() {
        return totalShards -  successfulShards;
    }

    /**
     * Number of successful shards
     *
     * @return number of successful shards
     */
    public int successfulShards() {
        return successfulShards;
    }

    private void setShards(Shards shards) {
        this.successfulShards = shards.getSuccessfulShards();
        this.totalShards = shards.getTotalShards();
    }

    /**
     * REST status of the operation
     *
     * @return REST status
     */
    public RestStatus status() {
        return RestStatus.OK;
    }

    static final class Fields {
        static final String SNAPSHOT = "snapshot";
        static final String INDICES = "indices";
        static final String SHARDS = "shards";
        static final String TOTAL = "total";
        static final String FAILED = "failed";
        static final String SUCCESSFUL = "successful";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Fields.SNAPSHOT, name);
        builder.startArray(Fields.INDICES);
        for (String index : indices) {
            builder.value(index);
        }
        builder.endArray();
        builder.startObject(Fields.SHARDS);
        builder.field(Fields.TOTAL, totalShards);
        builder.field(Fields.FAILED, failedShards());
        builder.field(Fields.SUCCESSFUL, successfulShards);
        builder.endObject();
        builder.endObject();
        return builder;
    }

    /**
     * Internal helper class solely for XContent parsing.
     */
    private static final class Shards {
        private static final ContextParser<Void, Shards> PARSER;

        static {
            ObjectParser<Shards, Void> internalParser = new ObjectParser<>("shards");
            internalParser.declareInt(Shards::setTotalShards, new ParseField(Fields.TOTAL));
            internalParser.declareInt(Shards::setFailedShards, new ParseField(Fields.FAILED));
            internalParser.declareInt(Shards::setSuccessfulShards, new ParseField(Fields.SUCCESSFUL));
            PARSER = (p, v) -> internalParser.parse(p, new Shards(), null);
        }

        private int totalShards;

        private int failedShards;

        private int successfulShards;

        public int getTotalShards() {
            return totalShards;
        }

        public void setTotalShards(int totalShards) {
            this.totalShards = totalShards;
        }

        public int getFailedShards() {
            return failedShards;
        }

        public void setFailedShards(int failedShards) {
            this.failedShards = failedShards;
        }

        public int getSuccessfulShards() {
            return successfulShards;
        }

        public void setSuccessfulShards(int successfulShards) {
            this.successfulShards = successfulShards;
        }
    }

    private static final ObjectParser<RestoreInfo, Void> PARSER = new ObjectParser<>(RestoreInfo.class.getName(), RestoreInfo::new);

    static {
        PARSER.declareString(RestoreInfo::setName, new ParseField(Fields.SNAPSHOT));
        PARSER.declareStringArray(RestoreInfo::setIndices, new ParseField(Fields.INDICES));
        PARSER.declareObject(RestoreInfo::setShards, Shards.PARSER, new ParseField(Fields.SHARDS));
    }

    public static RestoreInfo fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        int size = in.readVInt();
        List<String> indicesListBuilder = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            indicesListBuilder.add(in.readString());
        }
        indices = Collections.unmodifiableList(indicesListBuilder);
        totalShards = in.readVInt();
        successfulShards = in.readVInt();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeVInt(indices.size());
        for (String index : indices) {
            out.writeString(index);
        }
        out.writeVInt(totalShards);
        out.writeVInt(successfulShards);
    }

    /**
     * Reads restore info from {@link StreamInput}
     *
     * @param in stream input
     * @return restore info
     */
    public static RestoreInfo readRestoreInfo(StreamInput in) throws IOException {
        RestoreInfo snapshotInfo = new RestoreInfo();
        snapshotInfo.readFrom(in);
        return snapshotInfo;
    }

    /**
     * Reads optional restore info from {@link StreamInput}
     *
     * @param in stream input
     * @return restore info
     */
    public static RestoreInfo readOptionalRestoreInfo(StreamInput in) throws IOException {
        return in.readOptionalStreamable(RestoreInfo::new);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RestoreInfo that = (RestoreInfo) o;
        return totalShards == that.totalShards &&
            successfulShards == that.successfulShards &&
            Objects.equals(name, that.name) &&
            Objects.equals(indices, that.indices);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, indices, totalShards, successfulShards);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
