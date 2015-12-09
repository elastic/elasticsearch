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
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Information about successfully completed restore operation.
 * <p>
 * Returned as part of {@link org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse}
 */
public class RestoreInfo implements ToXContent, Streamable {

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

    /**
     * List of restored indices
     *
     * @return list of restored indices
     */
    public List<String> indices() {
        return indices;
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

    /**
     * REST status of the operation
     *
     * @return REST status
     */
    public RestStatus status() {
        return RestStatus.OK;
    }

    static final class Fields {
        static final XContentBuilderString SNAPSHOT = new XContentBuilderString("snapshot");
        static final XContentBuilderString INDICES = new XContentBuilderString("indices");
        static final XContentBuilderString SHARDS = new XContentBuilderString("shards");
        static final XContentBuilderString TOTAL = new XContentBuilderString("total");
        static final XContentBuilderString FAILED = new XContentBuilderString("failed");
        static final XContentBuilderString SUCCESSFUL = new XContentBuilderString("successful");
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

}