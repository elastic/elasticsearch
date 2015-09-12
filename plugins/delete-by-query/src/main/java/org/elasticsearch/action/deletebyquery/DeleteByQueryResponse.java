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

package org.elasticsearch.action.deletebyquery;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;

import static org.elasticsearch.action.search.ShardSearchFailure.readShardSearchFailure;

/**
 * Delete by query response
 * @see DeleteByQueryRequest
 */
public class DeleteByQueryResponse extends ActionResponse implements ToXContent {

    private long tookInMillis;
    private boolean timedOut = false;

    private long found;
    private long deleted;
    private long missing;
    private long failed;

    private IndexDeleteByQueryResponse[] indices = IndexDeleteByQueryResponse.EMPTY_ARRAY;
    private ShardOperationFailedException[] shardFailures = ShardSearchFailure.EMPTY_ARRAY;

    DeleteByQueryResponse() {
    }

    DeleteByQueryResponse(long tookInMillis, boolean timedOut, long found, long deleted, long missing, long failed, IndexDeleteByQueryResponse[] indices, ShardOperationFailedException[] shardFailures) {
        this.tookInMillis = tookInMillis;
        this.timedOut = timedOut;
        this.found = found;
        this.deleted = deleted;
        this.missing = missing;
        this.failed = failed;
        this.indices = indices;
        this.shardFailures = shardFailures;
    }

    /**
     * The responses from all the different indices.
     */
    public IndexDeleteByQueryResponse[] getIndices() {
        return indices;
    }

    /**
     * The response of a specific index.
     */
    public IndexDeleteByQueryResponse getIndex(String index) {
        if (index == null) {
            return null;
        }
        for (IndexDeleteByQueryResponse i : indices) {
            if (index.equals(i.getIndex())) {
                return i;
            }
        }
        return null;
    }

    public TimeValue getTook() {
        return new TimeValue(tookInMillis);
    }

    public long getTookInMillis() {
        return tookInMillis;
    }

    public boolean isTimedOut() {
        return this.timedOut;
    }

    public long getTotalFound() {
        return found;
    }

    public long getTotalDeleted() {
        return deleted;
    }

    public long getTotalMissing() {
        return missing;
    }

    public long getTotalFailed() {
        return failed;
    }

    public ShardOperationFailedException[] getShardFailures() {
        return shardFailures;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        tookInMillis = in.readVLong();
        timedOut = in.readBoolean();
        found = in.readVLong();
        deleted = in.readVLong();
        missing = in.readVLong();
        failed = in.readVLong();

        int size = in.readVInt();
        indices = new IndexDeleteByQueryResponse[size];
        for (int i = 0; i < size; i++) {
            IndexDeleteByQueryResponse index = new IndexDeleteByQueryResponse();
            index.readFrom(in);
            indices[i] = index;
        }

        size = in.readVInt();
        if (size == 0) {
            shardFailures = ShardSearchFailure.EMPTY_ARRAY;
        } else {
            shardFailures = new ShardSearchFailure[size];
            for (int i = 0; i < shardFailures.length; i++) {
                shardFailures[i] = readShardSearchFailure(in);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(tookInMillis);
        out.writeBoolean(timedOut);
        out.writeVLong(found);
        out.writeVLong(deleted);
        out.writeVLong(missing);
        out.writeVLong(failed);

        out.writeVInt(indices.length);
        for (IndexDeleteByQueryResponse indexResponse : indices) {
            indexResponse.writeTo(out);
        }

        out.writeVInt(shardFailures.length);
        for (ShardOperationFailedException shardSearchFailure : shardFailures) {
            shardSearchFailure.writeTo(out);
        }
    }

    static final class Fields {
        static final XContentBuilderString TOOK = new XContentBuilderString("took");
        static final XContentBuilderString TIMED_OUT = new XContentBuilderString("timed_out");
        static final XContentBuilderString INDICES = new XContentBuilderString("_indices");
        static final XContentBuilderString FAILURES = new XContentBuilderString("failures");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(Fields.TOOK, tookInMillis);
        builder.field(Fields.TIMED_OUT, timedOut);

        builder.startObject(Fields.INDICES);
        IndexDeleteByQueryResponse all = new IndexDeleteByQueryResponse("_all", found, deleted, missing, failed);
        all.toXContent(builder, params);
        for (IndexDeleteByQueryResponse indexResponse : indices) {
            indexResponse.toXContent(builder, params);
        }
        builder.endObject();

        builder.startArray(Fields.FAILURES);
        if (shardFailures != null) {
            for (ShardOperationFailedException shardFailure : shardFailures) {
                builder.startObject();
                shardFailure.toXContent(builder, params);
                builder.endObject();
            }
        }
        builder.endArray();
        return builder;
    }
}
