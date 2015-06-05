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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;

/**
 * Delete by query response executed on a specific index.
 */
public class IndexDeleteByQueryResponse extends ActionResponse implements ToXContent {

    public static final IndexDeleteByQueryResponse[] EMPTY_ARRAY = new IndexDeleteByQueryResponse[0];

    private String index;

    private long found = 0L;
    private long deleted = 0L;
    private long missing = 0L;
    private long failed = 0L;

    IndexDeleteByQueryResponse() {
    }

    IndexDeleteByQueryResponse(String index) {
        this.index = index;
    }

    /**
     * Instantiates an IndexDeleteByQueryResponse with given values for counters. Counters should not be negative.
     */
    public IndexDeleteByQueryResponse(String index, long found, long deleted, long missing, long failed) {
        this(index);
        incrementFound(found);
        incrementDeleted(deleted);
        incrementMissing(missing);
        incrementFailed(failed);
    }

    public String getIndex() {
        return this.index;
    }

    public long getFound() {
        return found;
    }

    public void incrementFound() {
        incrementFound(1L);
    }

    public void incrementFound(long delta) {
        assert (found + delta >= 0) : "counter 'found' cannot be negative";
        this.found = found + delta;
    }

    public long getDeleted() {
        return deleted;
    }

    public void incrementDeleted() {
        incrementDeleted(1L);
    }

    public void incrementDeleted(long delta) {
        assert (deleted + delta >= 0) : "counter 'deleted' cannot be negative";
        this.deleted = deleted + delta;
    }

    public long getMissing() {
        return missing;
    }

    public void incrementMissing() {
        incrementMissing(1L);
    }

    public void incrementMissing(long delta) {
        assert (missing + delta >= 0) : "counter 'missing' cannot be negative";
        this.missing = missing + delta;
    }

    public long getFailed() {
        return failed;
    }

    public void incrementFailed() {
        incrementFailed(1L);
    }

    public void incrementFailed(long delta) {
        assert (failed + delta >= 0) : "counter 'failed' cannot be negative";
        this.failed = failed + delta;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        index = in.readString();
        found = in.readVLong();
        deleted = in.readVLong();
        missing = in.readVLong();
        failed = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
        out.writeVLong(found);
        out.writeVLong(deleted);
        out.writeVLong(missing);
        out.writeVLong(failed);
    }

    static final class Fields {
        static final XContentBuilderString FOUND = new XContentBuilderString("found");
        static final XContentBuilderString DELETED = new XContentBuilderString("deleted");
        static final XContentBuilderString MISSING = new XContentBuilderString("missing");
        static final XContentBuilderString FAILED = new XContentBuilderString("failed");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(index);
        builder.field(Fields.FOUND, found);
        builder.field(Fields.DELETED, deleted);
        builder.field(Fields.MISSING, missing);
        builder.field(Fields.FAILED, failed);
        builder.endObject();
        return builder;
    }
}