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

package org.elasticsearch.plugin.reindex;

import static java.lang.Math.min;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.plugin.reindex.BulkIndexByScrollResponse.Fields.BATCHES;
import static org.elasticsearch.plugin.reindex.BulkIndexByScrollResponse.Fields.FAILURES;
import static org.elasticsearch.plugin.reindex.BulkIndexByScrollResponse.Fields.NOOPS;
import static org.elasticsearch.plugin.reindex.BulkIndexByScrollResponse.Fields.TOOK;
import static org.elasticsearch.plugin.reindex.BulkIndexByScrollResponse.Fields.UPDATED;
import static org.elasticsearch.plugin.reindex.BulkIndexByScrollResponse.Fields.VERSION_CONFLICTS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

/**
 * Response used for actions that index many documents using a scroll request.
 */
public class BulkIndexByScrollResponse extends ActionResponse implements ToXContent {
    private long took;
    private long updated;
    private int batches;
    private long versionConflicts;
    private long noops;
    private List<Failure> failures;

    public BulkIndexByScrollResponse() {
    }

    public BulkIndexByScrollResponse(long took, long updated, int batches, long versionConflicts, long noops, List<Failure> failures) {
        this.took = took;
        this.updated = updated;
        this.batches = batches;
        this.versionConflicts = versionConflicts;
        this.noops = noops;
        this.failures = unmodifiableList(failures);
    }

    public long getTook() {
        return took;
    }

    public long getUpdated() {
        return updated;
    }

    public int getBatches() {
        return batches;
    }

    public long getVersionConflicts() {
        return versionConflicts;
    }

    public long getNoops() {
        return noops;
    }

    /**
     * All recorded failures.
     */
    public List<Failure> getFailures() {
        return failures;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(took);
        out.writeVLong(updated);
        out.writeVInt(batches);
        out.writeVLong(versionConflicts);
        out.writeVLong(noops);
        out.writeVInt(failures.size());
        for (Failure failure: failures) {
            failure.writeTo(out);
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        took = in.readVLong();
        updated = in.readVLong();
        batches = in.readVInt();
        versionConflicts = in.readVLong();
        noops = in.readVLong();
        int failureCount = in.readVInt();
        List<Failure> failures = new ArrayList<>(failureCount);
        for (int i = 0; i < failureCount; i++) {
            Failure failure = new Failure();
            failure.readFrom(in);
            failures.add(failure);
        }
        this.failures = unmodifiableList(failures);
    }

    static final class Fields {
        static final XContentBuilderString TOOK = new XContentBuilderString("took");
        static final XContentBuilderString UPDATED = new XContentBuilderString("updated");
        static final XContentBuilderString BATCHES = new XContentBuilderString("batches");
        static final XContentBuilderString VERSION_CONFLICTS = new XContentBuilderString("versionConflicts");
        static final XContentBuilderString NOOPS = new XContentBuilderString("noops");
        static final XContentBuilderString FAILURES = new XContentBuilderString("failures");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(TOOK, took);
        builder.field(UPDATED, updated);
        builder.field(BATCHES, batches);
        builder.field(VERSION_CONFLICTS, versionConflicts);
        builder.field(NOOPS, noops);
        builder.startArray(FAILURES);
        for (Failure failure: failures) {
            builder.startObject();
            failure.toXContent(builder, params);
            builder.endObject();
        }
        builder.endArray();
        return builder;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("BulkIndexByScrollResponse[");
        builder.append("took=").append(took);
        builder.append(",updated=").append(updated);
        builder.append(",batches=").append(batches);
        builder.append(",versionConflicts=").append(versionConflicts);
        builder.append(",noops=").append(noops);
        builder.append(",failures=").append(getFailures().subList(0, min(3, getFailures().size())));
        return builder.append("]").toString();
    }
}