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

import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class ReindexResponse extends BulkIndexByScrollResponse {
    static final String CREATED_FIELD = "created";

    private long created;

    public ReindexResponse() {
    }

    public ReindexResponse(long took, long created, long updated, int batches, long versionConflicts, long noops, List<Failure> indexingFailures, List<ShardSearchFailure> searchFailures) {
        super(took, updated, batches, versionConflicts, noops, indexingFailures, searchFailures);
        this.created = created;
    }

    public long getCreated() {
        return created;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(created);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        created = in.readVLong();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        super.toXContent(builder, params);
        builder.field(CREATED_FIELD, created);
        return builder;
    }

    @Override
    protected String toStringName() {
        return "ReindexResponse";
    }

    @Override
    protected void innerToString(StringBuilder builder) {
        builder.append(",created=").append(created);
    }

    /**
     * Get the first few failures to build a useful for toString.
     */
    protected void truncatedFailures(StringBuilder builder) {
        builder.append(",failures=[");
        Iterator<Failure> failures = getIndexingFailures().iterator();
        int written = 0;
        while (failures.hasNext() && written < 3) {
            Failure failure = failures.next();
            builder.append(failure.getMessage());
            if (written != 0) {
                builder.append(", ");
            }
            written++;
        }
        builder.append(']');
    }
}
