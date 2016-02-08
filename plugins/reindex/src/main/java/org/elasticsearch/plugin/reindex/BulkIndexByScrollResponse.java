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

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Response used for actions that index many documents using a scroll request.
 */
public class BulkIndexByScrollResponse extends ActionResponse implements ToXContent {
    private long took;
    private BulkByScrollTask.Status status;

    public BulkIndexByScrollResponse() {
    }

    public BulkIndexByScrollResponse(long took, BulkByScrollTask.Status status) {
        this.took = took;
        this.status = requireNonNull(status);
    }

    public long getTook() {
        return took;
    }

    protected BulkByScrollTask.Status getStatus() {
        return status;
    }

    public long getUpdated() {
        return status.getUpdated();
    }

    public int getBatches() {
        return status.getBatches();
    }

    public long getVersionConflicts() {
        return status.getVersionConflicts();
    }

    public long getNoops() {
        return status.getNoops();
    }

    public List<Failure> getIndexingFailures() {
        return status.getIndexingFailures();
    }

    public List<ShardSearchFailure> getSearchFailures() {
        return status.getSearchFailures();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(took);
        status.writeTo(out);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        took = in.readVLong();
        status = new BulkByScrollTask.Status(in);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("took", took);
        status.innerXContent(builder, params, false, false);
        return builder;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("BulkIndexByScrollResponse[");
        builder.append("took=").append(took).append(',');
        status.innerToString(builder, false, false);
        return builder.append(']').toString();
    }
}