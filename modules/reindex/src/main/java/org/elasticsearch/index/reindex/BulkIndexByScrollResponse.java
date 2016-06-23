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

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.min;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static org.elasticsearch.action.search.ShardSearchFailure.readShardSearchFailure;

/**
 * Response used for actions that index many documents using a scroll request.
 */
public class BulkIndexByScrollResponse extends ActionResponse implements ToXContent {
    private TimeValue took;
    private BulkByScrollTask.Status status;
    private List<Failure> indexingFailures;
    private List<ShardSearchFailure> searchFailures;
    private boolean timedOut;

    public BulkIndexByScrollResponse() {
    }

    public BulkIndexByScrollResponse(TimeValue took, BulkByScrollTask.Status status, List<Failure> indexingFailures,
                                     List<ShardSearchFailure> searchFailures, boolean timedOut) {
        this.took = took;
        this.status = requireNonNull(status, "Null status not supported");
        this.indexingFailures = indexingFailures;
        this.searchFailures = searchFailures;
        this.timedOut = timedOut;
    }

    public TimeValue getTook() {
        return took;
    }

    protected BulkByScrollTask.Status getStatus() {
        return status;
    }

    public long getCreated() {
        return status.getCreated();
    }

    public long getDeleted() {
        return status.getDeleted();
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

    /**
     * The reason that the request was canceled or null if it hasn't been.
     */
    public String getReasonCancelled() {
        return status.getReasonCancelled();
    }

    /**
     * The number of times that the request had retry bulk actions.
     */
    public long getBulkRetries() {
        return status.getBulkRetries();
    }

    /**
     * The number of times that the request had retry search actions.
     */
    public long getSearchRetries() {
        return status.getSearchRetries();
    }

    /**
     * All of the indexing failures. Version conflicts are only included if the request sets abortOnVersionConflict to true (the
     * default).
     */
    public List<Failure> getIndexingFailures() {
        return indexingFailures;
    }

    /**
     * All search failures.
     */
    public List<ShardSearchFailure> getSearchFailures() {
        return searchFailures;
    }

    /**
     * Did any of the sub-requests that were part of this request timeout?
     */
    public boolean isTimedOut() {
        return timedOut;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        took.writeTo(out);
        status.writeTo(out);
        out.writeVInt(indexingFailures.size());
        for (Failure failure: indexingFailures) {
            failure.writeTo(out);
        }
        out.writeVInt(searchFailures.size());
        for (ShardSearchFailure failure: searchFailures) {
            failure.writeTo(out);
        }
        out.writeBoolean(timedOut);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        took = new TimeValue(in);
        status = new BulkByScrollTask.Status(in);
        int indexingFailuresCount = in.readVInt();
        List<Failure> indexingFailures = new ArrayList<>(indexingFailuresCount);
        for (int i = 0; i < indexingFailuresCount; i++) {
            indexingFailures.add(new Failure(in));
        }
        this.indexingFailures = unmodifiableList(indexingFailures);
        int searchFailuresCount = in.readVInt();
        List<ShardSearchFailure> searchFailures = new ArrayList<>(searchFailuresCount);
        for (int i = 0; i < searchFailuresCount; i++) {
            searchFailures.add(readShardSearchFailure(in));
        }
        this.searchFailures = unmodifiableList(searchFailures);
        this.timedOut = in.readBoolean();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("took", took.millis());
        builder.field("timed_out", timedOut);
        status.innerXContent(builder, params);
        builder.startArray("failures");
        for (Failure failure: indexingFailures) {
            builder.startObject();
            failure.toXContent(builder, params);
            builder.endObject();
        }
        for (ShardSearchFailure failure: searchFailures) {
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
        builder.append("took=").append(took).append(',');
        builder.append("timed_out=").append(timedOut).append(',');
        status.innerToString(builder);
        builder.append(",indexing_failures=").append(getIndexingFailures().subList(0, min(3, getIndexingFailures().size())));
        builder.append(",search_failures=").append(getSearchFailures().subList(0, min(3, getSearchFailures().size())));
        return builder.append(']').toString();
    }
}