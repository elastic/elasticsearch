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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static org.elasticsearch.common.unit.TimeValue.timeValueNanos;

/**
 * Response used for actions that index many documents using a scroll request.
 */
public class BulkByScrollResponse extends ActionResponse implements ToXContentFragment {
    private TimeValue took;
    private BulkByScrollTask.Status status;
    private List<Failure> bulkFailures;
    private List<ScrollableHitSource.SearchFailure> searchFailures;
    private boolean timedOut;

    public BulkByScrollResponse() {
    }

    public BulkByScrollResponse(TimeValue took, BulkByScrollTask.Status status, List<Failure> bulkFailures,
                                List<ScrollableHitSource.SearchFailure> searchFailures, boolean timedOut) {
        this.took = took;
        this.status = requireNonNull(status, "Null status not supported");
        this.bulkFailures = bulkFailures;
        this.searchFailures = searchFailures;
        this.timedOut = timedOut;
    }

    public BulkByScrollResponse(Iterable<BulkByScrollResponse> toMerge, @Nullable String reasonCancelled) {
        long mergedTook = 0;
        List<BulkByScrollTask.StatusOrException> statuses = new ArrayList<>();
        bulkFailures = new ArrayList<>();
        searchFailures = new ArrayList<>();
        for (BulkByScrollResponse response : toMerge) {
            mergedTook = max(mergedTook, response.getTook().nanos());
            statuses.add(new BulkByScrollTask.StatusOrException(response.status));
            bulkFailures.addAll(response.getBulkFailures());
            searchFailures.addAll(response.getSearchFailures());
            timedOut |= response.isTimedOut();
        }
        took = timeValueNanos(mergedTook);
        status = new BulkByScrollTask.Status(statuses, reasonCancelled);
    }

    public TimeValue getTook() {
        return took;
    }

    public BulkByScrollTask.Status getStatus() {
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
     * All of the bulk failures. Version conflicts are only included if the request sets abortOnVersionConflict to true (the default).
     */
    public List<Failure> getBulkFailures() {
        return bulkFailures;
    }

    /**
     * All search failures.
     */
    public List<ScrollableHitSource.SearchFailure> getSearchFailures() {
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
        out.writeList(bulkFailures);
        out.writeList(searchFailures);
        out.writeBoolean(timedOut);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        took = new TimeValue(in);
        status = new BulkByScrollTask.Status(in);
        bulkFailures = in.readList(Failure::new);
        searchFailures = in.readList(ScrollableHitSource.SearchFailure::new);
        timedOut = in.readBoolean();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("took", took.millis());
        builder.field("timed_out", timedOut);
        status.innerXContent(builder, params);
        builder.startArray("failures");
        for (Failure failure: bulkFailures) {
            builder.startObject();
            failure.toXContent(builder, params);
            builder.endObject();
        }
        for (ScrollableHitSource.SearchFailure failure: searchFailures) {
            failure.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(getClass().getSimpleName()).append("[");
        builder.append("took=").append(took).append(',');
        builder.append("timed_out=").append(timedOut).append(',');
        status.innerToString(builder);
        builder.append(",bulk_failures=").append(getBulkFailures().subList(0, min(3, getBulkFailures().size())));
        builder.append(",search_failures=").append(getSearchFailures().subList(0, min(3, getSearchFailures().size())));
        return builder.append(']').toString();
    }
}
