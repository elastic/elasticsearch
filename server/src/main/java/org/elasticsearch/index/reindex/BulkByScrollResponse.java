/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static org.elasticsearch.core.TimeValue.timeValueNanos;

/**
 * Response used for actions that index many documents using a scroll request.
 */
public class BulkByScrollResponse extends ActionResponse implements ToXContentFragment {
    private final TimeValue took;
    private final BulkByScrollTask.Status status;
    private final List<Failure> bulkFailures;
    private final List<ScrollableHitSource.SearchFailure> searchFailures;
    private boolean timedOut;

    static final String TOOK_FIELD = "took";
    static final String TIMED_OUT_FIELD = "timed_out";
    static final String FAILURES_FIELD = "failures";

    public BulkByScrollResponse(StreamInput in) throws IOException {
        took = in.readTimeValue();
        status = new BulkByScrollTask.Status(in);
        bulkFailures = in.readCollectionAsList(Failure::new);
        searchFailures = in.readCollectionAsList(ScrollableHitSource.SearchFailure::new);
        timedOut = in.readBoolean();
    }

    public BulkByScrollResponse(
        TimeValue took,
        BulkByScrollTask.Status status,
        List<Failure> bulkFailures,
        List<ScrollableHitSource.SearchFailure> searchFailures,
        boolean timedOut
    ) {
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

    public long getTotal() {
        return status.getTotal();
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
        out.writeTimeValue(took);
        status.writeTo(out);
        out.writeCollection(bulkFailures);
        out.writeCollection(searchFailures);
        out.writeBoolean(timedOut);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(TOOK_FIELD, took.millis());
        builder.field(TIMED_OUT_FIELD, timedOut);
        status.innerXContent(builder, params);
        builder.startArray(FAILURES_FIELD);
        for (Failure failure : bulkFailures) {
            builder.startObject();
            failure.toXContent(builder, params);
            builder.endObject();
        }
        for (ScrollableHitSource.SearchFailure failure : searchFailures) {
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
