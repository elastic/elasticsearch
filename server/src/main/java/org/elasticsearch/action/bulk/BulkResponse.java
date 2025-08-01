/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;

/**
 * A response of a bulk execution. Holding a response for each item responding (in order) of the
 * bulk requests. Each item holds the index/type/id is operated on, and if it failed or not (with the
 * failure message).
 */
public class BulkResponse extends ActionResponse implements Iterable<BulkItemResponse>, ChunkedToXContentObject {

    static final String ITEMS = "items";
    static final String ERRORS = "errors";
    static final String TOOK = "took";
    static final String INGEST_TOOK = "ingest_took";

    public static final long NO_INGEST_TOOK = -1L;

    private final BulkItemResponse[] responses;
    private final long tookInMillis;
    private final long ingestTookInMillis;
    private final BulkRequest.IncrementalState incrementalState;

    public BulkResponse(StreamInput in) throws IOException {
        responses = in.readArray(BulkItemResponse::new, BulkItemResponse[]::new);
        tookInMillis = in.readVLong();
        ingestTookInMillis = in.readZLong();
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
            incrementalState = new BulkRequest.IncrementalState(in);
        } else {
            incrementalState = BulkRequest.IncrementalState.EMPTY;
        }
    }

    public BulkResponse(BulkItemResponse[] responses, long tookInMillis) {
        this(responses, tookInMillis, NO_INGEST_TOOK);
    }

    public BulkResponse(BulkItemResponse[] responses, long tookInMillis, long ingestTookInMillis) {
        this(responses, tookInMillis, ingestTookInMillis, BulkRequest.IncrementalState.EMPTY);
    }

    public BulkResponse(
        BulkItemResponse[] responses,
        long tookInMillis,
        long ingestTookInMillis,
        BulkRequest.IncrementalState incrementalState
    ) {
        this.responses = responses;
        this.tookInMillis = tookInMillis;
        this.ingestTookInMillis = ingestTookInMillis;
        this.incrementalState = incrementalState;
    }

    /**
     * How long the bulk execution took. Excluding ingest preprocessing.
     */
    public TimeValue getTook() {
        return new TimeValue(tookInMillis);
    }

    public long getTookInMillis() {
        return tookInMillis;
    }

    /**
     * If ingest is enabled returns the bulk ingest preprocessing time, otherwise 0 is returned.
     */
    public TimeValue getIngestTook() {
        return new TimeValue(ingestTookInMillis);
    }

    /**
     * If ingest is enabled returns the bulk ingest preprocessing time. in milliseconds, otherwise -1 is returned.
     */
    public long getIngestTookInMillis() {
        return ingestTookInMillis;
    }

    BulkRequest.IncrementalState getIncrementalState() {
        return incrementalState;
    }

    /**
     * Has anything failed with the execution.
     */
    public boolean hasFailures() {
        for (BulkItemResponse response : responses) {
            if (response.isFailed()) {
                return true;
            }
        }
        return false;
    }

    public String buildFailureMessage() {
        StringBuilder sb = new StringBuilder();
        sb.append("failure in bulk execution:");
        for (int i = 0; i < responses.length; i++) {
            BulkItemResponse response = responses[i];
            if (response.isFailed()) {
                sb.append("\n[")
                    .append(i)
                    .append("]: index [")
                    .append(response.getIndex())
                    .append("], id [")
                    .append(response.getId())
                    .append("], message [")
                    .append(response.getFailureMessage())
                    .append("]");
            }
        }
        return sb.toString();
    }

    /**
     * The items representing each action performed in the bulk operation (in the same order!).
     */
    public BulkItemResponse[] getItems() {
        return responses;
    }

    @Override
    public Iterator<BulkItemResponse> iterator() {
        return Iterators.forArray(responses);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeArray(responses);
        out.writeVLong(tookInMillis);
        out.writeZLong(ingestTookInMillis);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
            incrementalState.writeTo(out);
        }
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return Iterators.concat(Iterators.single((builder, p) -> {
            builder.startObject();
            builder.field(ERRORS, hasFailures());
            builder.field(TOOK, tookInMillis);
            if (ingestTookInMillis != BulkResponse.NO_INGEST_TOOK) {
                builder.field(INGEST_TOOK, ingestTookInMillis);
            }
            return builder.startArray(ITEMS);
        }), Iterators.forArray(responses), Iterators.<ToXContent>single((builder, p) -> builder.endArray().endObject()));
    }
}
