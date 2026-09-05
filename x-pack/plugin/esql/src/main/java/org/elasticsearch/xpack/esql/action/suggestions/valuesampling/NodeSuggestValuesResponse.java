/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action.suggestions.valuesampling;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;
import java.util.Map;

/**
 * Per-node result of a {@link NodeSuggestValuesRequest}: up to {@code size} first-encountered terms
 * per contributing shard on this node (see the suggestions API spec's ranking-choice note), each with
 * its raw {@code docFreq} count, plus this node's total live-doc count across the shards it actually
 * read from (the denominator for turning those counts into a fraction).
 *
 * <p>{@code complete} is {@code false} when this node's timeout budget ran out before finishing (a
 * partial result, still returned rather than failing) or a shard read failed (see {@link #error()}).
 * {@code dlsActive} is {@code true} when this node refused to read one or more requested shards
 * because the requesting user's DLS role query does not rewrite to {@code match_all}.
 */
public class NodeSuggestValuesResponse extends TransportResponse {

    private final String nodeId;
    private final Map<Object, Long> docFreqByTerm;
    private final long liveDocs;
    private final boolean complete;
    private final boolean dlsActive;
    private final String error;

    public NodeSuggestValuesResponse(
        String nodeId,
        Map<Object, Long> docFreqByTerm,
        long liveDocs,
        boolean complete,
        boolean dlsActive,
        String error
    ) {
        this.nodeId = nodeId;
        this.docFreqByTerm = docFreqByTerm;
        this.liveDocs = liveDocs;
        this.complete = complete;
        this.dlsActive = dlsActive;
        this.error = error;
    }

    public NodeSuggestValuesResponse(StreamInput in) throws IOException {
        this.nodeId = in.readString();
        this.docFreqByTerm = in.readMap(StreamInput::readGenericValue, StreamInput::readVLong);
        this.liveDocs = in.readVLong();
        this.complete = in.readBoolean();
        this.dlsActive = in.readBoolean();
        this.error = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(nodeId);
        out.writeMap(docFreqByTerm, StreamOutput::writeGenericValue, StreamOutput::writeVLong);
        out.writeVLong(liveDocs);
        out.writeBoolean(complete);
        out.writeBoolean(dlsActive);
        out.writeOptionalString(error);
    }

    public static NodeSuggestValuesResponse error(String nodeId, Exception e) {
        return new NodeSuggestValuesResponse(nodeId, Map.of(), 0, false, false, e.toString());
    }

    public String nodeId() {
        return nodeId;
    }

    public Map<Object, Long> docFreqByTerm() {
        return docFreqByTerm;
    }

    public long liveDocs() {
        return liveDocs;
    }

    public boolean complete() {
        return complete;
    }

    public boolean dlsActive() {
        return dlsActive;
    }

    public String error() {
        return error;
    }

    /** {@code true} if this node's result should count as a partial/skipped-shard signal ({@code shards_skipped}). */
    public boolean partialOrErrored() {
        return complete == false || error != null;
    }
}
