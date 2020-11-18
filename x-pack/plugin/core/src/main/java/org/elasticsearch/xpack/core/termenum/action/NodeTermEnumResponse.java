/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.termenum.action;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;
import java.util.List;

/**
 * Internal response of a term enum request executed directly against a specific shard.
 *
 *
 */
class NodeTermEnumResponse extends TransportResponse {

    private String error;
    private boolean complete;

    private List<TermCount> terms;
    private String nodeId;

    NodeTermEnumResponse(StreamInput in) throws IOException {
        super(in);
        terms = in.readList(TermCount::new);
        error = in.readOptionalString();
        complete = in.readBoolean();
        nodeId = in.readString();
    }

    NodeTermEnumResponse(String nodeId, List<TermCount> terms, String error, boolean complete) {
        this.nodeId = nodeId;
        this.terms = terms;
        this.error = error;
        this.complete = complete;
    }

    public List<TermCount> terms() {
        return this.terms;
    }

    public String getError() {
        return error;
    }

    public String getNodeId() {
        return nodeId;
    }
        
    public boolean getComplete() {
        return complete;
    }


    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(terms);
        out.writeOptionalString(error);
        out.writeBoolean(complete);
        out.writeString(nodeId);
    }
}
