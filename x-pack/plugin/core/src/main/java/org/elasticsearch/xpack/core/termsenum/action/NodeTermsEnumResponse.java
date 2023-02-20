/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.termsenum.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Internal response of a terms enum request executed directly against a specific shard.
 *
 *
 */
class NodeTermsEnumResponse extends TransportResponse {

    private String error;
    private boolean complete;

    private List<String> terms;
    private String nodeId;

    NodeTermsEnumResponse(StreamInput in) throws IOException {
        super(in);
        if (in.getTransportVersion().before(TransportVersion.V_8_2_0)) {
            terms = in.readList(r -> {
                String term = r.readString();
                in.readLong(); // obsolete docCount field
                return term;
            });
        } else {
            terms = in.readStringList();
        }
        error = in.readOptionalString();
        complete = in.readBoolean();
        nodeId = in.readString();
    }

    NodeTermsEnumResponse(String nodeId, List<String> terms, String error, boolean complete) {
        this.nodeId = nodeId;
        this.terms = terms;
        this.error = error;
        this.complete = complete;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().before(TransportVersion.V_8_2_0)) {
            out.writeCollection(terms.stream().map(term -> (Writeable) out1 -> {
                out1.writeString(term);
                out1.writeLong(1); // obsolete docCount field
            }).collect(Collectors.toList()));
        } else {
            out.writeStringCollection(terms);
        }
        out.writeOptionalString(error);
        out.writeBoolean(complete);
        out.writeString(nodeId);
    }

    public List<String> terms() {
        return this.terms;
    }

    public String getError() {
        return error;
    }

    public String getNodeId() {
        return nodeId;
    }

    public boolean isComplete() {
        return complete;
    }
}
