/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.plugin;

import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;

/**
 * Request to gather usage statistics
 */
public class EqlStatsRequest extends BaseNodesRequest<EqlStatsRequest> {

    private boolean includeStats;

    public EqlStatsRequest() {
        super((String[]) null);
    }

    public boolean includeStats() {
        return includeStats;
    }

    public void includeStats(boolean includeStats) {
        this.includeStats = includeStats;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        TransportAction.localOnly();
    }

    @Override
    public String toString() {
        return "eql_stats";
    }

    static class NodeStatsRequest extends TransportRequest {
        boolean includeStats;

        NodeStatsRequest(StreamInput in) throws IOException {
            super(in);
            includeStats = in.readBoolean();
        }

        NodeStatsRequest(EqlStatsRequest request) {
            includeStats = request.includeStats();
        }

        public boolean includeStats() {
            return includeStats;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(includeStats);
        }
    }
}
