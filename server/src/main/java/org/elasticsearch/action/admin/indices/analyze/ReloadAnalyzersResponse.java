/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.analyze;

import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

/**
 * The response object that will be returned when reloading analyzers
 */
public class ReloadAnalyzersResponse extends BroadcastResponse {

    private final Map<String, ReloadDetails> reloadDetails;

    static final ParseField RELOAD_DETAILS_FIELD = new ParseField("reload_details");
    static final ParseField INDEX_FIELD = new ParseField("index");
    static final ParseField RELOADED_ANALYZERS_FIELD = new ParseField("reloaded_analyzers");
    static final ParseField RELOADED_NODE_IDS_FIELD = new ParseField("reloaded_node_ids");

    public ReloadAnalyzersResponse(StreamInput in) throws IOException {
        super(in);
        this.reloadDetails = in.readMap(ReloadDetails::new);
    }

    public ReloadAnalyzersResponse(
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> shardFailures,
        Map<String, ReloadDetails> reloadedIndicesNodes
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.reloadDetails = reloadedIndicesNodes;
    }

    public final Map<String, ReloadDetails> getReloadDetails() {
        return this.reloadDetails;
    }

    /**
     * Override in subclass to add custom fields following the common `_shards` field
     */
    @Override
    protected void addCustomXContentFields(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(RELOAD_DETAILS_FIELD.getPreferredName());
        for (Entry<String, ReloadDetails> indexDetails : reloadDetails.entrySet()) {
            builder.startObject();
            ReloadDetails value = indexDetails.getValue();
            builder.field(INDEX_FIELD.getPreferredName(), value.getIndexName());
            builder.stringListField(RELOADED_ANALYZERS_FIELD.getPreferredName(), value.getReloadedAnalyzers());
            builder.stringListField(RELOADED_NODE_IDS_FIELD.getPreferredName(), value.getReloadedIndicesNodes());
            builder.endObject();
        }
        builder.endArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeMap(reloadDetails, StreamOutput::writeWriteable);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ReloadAnalyzersResponse that = (ReloadAnalyzersResponse) o;
        return Objects.equals(reloadDetails, that.reloadDetails);
    }

    @Override
    public int hashCode() {
        return Objects.hash(reloadDetails);
    }

    public static class ReloadDetails implements Writeable {

        private final String indexName;
        private final Set<String> reloadedIndicesNodes;
        private final Set<String> reloadedAnalyzers;

        public ReloadDetails(String name, Set<String> reloadedIndicesNodes, Set<String> reloadedAnalyzers) {
            this.indexName = name;
            this.reloadedIndicesNodes = reloadedIndicesNodes;
            this.reloadedAnalyzers = reloadedAnalyzers;
        }

        ReloadDetails(StreamInput in) throws IOException {
            this.indexName = in.readString();
            this.reloadedIndicesNodes = new HashSet<>(in.readCollectionAsList(StreamInput::readString));
            this.reloadedAnalyzers = new HashSet<>(in.readCollectionAsList(StreamInput::readString));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(indexName);
            out.writeStringCollection(reloadedIndicesNodes);
            out.writeStringCollection(reloadedAnalyzers);
        }

        public String getIndexName() {
            return indexName;
        }

        public Set<String> getReloadedIndicesNodes() {
            return reloadedIndicesNodes;
        }

        public Set<String> getReloadedAnalyzers() {
            return reloadedAnalyzers;
        }

        void merge(TransportReloadAnalyzersAction.ReloadResult other) {
            assert this.indexName.equals(other.index);
            this.reloadedAnalyzers.addAll(other.reloadedSearchAnalyzers);
            this.reloadedIndicesNodes.add(other.nodeId);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ReloadDetails that = (ReloadDetails) o;
            return Objects.equals(indexName, that.indexName)
                && Objects.equals(reloadedIndicesNodes, that.reloadedIndicesNodes)
                && Objects.equals(reloadedAnalyzers, that.reloadedAnalyzers);
        }

        @Override
        public int hashCode() {
            return Objects.hash(indexName, reloadedIndicesNodes, reloadedAnalyzers);
        }
    }
}
