/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.action.TransportReloadAnalyzersAction.ReloadResult;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * The response object that will be returned when reloading analyzers
 */
public class ReloadAnalyzersResponse extends BroadcastResponse {

    private final Map<String, ReloadDetails> reloadDetails;

    private static final ParseField RELOAD_DETAILS_FIELD = new ParseField("reload_details");
    private static final ParseField INDEX_FIELD = new ParseField("index");
    private static final ParseField RELOADED_ANALYZERS_FIELD = new ParseField("reloaded_analyzers");
    private static final ParseField RELOADED_NODE_IDS_FIELD = new ParseField("reloaded_node_ids");

    public ReloadAnalyzersResponse(StreamInput in) throws IOException {
        super(in);
        this.reloadDetails = in.readMap(StreamInput::readString, ReloadDetails::new);
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

    @SuppressWarnings({ "unchecked" })
    private static final ConstructingObjectParser<ReloadAnalyzersResponse, Void> PARSER = new ConstructingObjectParser<>(
        "reload_analyzer",
        true,
        arg -> {
            BroadcastResponse response = (BroadcastResponse) arg[0];
            List<ReloadDetails> results = (List<ReloadDetails>) arg[1];
            Map<String, ReloadDetails> reloadedNodeIds = new HashMap<>();
            for (ReloadDetails result : results) {
                reloadedNodeIds.put(result.getIndexName(), result);
            }
            return new ReloadAnalyzersResponse(
                response.getTotalShards(),
                response.getSuccessfulShards(),
                response.getFailedShards(),
                Arrays.asList(response.getShardFailures()),
                reloadedNodeIds
            );
        }
    );

    @SuppressWarnings({ "unchecked" })
    private static final ConstructingObjectParser<ReloadDetails, Void> ENTRY_PARSER = new ConstructingObjectParser<>(
        "reload_analyzer.entry",
        true,
        arg -> { return new ReloadDetails((String) arg[0], new HashSet<>((List<String>) arg[1]), new HashSet<>((List<String>) arg[2])); }
    );

    static {
        declareBroadcastFields(PARSER);
        PARSER.declareObjectArray(constructorArg(), ENTRY_PARSER, RELOAD_DETAILS_FIELD);
        ENTRY_PARSER.declareString(constructorArg(), INDEX_FIELD);
        ENTRY_PARSER.declareStringArray(constructorArg(), RELOADED_ANALYZERS_FIELD);
        ENTRY_PARSER.declareStringArray(constructorArg(), RELOADED_NODE_IDS_FIELD);
    }

    public static ReloadAnalyzersResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeMap(reloadDetails, StreamOutput::writeString, (stream, details) -> details.writeTo(stream));
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
            this.reloadedIndicesNodes = new HashSet<>(in.readList(StreamInput::readString));
            this.reloadedAnalyzers = new HashSet<>(in.readList(StreamInput::readString));
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

        void merge(ReloadResult other) {
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
