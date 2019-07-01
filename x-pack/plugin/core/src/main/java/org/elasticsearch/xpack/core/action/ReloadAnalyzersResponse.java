/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.action.TransportReloadAnalyzersAction.ReloadResult;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * The response object that will be returned when reloading analyzers
 */
public class ReloadAnalyzersResponse extends BroadcastResponse  {

    private final Map<String, IndexDetails> reloadedIndicesDetails;

    public ReloadAnalyzersResponse() {
        reloadedIndicesDetails = Collections.emptyMap();
    }

    public ReloadAnalyzersResponse(int totalShards, int successfulShards, int failedShards,
            List<DefaultShardOperationFailedException> shardFailures, Map<String, IndexDetails> reloadedIndicesNodes) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.reloadedIndicesDetails = reloadedIndicesNodes;
    }

    public final Map<String, IndexDetails> getReloadedIndicesDetails() {
        return this.reloadedIndicesDetails;
    }

    /**
     * Override in subclass to add custom fields following the common `_shards` field
     */
    @Override
    protected void addCustomXContentFields(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("reloaded_nodes");
        for (Entry<String, IndexDetails> indexDetails : reloadedIndicesDetails.entrySet()) {
            builder.startObject();
            IndexDetails value = indexDetails.getValue();
            builder.field("index", value.getIndexName());
            builder.field("reloaded_analyzers", value.getReloadedAnalyzers());
            builder.field("reloaded_node_ids", value.getReloadedIndicesNodes());
            builder.endObject();
        }
        builder.endArray();
    }

    @SuppressWarnings({ "unchecked" })
    private static final ConstructingObjectParser<ReloadAnalyzersResponse, Void> PARSER = new ConstructingObjectParser<>("reload_analyzer",
            true, arg -> {
                BroadcastResponse response = (BroadcastResponse) arg[0];
                List<IndexDetails> results = (List<IndexDetails>) arg[1];
                Map<String, IndexDetails> reloadedNodeIds = new HashMap<>();
                for (IndexDetails result : results) {
                    reloadedNodeIds.put(result.getIndexName(), result);
                }
                return new ReloadAnalyzersResponse(response.getTotalShards(), response.getSuccessfulShards(), response.getFailedShards(),
                        Arrays.asList(response.getShardFailures()), reloadedNodeIds);
            });

    @SuppressWarnings({ "unchecked" })
    private static final ConstructingObjectParser<IndexDetails, Void> ENTRY_PARSER = new ConstructingObjectParser<>(
            "reload_analyzer.entry", true, arg -> {
                return new IndexDetails((String) arg[0], new HashSet<>((List<String>) arg[1]), new HashSet<>((List<String>) arg[2]));
            });

    static {
        declareBroadcastFields(PARSER);
        PARSER.declareObjectArray(constructorArg(), ENTRY_PARSER, new ParseField("reloaded_nodes"));
        ENTRY_PARSER.declareString(constructorArg(), new ParseField("index"));
        ENTRY_PARSER.declareStringArray(constructorArg(), new ParseField("reloaded_analyzers"));
        ENTRY_PARSER.declareStringArray(constructorArg(), new ParseField("reloaded_node_ids"));
    }

    public static ReloadAnalyzersResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public static class IndexDetails {

        private final String indexName;
        private final Set<String> reloadedIndicesNodes;
        private final Set<String> reloadedAnalyzers;

        IndexDetails(String name, Set<String> reloadedIndicesNodes, Set<String> reloadedAnalyzers) {
            this.indexName = name;
            this.reloadedIndicesNodes = reloadedIndicesNodes;
            this.reloadedAnalyzers = reloadedAnalyzers;
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
            assert this.indexName == other.index;
            this.reloadedAnalyzers.addAll(other.reloadedSearchAnalyzers);
            this.reloadedIndicesNodes.add(other.nodeId);
        }
    }
}
