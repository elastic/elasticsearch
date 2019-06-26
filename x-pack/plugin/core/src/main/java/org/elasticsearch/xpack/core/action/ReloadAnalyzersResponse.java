/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * The response object that will be returned when reloading analyzers
 */
public class ReloadAnalyzersResponse extends BroadcastResponse  {

    private final Map<String, List<String>> reloadedIndicesNodes;

    public ReloadAnalyzersResponse() {
        reloadedIndicesNodes = Collections.emptyMap();
    }

    public ReloadAnalyzersResponse(int totalShards, int successfulShards, int failedShards,
            List<DefaultShardOperationFailedException> shardFailures, Map<String, List<String>> reloadedIndicesNodes) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.reloadedIndicesNodes = reloadedIndicesNodes;
    }

    public Map<String, List<String>> getReloadedIndicesNodes() {
        return reloadedIndicesNodes;
    }

    /**
     * Override in subclass to add custom fields following the common `_shards` field
     */
    @Override
    protected void addCustomXContentFields(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("reloaded_nodes");
        for (Entry<String, List<String>> indexNodesReloaded : reloadedIndicesNodes.entrySet()) {
            builder.startObject();
            builder.field("index", indexNodesReloaded.getKey());
            builder.field("reloaded_node_ids", indexNodesReloaded.getValue());
            builder.endObject();
        }
        builder.endArray();
    }

    @SuppressWarnings({ "unchecked" })
    private static final ConstructingObjectParser<ReloadAnalyzersResponse, Void> PARSER = new ConstructingObjectParser<>("reload_analyzer",
            true, arg -> {
                BroadcastResponse response = (BroadcastResponse) arg[0];
                List<Tuple<String, List<String>>> results = (List<Tuple<String, List<String>>>) arg[1];
                Map<String, List<String>> reloadedNodeIds = new HashMap<>();
                for (Tuple<String, List<String>> result : results) {
                    reloadedNodeIds.put(result.v1(), result.v2());
                }
                return new ReloadAnalyzersResponse(response.getTotalShards(), response.getSuccessfulShards(), response.getFailedShards(),
                        Arrays.asList(response.getShardFailures()), reloadedNodeIds);
            });

    @SuppressWarnings({ "unchecked" })
    private static final ConstructingObjectParser<Tuple<String, List<String>>, Void> ENTRY_PARSER = new ConstructingObjectParser<>(
            "reload_analyzer.entry", true, arg -> {
                String index = (String) arg[0];
                List<String> nodeIds = (List<String>) arg[1];
                return new Tuple<>(index, nodeIds);
            });

    static {
        declareBroadcastFields(PARSER);
        PARSER.declareObjectArray(constructorArg(), ENTRY_PARSER, new ParseField("reloaded_nodes"));
        ENTRY_PARSER.declareString(constructorArg(), new ParseField("index"));
        ENTRY_PARSER.declareStringArray(constructorArg(), new ParseField("reloaded_node_ids"));
    }

    public static ReloadAnalyzersResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }
}
