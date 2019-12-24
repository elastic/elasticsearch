/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.indices;

import org.elasticsearch.client.core.BroadcastResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * The response object that will be returned when reloading analyzers
 */
public class ReloadAnalyzersResponse extends BroadcastResponse {

    private final Map<String, ReloadDetails> reloadDetails;

    ReloadAnalyzersResponse(final Shards shards, Map<String, ReloadDetails> reloadDetails) {
        super(shards);
        this.reloadDetails = reloadDetails;
    }

    @SuppressWarnings({ "unchecked" })
    private static final ConstructingObjectParser<ReloadAnalyzersResponse, Void> PARSER = new ConstructingObjectParser<>("reload_analyzer",
            true, arg -> {
                Shards shards = (Shards) arg[0];
                List<Tuple<String, ReloadDetails>> results = (List<Tuple<String, ReloadDetails>>) arg[1];
                Map<String, ReloadDetails> reloadDetails = new HashMap<>();
                for (Tuple<String, ReloadDetails> result : results) {
                    reloadDetails.put(result.v1(), result.v2());
                }
                return new ReloadAnalyzersResponse(shards, reloadDetails);
            });

    @SuppressWarnings({ "unchecked" })
    private static final ConstructingObjectParser<Tuple<String, ReloadDetails>, Void> ENTRY_PARSER = new ConstructingObjectParser<>(
            "reload_analyzer.entry", true, arg -> {
                String index = (String) arg[0];
                Set<String> nodeIds = new HashSet<>((List<String>) arg[1]);
                Set<String> analyzers = new HashSet<>((List<String>) arg[2]);
                return new Tuple<>(index, new ReloadDetails(index, nodeIds, analyzers));
            });

    static {
        declareShardsField(PARSER);
        PARSER.declareObjectArray(constructorArg(), ENTRY_PARSER, new ParseField("reload_details"));
        ENTRY_PARSER.declareString(constructorArg(), new ParseField("index"));
        ENTRY_PARSER.declareStringArray(constructorArg(), new ParseField("reloaded_node_ids"));
        ENTRY_PARSER.declareStringArray(constructorArg(), new ParseField("reloaded_analyzers"));
    }

    public static ReloadAnalyzersResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public Map<String, ReloadDetails> getReloadedDetails() {
        return reloadDetails;
    }

    public static class ReloadDetails {

        private final String indexName;
        private final Set<String> reloadedIndicesNodes;
        private final Set<String> reloadedAnalyzers;

        public ReloadDetails(String name, Set<String> reloadedIndicesNodes, Set<String> reloadedAnalyzers) {
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
    }
}
