/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.profile.aggregation.AggregationProfileShardResult;
import org.elasticsearch.search.profile.query.QueryProfileShardResult;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Profile results for all shards.
 */
public final class SearchProfileResults implements Writeable, ToXContentFragment {

    private static final Logger logger = LogManager.getLogger(SearchProfileResults.class);
    private static final String ID_FIELD = "id";
    private static final String NODE_ID_FIELD = "node_id";
    private static final String CLUSTER_FIELD = "cluster";
    private static final String INDEX_NAME_FIELD = "index";
    private static final String SHARD_ID_FIELD = "shard_id";
    private static final String SHARDS_FIELD = "shards";
    public static final String PROFILE_FIELD = "profile";

    /// MP: key to this map is [nodeId] [remote-index-name] [shardId] created from the SearchShardTarget.toString method
    private Map<String, SearchProfileShardResult> shardResults;

    public SearchProfileResults(Map<String, SearchProfileShardResult> shardResults) {
        this.shardResults = Collections.unmodifiableMap(shardResults);
    }

    public SearchProfileResults(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_7_16_0)) {
            shardResults = in.readMap(SearchProfileShardResult::new);
        } else {
            // Before 8.0.0 we only send the query phase result
            shardResults = in.readMap(i -> new SearchProfileShardResult(new SearchProfileQueryPhaseResult(i), null));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_7_16_0)) {
            out.writeMap(shardResults, StreamOutput::writeString, (o, r) -> r.writeTo(o));
        } else {
            // Before 8.0.0 we only send the query phase
            out.writeMap(shardResults, StreamOutput::writeString, (o, r) -> r.getQueryPhase().writeTo(o));
        }
    }

    public Map<String, SearchProfileShardResult> getShardResults() {
        return shardResults;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(PROFILE_FIELD).startArray(SHARDS_FIELD);
        // shardResults is a map, but we print entries in a json array, which is ordered.
        // we sort the keys of the map, so that toXContent always prints out the same array order
        TreeSet<String> sortedKeys = new TreeSet<>(shardResults.keySet());
        for (String key : sortedKeys) {
            builder.startObject();
            builder.field(ID_FIELD, key);

            ShardProfileId shardProfileId = parseProfileShardId(key);
            if (shardProfileId != null) {
                builder.field(NODE_ID_FIELD, shardProfileId.nodeId());
                builder.field(SHARD_ID_FIELD, shardProfileId.shardId());
                builder.field(INDEX_NAME_FIELD, shardProfileId.indexName());
                String cluster = shardProfileId.clusterName();
                if (cluster == null) {
                    cluster = "(local)";
                }
                builder.field(CLUSTER_FIELD, cluster);
            }

            SearchProfileShardResult shardResult = shardResults.get(key);
            shardResult.toXContent(builder, params);
            builder.endObject();
        }

        builder.endArray().endObject();
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SearchProfileResults other = (SearchProfileResults) obj;
        return shardResults.equals(other.shardResults);
    }

    @Override
    public int hashCode() {
        return shardResults.hashCode();
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public static SearchProfileResults fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
        Map<String, SearchProfileShardResult> profileResults = new HashMap<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.START_ARRAY) {
                if (SHARDS_FIELD.equals(parser.currentName())) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        parseProfileResultsEntry(parser, profileResults);
                    }
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                parser.skipChildren();
            }
        }
        return new SearchProfileResults(profileResults);
    }

    /// MP: what is the key to this map?
    private static void parseProfileResultsEntry(XContentParser parser, Map<String, SearchProfileShardResult> searchProfileResults)
        throws IOException {
        XContentParser.Token token = parser.currentToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
        SearchProfileDfsPhaseResult searchProfileDfsPhaseResult = null;
        List<QueryProfileShardResult> queryProfileResults = new ArrayList<>();
        AggregationProfileShardResult aggProfileShardResult = null;
        ProfileResult fetchResult = null;
        String id = null;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (ID_FIELD.equals(currentFieldName)) {
                    id = parser.text();
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("searches".equals(currentFieldName)) {
                    while ((parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        queryProfileResults.add(QueryProfileShardResult.fromXContent(parser));
                    }
                } else if (AggregationProfileShardResult.AGGREGATIONS.equals(currentFieldName)) {
                    aggProfileShardResult = AggregationProfileShardResult.fromXContent(parser);
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("dfs".equals(currentFieldName)) {
                    searchProfileDfsPhaseResult = SearchProfileDfsPhaseResult.fromXContent(parser);
                } else if ("fetch".equals(currentFieldName)) {
                    fetchResult = ProfileResult.fromXContent(parser);
                } else {
                    parser.skipChildren();
                }
            } else {
                parser.skipChildren();
            }
        }
        SearchProfileShardResult result = new SearchProfileShardResult(
            new SearchProfileQueryPhaseResult(queryProfileResults, aggProfileShardResult),
            fetchResult
        );
        result.getQueryPhase().setSearchProfileDfsPhaseResult(searchProfileDfsPhaseResult);
        searchProfileResults.put(id, result);
    }

    /**
     * Parsed representation of a composite id used for shards in a profile.
     * The composite id format is specified/created via the {@code SearchShardTargetÏ} method.
     * @param nodeId nodeId that the shard is on
     * @param indexName index profiled
     * @param shardId integer shard id
     * @param clusterName if a CCS search, the remote clusters will have a name in the id. Local clusters will be null.
     */
    record ShardProfileId(String nodeId, String indexName, int shardId, @Nullable String clusterName) {}

    /**
     * Parse the composite "shard id" from the profiles output, which comes from the
     * {@code SearchShardTarget.toString()} method, into its separate components.
     * <p>
     * One of two expected patterns is accepted:
     * <p>
     * 1) [nodeId][indexName][shardId]
     * example: [2m7SW9oIRrirdrwirM1mwQ][blogs][1]
     * <p>
     * 2) [nodeId][clusterName:indexName][shardId]
     * example: [UngEVXTBQL-7w5j_tftGAQ][remote1:blogs][0]
     *
     * @param compositeId see above for accepted formats
     * @return ShardProfileId with parsed components or null if the compositeId has an unsupported format
     */
    static ShardProfileId parseProfileShardId(String compositeId) {
        if (Strings.isNullOrEmpty(compositeId)) {
            return null;
        }
        Pattern r = Pattern.compile("\\[([^]]+)\\]\\[([^]]+)\\]\\[(\\d+)\\]");
        Matcher m = r.matcher(compositeId);
        if (m.find()) {
            String nodeId = m.group(1);
            String indexName = m.group(2);
            int shardId = Integer.parseInt(m.group(3));
            String cluster = null;
            if (indexName.contains(":")) {
                // index names and cluster names cannot contain a ':', so this split should be accurate
                String[] tokens = indexName.split(":", 2);
                cluster = tokens[0];
                indexName = tokens[1];
            }
            return new ShardProfileId(nodeId, indexName, shardId, cluster);
        } else {
            logger.warn("Unable to match input against expected pattern of [nodeId][indexName][shardId]. Input: {}", compositeId);
            return null;
        }
    }
}
