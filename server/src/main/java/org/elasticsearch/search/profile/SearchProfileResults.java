/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.profile;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Profile results for all shards.
 */
public final class SearchProfileResults implements Writeable, ToXContentFragment {

    private static final Logger logger = LogManager.getLogger(SearchProfileResults.class);
    public static final String ID_FIELD = "id";
    private static final String NODE_ID_FIELD = "node_id";
    private static final String CLUSTER_FIELD = "cluster";
    private static final String INDEX_NAME_FIELD = "index";
    private static final String SHARD_ID_FIELD = "shard_id";
    public static final String SHARDS_FIELD = "shards";
    public static final String PROFILE_FIELD = "profile";

    // map key is the composite "id" of form [nodeId][(clusterName:)indexName][shardId] created from SearchShardTarget.toString
    private final Map<String, SearchProfileShardResult> shardResults;

    public SearchProfileResults(Map<String, SearchProfileShardResult> shardResults) {
        this.shardResults = Collections.unmodifiableMap(shardResults);
    }

    public SearchProfileResults(StreamInput in) throws IOException {
        shardResults = in.readMap(SearchProfileShardResult::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(shardResults, StreamOutput::writeWriteable);
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

            ShardProfileId shardProfileId = parseCompositeProfileShardId(key);
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

    /**
     * Parsed representation of a composite id used for shards in a profile.
     * The composite id format is specified/created via the {@code SearchShardTarget} method.
     * @param nodeId nodeId that the shard is on
     * @param indexName index being profiled
     * @param shardId shard id being profiled
     * @param clusterName if a CCS search, the remote clusters will have a name in the id. Local clusters will be null.
     */
    record ShardProfileId(String nodeId, String indexName, int shardId, @Nullable String clusterName) {}

    private static final Pattern SHARD_ID_DECOMPOSITION = Pattern.compile("\\[([^]]+)\\]\\[([^]]+)\\]\\[(\\d+)\\]");

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
    static ShardProfileId parseCompositeProfileShardId(String compositeId) {
        assert Strings.isNullOrEmpty(compositeId) == false : "An empty id should not be passed to parseCompositeProfileShardId";

        Matcher m = SHARD_ID_DECOMPOSITION.matcher(compositeId);
        if (m.find()) {
            String nodeId = m.group(1);
            String[] tokens = RemoteClusterAware.splitIndexName(m.group(2));
            String cluster = tokens[0];
            String indexName = tokens[1];
            int shardId = Integer.parseInt(m.group(3));
            return new ShardProfileId(nodeId, indexName, shardId, cluster);
        } else {
            assert false : "Unable to match input against expected pattern of [nodeId][indexName][shardId]. Input: " + compositeId;
            logger.warn("Unable to match input against expected pattern of [nodeId][indexName][shardId]. Input: {}", compositeId);
            return null;
        }
    }
}
