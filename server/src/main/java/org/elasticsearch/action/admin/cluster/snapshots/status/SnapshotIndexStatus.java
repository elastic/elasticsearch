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

package org.elasticsearch.action.admin.cluster.snapshots.status;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

/**
 * Represents snapshot status of all shards in the index
 */
public class SnapshotIndexStatus implements Iterable<SnapshotIndexShardStatus>, ToXContentFragment {

    private final String index;

    private final Map<Integer, SnapshotIndexShardStatus> indexShards;

    private final SnapshotShardsStats shardsStats;

    private final SnapshotStats stats;

    SnapshotIndexStatus(String index, Collection<SnapshotIndexShardStatus> shards) {
        this.index = index;

        Map<Integer, SnapshotIndexShardStatus> indexShards = new HashMap<>();
        stats = new SnapshotStats();
        for (SnapshotIndexShardStatus shard : shards) {
            indexShards.put(shard.getShardId().getId(), shard);
            stats.add(shard.getStats());
        }
        shardsStats = new SnapshotShardsStats(shards);
        this.indexShards = unmodifiableMap(indexShards);
    }

    public SnapshotIndexStatus(String index, Map<Integer, SnapshotIndexShardStatus> indexShards, SnapshotShardsStats shardsStats, SnapshotStats stats) {
        this.index = index;
        this.indexShards = indexShards;
        this.shardsStats = shardsStats;
        this.stats = stats;
    }

    /**
     * Returns the index name
     */
    public String getIndex() {
        return this.index;
    }

    /**
     * A shard id to index snapshot shard status map
     */
    public Map<Integer, SnapshotIndexShardStatus> getShards() {
        return this.indexShards;
    }

    /**
     * Shards stats
     */
    public SnapshotShardsStats getShardsStats() {
        return shardsStats;
    }

    /**
     * Returns snapshot stats
     */
    public SnapshotStats getStats() {
        return stats;
    }

    @Override
    public Iterator<SnapshotIndexShardStatus> iterator() {
        return indexShards.values().iterator();
    }

    static final class Fields {
        static final String SHARDS = "shards";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getIndex());
        shardsStats.toXContent(builder, params);
        stats.toXContent(builder, params);
        builder.startObject(Fields.SHARDS);
        for (SnapshotIndexShardStatus shard : indexShards.values()) {
            shard.toXContent(builder, params);
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    public static SnapshotIndexStatus fromXContent(XContentParser parser) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser::getTokenLocation);
        String indexName = parser.currentName();
        XContentParser.Token token = parser.nextToken();
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser::getTokenLocation);
        SnapshotShardsStats shardsStats = null;
        SnapshotStats stats = null;
        Map<Integer, SnapshotIndexShardStatus> shards = new HashMap<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token.equals(XContentParser.Token.FIELD_NAME)) {
                String currentName = parser.currentName();
                if (currentName.equals(SnapshotShardsStats.Fields.SHARDS_STATS)) {
                    shardsStats = SnapshotShardsStats.fromXContent(parser);
                } else if (currentName.equals(SnapshotStats.Fields.STATS)) {
                    stats = SnapshotStats.fromXContent(parser);
                } else if (currentName.equals(Fields.SHARDS)) {
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
                    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                        SnapshotIndexShardStatus shardStatus = SnapshotIndexShardStatus.fromXContent(parser, indexName);
                        shards.put(shardStatus.getShardId().getId(), shardStatus);
                    }
                } else {
                    throw new ElasticsearchParseException("failed to parse snapshot index status [{}], unknown field [{}]", indexName, currentName);
                }
            } else {
                throw new ElasticsearchParseException("failed to parse snapshot index status [{}]", indexName);
            }
        }
        return new SnapshotIndexStatus(indexName, shards, shardsStats, stats);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SnapshotIndexStatus that = (SnapshotIndexStatus) o;

        if (index != null ? !index.equals(that.index) : that.index != null) return false;
        if (indexShards != null ? !indexShards.equals(that.indexShards) : that.indexShards != null) return false;
        if (shardsStats != null ? !shardsStats.equals(that.shardsStats) : that.shardsStats != null) return false;
        return stats != null ? stats.equals(that.stats) : that.stats == null;
    }

    @Override
    public int hashCode() {
        int result = index != null ? index.hashCode() : 0;
        result = 31 * result + (indexShards != null ? indexShards.hashCode() : 0);
        result = 31 * result + (shardsStats != null ? shardsStats.hashCode() : 0);
        result = 31 * result + (stats != null ? stats.hashCode() : 0);
        return result;
    }
}
