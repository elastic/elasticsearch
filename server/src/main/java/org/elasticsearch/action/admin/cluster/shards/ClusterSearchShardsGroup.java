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

package org.elasticsearch.action.admin.cluster.shards;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.AliasFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class ClusterSearchShardsGroup implements Streamable, ToXContentObject {

    private static final ParseField ALIASES = new ParseField("aliases");
    private static final ParseField FILTER = new ParseField("filter");

    private static final ConstructingObjectParser<AliasFilter, String> PARSER =
        new ConstructingObjectParser<>("alias_filter", false,
            a -> {
                final String[] aliases = (String[]) a[0];
                final QueryBuilder filter = (QueryBuilder) a[1];
                return new AliasFilter(filter, aliases);
            });

    static {
        PARSER.declareField(constructorArg(),
            (p, c) -> p.list().toArray(Strings.EMPTY_ARRAY), ALIASES, ObjectParser.ValueType.STRING_ARRAY);
        PARSER.declareField(constructorArg(),
            (p, c) -> AbstractQueryBuilder.parseInnerQueryBuilder(p), FILTER, ObjectParser.ValueType.OBJECT);
    }

    private ShardId shardId;
    private ShardRouting[] shards;

    private ClusterSearchShardsGroup() {

    }

    public ClusterSearchShardsGroup(ShardId shardId, ShardRouting[] shards) {
        this.shardId = shardId;
        this.shards = shards;
    }

    static ClusterSearchShardsGroup readSearchShardsGroupResponse(StreamInput in) throws IOException {
        ClusterSearchShardsGroup response = new ClusterSearchShardsGroup();
        response.readFrom(in);
        return response;
    }

    public ShardId getShardId() {
        return shardId;
    }

    public ShardRouting[] getShards() {
        return shards;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        shardId = ShardId.readShardId(in);
        shards = new ShardRouting[in.readVInt()];
        for (int i = 0; i < shards.length; i++) {
            shards[i] = new ShardRouting(shardId, in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardId.writeTo(out);
        out.writeVInt(shards.length);
        for (ShardRouting shardRouting : shards) {
            shardRouting.writeToThin(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray();
        for (ShardRouting shard : getShards()) {
            shard.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    public static ClusterSearchShardsGroup fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser::getTokenLocation);
        parser.nextToken();
        List<ShardRouting> routings = new ArrayList<>();
        while (parser.currentToken() != XContentParser.Token.END_ARRAY) {
            final ShardRouting routing = ShardRouting.fromXContent(parser);
            routings.add(routing);
            parser.nextToken();
        }

        ensureExpectedToken(XContentParser.Token.END_ARRAY, parser.currentToken(), parser::getTokenLocation);

        if (routings.isEmpty()) {
            return new ClusterSearchShardsGroup();
        }
        final ShardRouting[] shards = routings.toArray(new ShardRouting[0]);
        ShardId shardId = shards[0].shardId();
        return new ClusterSearchShardsGroup(shardId, shards);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClusterSearchShardsGroup group = (ClusterSearchShardsGroup) o;

        return shardId != null ? shardId.equals(group.shardId) : group.shardId == null
            && Arrays.equals(shards, group.shards);
    }

    @Override
    public int hashCode() {
        int result = shardId != null ? shardId.hashCode() : 0;
        result = 31 * result + Arrays.hashCode(shards);
        return result;
    }

    @Override
    public String toString() {
        return "ClusterSearchShardsGroup{shardId=" + shardId + ", shards=" + Arrays.toString(shards) + '}';
    }
}
