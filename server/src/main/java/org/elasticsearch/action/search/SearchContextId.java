/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchContextId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SearchContextId {
    private final Map<ShardId, SearchContextIdForNode> shards;
    private final Map<String, AliasFilter> aliasFilter;

    private SearchContextId(Map<ShardId, SearchContextIdForNode> shards, Map<String, AliasFilter> aliasFilter) {
        this.shards = shards;
        this.aliasFilter = aliasFilter;
    }

    public Map<ShardId, SearchContextIdForNode> shards() {
        return shards;
    }

    public Map<String, AliasFilter> aliasFilter() {
        return aliasFilter;
    }

    static String encode(List<SearchPhaseResult> searchPhaseResults, Map<String, AliasFilter> aliasFilter, Version version) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(version.id);
            out.writeVInt(searchPhaseResults.size());
            for (SearchPhaseResult searchPhaseResult : searchPhaseResults) {
                SearchShardTarget target = searchPhaseResult.getSearchShardTarget();
                ShardId shardId = target.getShardId();
                // shardId
                out.writeString(shardId.getIndex().getName());
                out.writeString(shardId.getIndex().getUUID());
                out.writeVInt(shardId.getId());
                // nodeId
                if (target.getClusterAlias() != null) {
                    out.writeByte((byte) 1);
                    out.writeString(target.getClusterAlias());
                } else {
                    out.writeByte((byte) 0);
                }
                out.writeString(target.getNodeId());
                // readerId
                out.writeString(searchPhaseResult.getContextId().getReaderId());
                out.writeLong(searchPhaseResult.getContextId().getId());
            }
            out.writeMap(aliasFilter, StreamOutput::writeString, (outstream, val) -> val.writeTo(outstream));
            return Base64.getUrlEncoder().encodeToString(BytesReference.toBytes(out.bytes()));
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    static SearchContextId decode(String id) {
        final ByteBuffer byteBuffer;
        try {
            byteBuffer = ByteBuffer.wrap(Base64.getUrlDecoder().decode(id));
        } catch (Exception e) {
            throw new IllegalArgumentException("invalid id: [" + id + "]", e);
        }
        try (StreamInput in = new ByteBufferStreamInput(byteBuffer)) {
            Version version = Version.fromId(in.readVInt());
            final int numOfContexts = in.readVInt();
            final Map<ShardId, SearchContextIdForNode> shards = new HashMap<>();
            for (int i = 0; i < numOfContexts; i++) {
                final ShardId shardId = new ShardId(new Index(in.readString(), in.readString()), in.readVInt());
                final String clusterAlias;
                if (in.readByte() == 1) {
                    clusterAlias = in.readString();
                } else {
                    clusterAlias = null;
                }
                final String nodeId = in.readString();
                final ShardSearchContextId contextId = new ShardSearchContextId(in.readString(), in.readLong());
                shards.put(shardId, new SearchContextIdForNode(clusterAlias, nodeId, contextId));
            }
            Map<String, AliasFilter> aliasFilterMap = in.readMap(StreamInput::readString, AliasFilter::new);
            if (in.available() > 0) {
                throw new IllegalArgumentException("Not all bytes were read");
            }
            return new SearchContextId(Collections.unmodifiableMap(shards), Collections.unmodifiableMap(aliasFilterMap));
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
