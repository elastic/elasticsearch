/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.transport.RemoteClusterAware;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public final class SearchContextId {
    private final Map<ShardId, SearchContextIdForNode> shards;
    private final Map<String, AliasFilter> aliasFilter;
    private transient Set<ShardSearchContextId> contextIds;

    SearchContextId(Map<ShardId, SearchContextIdForNode> shards, Map<String, AliasFilter> aliasFilter) {
        this.shards = shards;
        this.aliasFilter = aliasFilter;
        this.contextIds = shards.values().stream().map(SearchContextIdForNode::getSearchContextId).collect(Collectors.toSet());
    }

    public Map<ShardId, SearchContextIdForNode> shards() {
        return shards;
    }

    public Map<String, AliasFilter> aliasFilter() {
        return aliasFilter;
    }

    public boolean contains(ShardSearchContextId contextId) {
        return contextIds.contains(contextId);
    }

    public static String encode(List<SearchPhaseResult> searchPhaseResults, Map<String, AliasFilter> aliasFilter, Version version) {
        final Map<ShardId, SearchContextIdForNode> shards = new HashMap<>();
        for (SearchPhaseResult searchPhaseResult : searchPhaseResults) {
            final SearchShardTarget target = searchPhaseResult.getSearchShardTarget();
            shards.put(
                target.getShardId(),
                new SearchContextIdForNode(target.getClusterAlias(), target.getNodeId(), searchPhaseResult.getContextId())
            );
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(version);
            Version.writeVersion(version, out);
            out.writeMap(shards, (o, k) -> k.writeTo(o), (o, v) -> v.writeTo(o));
            out.writeMap(aliasFilter, StreamOutput::writeString, (o, v) -> v.writeTo(o));
            return Base64.getUrlEncoder().encodeToString(BytesReference.toBytes(out.bytes()));
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static SearchContextId decode(NamedWriteableRegistry namedWriteableRegistry, String id) {
        final ByteBuffer byteBuffer;
        try {
            byteBuffer = ByteBuffer.wrap(Base64.getUrlDecoder().decode(id));
        } catch (Exception e) {
            throw new IllegalArgumentException("invalid id: [" + id + "]", e);
        }
        try (StreamInput in = new NamedWriteableAwareStreamInput(new ByteBufferStreamInput(byteBuffer), namedWriteableRegistry)) {
            final Version version = Version.readVersion(in);
            in.setVersion(version);
            final Map<ShardId, SearchContextIdForNode> shards = in.readMap(ShardId::new, SearchContextIdForNode::new);
            final Map<String, AliasFilter> aliasFilters = in.readMap(StreamInput::readString, AliasFilter::new);
            if (in.available() > 0) {
                throw new IllegalArgumentException("Not all bytes were read");
            }
            return new SearchContextId(Collections.unmodifiableMap(shards), Collections.unmodifiableMap(aliasFilters));
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public String[] getActualIndices() {
        final Set<String> indices = new HashSet<>();
        for (Map.Entry<ShardId, SearchContextIdForNode> entry : shards().entrySet()) {
            final String indexName = entry.getKey().getIndexName();
            final String clusterAlias = entry.getValue().getClusterAlias();
            if (Strings.isEmpty(clusterAlias)) {
                indices.add(indexName);
            } else {
                indices.add(clusterAlias + RemoteClusterAware.REMOTE_CLUSTER_INDEX_SEPARATOR + indexName);
            }
        }
        return indices.toArray(String[]::new);
    }
}
