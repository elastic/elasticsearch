/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.transport.RemoteClusterAware;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public final class SearchContextId {
    private final Map<ShardId, SearchContextIdForNode> shards;
    private final Map<String, AliasFilter> aliasFilter;
    private final transient Set<ShardSearchContextId> contextIds;

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

    public static BytesReference encode(
        List<SearchPhaseResult> searchPhaseResults,
        Map<String, AliasFilter> aliasFilter,
        TransportVersion version,
        ShardSearchFailure[] shardFailures
    ) {
        assert shardFailures.length == 0 || version.onOrAfter(TransportVersions.ALLOW_PARTIAL_SEARCH_RESULTS_IN_PIT)
            : "[allow_partial_search_results] cannot be enabled on a cluster that has not been fully upgraded to version ["
                + TransportVersions.ALLOW_PARTIAL_SEARCH_RESULTS_IN_PIT
                + "] or higher.";
        try (var out = new BytesStreamOutput()) {
            out.setTransportVersion(version);
            TransportVersion.writeVersion(version, out);
            boolean allowNullContextId = out.getTransportVersion().onOrAfter(TransportVersions.ALLOW_PARTIAL_SEARCH_RESULTS_IN_PIT);
            int shardSize = searchPhaseResults.size() + (allowNullContextId ? shardFailures.length : 0);
            out.writeVInt(shardSize);
            for (var searchResult : searchPhaseResults) {
                final SearchShardTarget target = searchResult.getSearchShardTarget();
                target.getShardId().writeTo(out);
                new SearchContextIdForNode(target.getClusterAlias(), target.getNodeId(), searchResult.getContextId()).writeTo(out);
            }
            if (allowNullContextId) {
                for (var failure : shardFailures) {
                    failure.shard().getShardId().writeTo(out);
                    new SearchContextIdForNode(failure.shard().getClusterAlias(), null, null).writeTo(out);
                }
            }
            out.writeMap(aliasFilter, StreamOutput::writeWriteable);
            return out.bytes();
        } catch (IOException e) {
            assert false : e;
            throw new IllegalArgumentException(e);
        }
    }

    public static SearchContextId decode(NamedWriteableRegistry namedWriteableRegistry, BytesReference id) {
        try (var in = new NamedWriteableAwareStreamInput(id.streamInput(), namedWriteableRegistry)) {
            final TransportVersion version = TransportVersion.readVersion(in);
            in.setTransportVersion(version);
            final Map<ShardId, SearchContextIdForNode> shards = Collections.unmodifiableMap(
                in.readCollection(Maps::newHashMapWithExpectedSize, SearchContextId::readShardsMapEntry)
            );
            final Map<String, AliasFilter> aliasFilters = in.readImmutableMap(AliasFilter::readFrom);
            if (in.available() > 0) {
                throw new IllegalArgumentException("Not all bytes were read");
            }
            return new SearchContextId(shards, aliasFilters);
        } catch (IOException e) {
            assert false : e;
            throw new IllegalArgumentException(e);
        }
    }

    public static String[] decodeIndices(BytesReference id) {
        try (var in = id.streamInput()) {
            final TransportVersion version = TransportVersion.readVersion(in);
            in.setTransportVersion(version);
            final Map<ShardId, SearchContextIdForNode> shards = Collections.unmodifiableMap(
                in.readCollection(Maps::newHashMapWithExpectedSize, SearchContextId::readShardsMapEntry)
            );
            return new SearchContextId(shards, Collections.emptyMap()).getActualIndices();
        } catch (IOException e) {
            assert false : e;
            throw new IllegalArgumentException(e);
        }
    }

    private static void readShardsMapEntry(StreamInput in, Map<ShardId, SearchContextIdForNode> shards) throws IOException {
        shards.put(new ShardId(in), new SearchContextIdForNode(in));
    }

    public String[] getActualIndices() {
        // ensure that the order is consistent
        final Set<String> indices = new TreeSet<>();
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
