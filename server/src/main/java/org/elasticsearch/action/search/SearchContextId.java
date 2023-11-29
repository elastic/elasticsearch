/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.transport.RemoteClusterAware;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

    public static String encode(
        List<SearchPhaseResult> searchPhaseResults,
        Map<String, AliasFilter> aliasFilter,
        TransportVersion version
    ) {
        final BytesReference bytesReference;
        try (var encodedStreamOutput = new BytesStreamOutput()) {
            try (var out = new OutputStreamStreamOutput(Base64.getUrlEncoder().wrap(encodedStreamOutput))) {
                out.setTransportVersion(version);
                TransportVersion.writeVersion(version, out);
                out.writeCollection(searchPhaseResults, SearchContextId::writeSearchPhaseResult);
                out.writeMap(aliasFilter, StreamOutput::writeWriteable);
            }
            bytesReference = encodedStreamOutput.bytes();
        } catch (IOException e) {
            assert false : e;
            throw new IllegalArgumentException(e);
        }
        final BytesRef bytesRef = bytesReference.toBytesRef();
        return new String(bytesRef.bytes, bytesRef.offset, bytesRef.length, StandardCharsets.ISO_8859_1);
    }

    private static void writeSearchPhaseResult(StreamOutput out, SearchPhaseResult searchPhaseResult) throws IOException {
        final SearchShardTarget target = searchPhaseResult.getSearchShardTarget();
        target.getShardId().writeTo(out);
        new SearchContextIdForNode(target.getClusterAlias(), target.getNodeId(), searchPhaseResult.getContextId()).writeTo(out);
    }

    public static SearchContextId decode(NamedWriteableRegistry namedWriteableRegistry, String id) {
        try (
            var decodedInputStream = Base64.getUrlDecoder().wrap(new ByteArrayInputStream(id.getBytes(StandardCharsets.ISO_8859_1)));
            var in = new NamedWriteableAwareStreamInput(new InputStreamStreamInput(decodedInputStream), namedWriteableRegistry)
        ) {
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

    private static void readShardsMapEntry(StreamInput in, Map<ShardId, SearchContextIdForNode> shards) throws IOException {
        shards.put(new ShardId(in), new SearchContextIdForNode(in));
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
