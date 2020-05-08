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

package org.elasticsearch.action.search;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.RAMOutputStream;
import org.elasticsearch.Version;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.InternalScrollSearchRequest;
import org.elasticsearch.search.internal.SearchContextId;
import org.elasticsearch.transport.RemoteClusterAware;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

final class TransportSearchHelper {

    private static final String INCLUDE_CONTEXT_UUID = "include_context_uuid";

    static InternalScrollSearchRequest internalScrollSearchRequest(SearchContextId id, SearchScrollRequest request) {
        return new InternalScrollSearchRequest(request, id);
    }

    static String buildScrollId(AtomicArray<? extends SearchPhaseResult> searchPhaseResults, Version version) {
        boolean includeContextUUID = version.onOrAfter(Version.V_7_7_0);
        try (RAMOutputStream out = new RAMOutputStream()) {
            if (includeContextUUID) {
                out.writeString(INCLUDE_CONTEXT_UUID);
            }
            out.writeString(searchPhaseResults.length() == 1 ? ParsedScrollId.QUERY_AND_FETCH_TYPE : ParsedScrollId.QUERY_THEN_FETCH_TYPE);
            out.writeVInt(searchPhaseResults.asList().size());
            for (SearchPhaseResult searchPhaseResult : searchPhaseResults.asList()) {
                if (includeContextUUID) {
                    out.writeString(searchPhaseResult.getContextId().getReaderId());
                }
                out.writeLong(searchPhaseResult.getContextId().getId());
                SearchShardTarget searchShardTarget = searchPhaseResult.getSearchShardTarget();
                if (searchShardTarget.getClusterAlias() != null) {
                    out.writeString(
                        RemoteClusterAware.buildRemoteIndexName(searchShardTarget.getClusterAlias(), searchShardTarget.getNodeId()));
                } else {
                    out.writeString(searchShardTarget.getNodeId());
                }
            }
            byte[] bytes = new byte[(int) out.getFilePointer()];
            out.writeTo(bytes, 0);
            return Base64.getUrlEncoder().encodeToString(bytes);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static ParsedScrollId parseScrollId(String scrollId) {
        try {
            byte[] bytes = Base64.getUrlDecoder().decode(scrollId);
            ByteArrayDataInput in = new ByteArrayDataInput(bytes);
            final boolean includeContextUUID;
            final String type;
            final String firstChunk = in.readString();
            if (INCLUDE_CONTEXT_UUID.equals(firstChunk)) {
                includeContextUUID = true;
                type = in.readString();
            } else {
                includeContextUUID = false;
                type = firstChunk;
            }
            SearchContextIdForNode[] context = new SearchContextIdForNode[in.readVInt()];
            for (int i = 0; i < context.length; ++i) {
                final String contextUUID = includeContextUUID ? in.readString() : "";
                long id = in.readLong();
                String target = in.readString();
                String clusterAlias;
                final int index = target.indexOf(RemoteClusterAware.REMOTE_CLUSTER_INDEX_SEPARATOR);
                if (index == -1) {
                    clusterAlias = null;
                } else {
                    clusterAlias = target.substring(0, index);
                    target = target.substring(index+1);
                }
                context[i] = new SearchContextIdForNode(clusterAlias, target, new SearchContextId(contextUUID, id));
            }
            if (in.getPosition() != bytes.length) {
                throw new IllegalArgumentException("Not all bytes were read");
            }
            return new ParsedScrollId(scrollId, type, context);
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot parse scroll id", e);
        }
    }

    static String encodeSearchContextId(AtomicArray<? extends SearchPhaseResult> searchPhaseResults, Version version) {
        try (RAMOutputStream out = new RAMOutputStream()) {
            out.writeVInt(version.id);
            out.writeVInt(searchPhaseResults.asList().size());
            for (SearchPhaseResult searchPhaseResult : searchPhaseResults.asList()) {
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
            byte[] bytes = new byte[(int) out.getFilePointer()];
            out.writeTo(bytes, 0);
            return Base64.getUrlEncoder().encodeToString(bytes);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static Map<ShardId, SearchContextIdForNode> decodeSearchContextId(String searchContextId) {
        try {
            final byte[] bytes = Base64.getUrlDecoder().decode(searchContextId);
            final ByteArrayDataInput in = new ByteArrayDataInput(bytes);
            Version.fromId(in.readVInt());
            final int numOfContexts = in.readVInt();
            final Map<ShardId, SearchContextIdForNode> contexts = new HashMap<>();
            for (int i = 0; i < numOfContexts; i++) {
                final ShardId shardId = new ShardId(new Index(in.readString(), in.readString()), in.readVInt());
                final String clusterAlias;
                if (in.readByte() == 1) {
                    clusterAlias = in.readString();
                } else {
                    clusterAlias = null;
                }
                final String nodeId = in.readString();
                final SearchContextId contextId = new SearchContextId(in.readString(), in.readLong());
                contexts.put(shardId, new SearchContextIdForNode(clusterAlias, nodeId, contextId));
            }
            if (in.getPosition() != bytes.length) {
                throw new IllegalArgumentException("Not all bytes were read");
            }
            return contexts;
        } catch (Exception e) {
            throw new IllegalArgumentException("Can't parse reader context ids", e);
        }
    }

    private TransportSearchHelper() {

    }
}
