/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.RAMOutputStream;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.InternalScrollSearchRequest;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.transport.RemoteClusterAware;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Base64;

final class TransportSearchHelper {

    private static final String INCLUDE_CONTEXT_UUID = "include_context_uuid";

    static InternalScrollSearchRequest internalScrollSearchRequest(ShardSearchContextId id, SearchScrollRequest request) {
        return new InternalScrollSearchRequest(request, id);
    }

    static String buildScrollId(AtomicArray<? extends SearchPhaseResult> searchPhaseResults) {
        try (RAMOutputStream out = new RAMOutputStream()) {
            out.writeString(INCLUDE_CONTEXT_UUID);
            out.writeString(searchPhaseResults.length() == 1 ? ParsedScrollId.QUERY_AND_FETCH_TYPE : ParsedScrollId.QUERY_THEN_FETCH_TYPE);
            out.writeVInt(searchPhaseResults.asList().size());
            for (SearchPhaseResult searchPhaseResult : searchPhaseResults.asList()) {
                out.writeString(searchPhaseResult.getContextId().getSessionId());
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
                context[i] = new SearchContextIdForNode(clusterAlias, target, new ShardSearchContextId(contextUUID, id));
            }
            if (in.getPosition() != bytes.length) {
                throw new IllegalArgumentException("Not all bytes were read");
            }
            return new ParsedScrollId(scrollId, type, context);
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot parse scroll id", e);
        }
    }

    private TransportSearchHelper() {

    }
}
