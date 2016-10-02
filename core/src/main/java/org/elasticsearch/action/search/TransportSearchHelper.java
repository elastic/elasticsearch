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
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.internal.InternalScrollSearchRequest;
import org.elasticsearch.search.internal.ShardSearchTransportRequest;

import java.io.IOException;
import java.util.Base64;

/**
 *
 */
final class TransportSearchHelper {

    static ShardSearchTransportRequest internalSearchRequest(ShardRouting shardRouting, int numberOfShards, SearchRequest request,
                                                             String[] filteringAliases, long nowInMillis) {
        return new ShardSearchTransportRequest(request, shardRouting, numberOfShards, filteringAliases, nowInMillis);
    }

    static InternalScrollSearchRequest internalScrollSearchRequest(long id, SearchScrollRequest request) {
        return new InternalScrollSearchRequest(request, id);
    }

    static String buildScrollId(SearchType searchType, AtomicArray<? extends SearchPhaseResult> searchPhaseResults) throws IOException {
        if (searchType == SearchType.DFS_QUERY_THEN_FETCH || searchType == SearchType.QUERY_THEN_FETCH) {
            return buildScrollId(ParsedScrollId.QUERY_THEN_FETCH_TYPE, searchPhaseResults);
        } else if (searchType == SearchType.QUERY_AND_FETCH || searchType == SearchType.DFS_QUERY_AND_FETCH) {
            return buildScrollId(ParsedScrollId.QUERY_AND_FETCH_TYPE, searchPhaseResults);
        } else {
            throw new IllegalStateException("search_type [" + searchType + "] not supported");
        }
    }

    static String buildScrollId(String type, AtomicArray<? extends SearchPhaseResult> searchPhaseResults) throws IOException {
        try (RAMOutputStream out = new RAMOutputStream()) {
            out.writeString(type);
            out.writeVInt(searchPhaseResults.asList().size());
            for (AtomicArray.Entry<? extends SearchPhaseResult> entry : searchPhaseResults.asList()) {
                SearchPhaseResult searchPhaseResult = entry.value;
                out.writeLong(searchPhaseResult.id());
                out.writeString(searchPhaseResult.shardTarget().nodeId());
            }
            byte[] bytes = new byte[(int) out.getFilePointer()];
            out.writeTo(bytes, 0);
            return Base64.getUrlEncoder().encodeToString(bytes);
        }
    }

    static ParsedScrollId parseScrollId(String scrollId) {
        try {
            byte[] bytes = Base64.getUrlDecoder().decode(scrollId);
            ByteArrayDataInput in = new ByteArrayDataInput(bytes);
            String type = in.readString();
            ScrollIdForNode[] context = new ScrollIdForNode[in.readVInt()];
            for (int i = 0; i < context.length; ++i) {
                long id = in.readLong();
                String target = in.readString();
                context[i] = new ScrollIdForNode(target, id);
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
