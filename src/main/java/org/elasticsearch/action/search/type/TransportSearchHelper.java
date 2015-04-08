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

package org.elasticsearch.action.search.type;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.internal.InternalScrollSearchRequest;
import org.elasticsearch.search.internal.ShardSearchTransportRequest;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public abstract class TransportSearchHelper {

    public static ShardSearchTransportRequest internalSearchRequest(ShardRouting shardRouting, int numberOfShards, SearchRequest request, String[] filteringAliases, long nowInMillis) {
        return new ShardSearchTransportRequest(request, shardRouting, numberOfShards, filteringAliases, nowInMillis);
    }

    public static InternalScrollSearchRequest internalScrollSearchRequest(long id, SearchScrollRequest request) {
        return new InternalScrollSearchRequest(request, id);
    }

    public static String buildScrollId(SearchType searchType, AtomicArray<? extends SearchPhaseResult> searchPhaseResults, @Nullable Map<String, String> attributes) throws IOException {
        if (searchType == SearchType.DFS_QUERY_THEN_FETCH || searchType == SearchType.QUERY_THEN_FETCH) {
            return buildScrollId(ParsedScrollId.QUERY_THEN_FETCH_TYPE, searchPhaseResults, attributes);
        } else if (searchType == SearchType.QUERY_AND_FETCH || searchType == SearchType.DFS_QUERY_AND_FETCH) {
            return buildScrollId(ParsedScrollId.QUERY_AND_FETCH_TYPE, searchPhaseResults, attributes);
        } else if (searchType == SearchType.SCAN) {
            return buildScrollId(ParsedScrollId.SCAN, searchPhaseResults, attributes);
        } else {
            throw new ElasticsearchIllegalStateException("search_type [" + searchType + "] not supported");
        }
    }

    public static String buildScrollId(String type, AtomicArray<? extends SearchPhaseResult> searchPhaseResults, @Nullable Map<String, String> attributes) throws IOException {
        StringBuilder sb = new StringBuilder().append(type).append(';');
        sb.append(searchPhaseResults.asList().size()).append(';');
        for (AtomicArray.Entry<? extends SearchPhaseResult> entry : searchPhaseResults.asList()) {
            SearchPhaseResult searchPhaseResult = entry.value;
            sb.append(searchPhaseResult.id()).append(':').append(searchPhaseResult.shardTarget().nodeId()).append(';');
        }
        if (attributes == null) {
            sb.append("0;");
        } else {
            sb.append(attributes.size()).append(";");
            for (Map.Entry<String, String> entry : attributes.entrySet()) {
                sb.append(entry.getKey()).append(':').append(entry.getValue()).append(';');
            }
        }
        BytesRef bytesRef = new BytesRef(sb);
        return Base64.encodeBytes(bytesRef.bytes, bytesRef.offset, bytesRef.length, Base64.URL_SAFE);
    }

    public static ParsedScrollId parseScrollId(String scrollId) {
        CharsRefBuilder spare = new CharsRefBuilder();
        try {
            byte[] decode = Base64.decode(scrollId, Base64.URL_SAFE);
            spare.copyUTF8Bytes(decode, 0, decode.length);
        } catch (Exception e) {
            throw new ElasticsearchIllegalArgumentException("Failed to decode scrollId", e);
        }
        String[] elements = Strings.splitStringToArray(spare.get(), ';');
        if (elements.length < 2) {
            throw new ElasticsearchIllegalArgumentException("Malformed scrollId [" + scrollId + "]");
        }

        int index = 0;
        String type = elements[index++];
        int contextSize = Integer.parseInt(elements[index++]);
        if (elements.length < contextSize + 2) {
            throw new ElasticsearchIllegalArgumentException("Malformed scrollId [" + scrollId + "]");
        }

        @SuppressWarnings({"unchecked"}) Tuple<String, Long>[] context = new Tuple[contextSize];
        for (int i = 0; i < contextSize; i++) {
            String element = elements[index++];
            int sep = element.indexOf(':');
            if (sep == -1) {
                throw new ElasticsearchIllegalArgumentException("Malformed scrollId [" + scrollId + "]");
            }
            context[i] = new Tuple<>(element.substring(sep + 1), Long.parseLong(element.substring(0, sep)));
        }
        Map<String, String> attributes;
        int attributesSize = Integer.parseInt(elements[index++]);
        if (attributesSize == 0) {
            attributes = ImmutableMap.of();
        } else {
            attributes = Maps.newHashMapWithExpectedSize(attributesSize);
            for (int i = 0; i < attributesSize; i++) {
                String element = elements[index++];
                int sep = element.indexOf(':');
                attributes.put(element.substring(0, sep), element.substring(sep + 1));
            }
        }
        return new ParsedScrollId(scrollId, type, context, attributes);
    }

    private TransportSearchHelper() {

    }

}
