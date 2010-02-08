/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.search.fetch.FetchSearchResultProvider;
import org.elasticsearch.search.internal.InternalScrollSearchRequest;
import org.elasticsearch.search.internal.InternalSearchRequest;
import org.elasticsearch.util.Tuple;

import java.util.regex.Pattern;

/**
 * @author kimchy (Shay Banon)
 */
public abstract class TransportSearchHelper {


    private final static Pattern scrollIdPattern;

    static {
        scrollIdPattern = Pattern.compile(";");
    }

    public static InternalSearchRequest internalSearchRequest(ShardRouting shardRouting, SearchRequest request) {
        InternalSearchRequest internalRequest = new InternalSearchRequest(shardRouting, request.source());
        internalRequest.from(request.from()).size(request.size());
        internalRequest.scroll(request.scroll());
        if (request.queryBoost() != null) {
            if (request.queryBoost().containsKey(shardRouting.index())) {
                internalRequest.queryBoost(request.queryBoost().get(shardRouting.index()));
            }
        }
        internalRequest.timeout(request.timeout());
        internalRequest.types(request.types());
        return internalRequest;
    }

    public static InternalScrollSearchRequest internalScrollSearchRequest(long id, SearchScrollRequest request) {
        InternalScrollSearchRequest internalRequest = new InternalScrollSearchRequest(id);
        internalRequest.scroll(request.scroll());
        return internalRequest;
    }

    public static String buildScrollId(SearchType searchType, Iterable<? extends FetchSearchResultProvider> fetchResults) {
        if (searchType == SearchType.DFS_QUERY_THEN_FETCH || searchType == SearchType.QUERY_THEN_FETCH) {
            return buildScrollId(ParsedScrollId.QUERY_THEN_FETCH_TYPE, fetchResults);
        } else if (searchType == SearchType.QUERY_AND_FETCH || searchType == SearchType.DFS_QUERY_AND_FETCH) {
            return buildScrollId(ParsedScrollId.QUERY_AND_FETCH_TYPE, fetchResults);
        } else {
            throw new ElasticSearchIllegalStateException();
        }
    }

    public static String buildScrollId(String type, Iterable<? extends FetchSearchResultProvider> fetchResults) {
        StringBuilder sb = new StringBuilder().append(type).append(';');
        for (FetchSearchResultProvider fetchResult : fetchResults) {
            sb.append(fetchResult.id()).append(':').append(fetchResult.shardTarget().nodeId()).append(';');
        }
        return sb.toString();
    }

    public static ParsedScrollId parseScrollId(String scrollId) {
        String[] elements = scrollIdPattern.split(scrollId);
        @SuppressWarnings({"unchecked"}) Tuple<String, Long>[] values = new Tuple[elements.length - 1];
        for (int i = 1; i < elements.length; i++) {
            String element = elements[i];
            int index = element.indexOf(':');
            values[i - 1] = new Tuple<String, Long>(element.substring(index + 1), Long.parseLong(element.substring(0, index)));
        }
        return new ParsedScrollId(scrollId, elements[0], values);
    }

    private TransportSearchHelper() {

    }

}
