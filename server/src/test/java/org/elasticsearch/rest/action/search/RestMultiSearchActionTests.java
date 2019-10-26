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

package org.elasticsearch.rest.action.search;


import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;

public class RestMultiSearchActionTests extends RestActionTestCase {
    public void testRestRequestQueryParamsParsing() throws IOException {
        String body = "{\"index\":\"book-index\"}\n" +
                        "{\"query\": {\"match_all\" : {}}}\n" +
                        "{\"index\":\"other-index\"}\n" +
                        "{\"query\": {\"match_all\" : {}}}\n";
        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry())
            .withMethod(RestRequest.Method.GET)
            .withPath("/_msearch")
            .withParams(new HashMap<>() {{
                put("explain", "true");
                put("stored_fields", "author,title");
                put("docvalue_fields", "author,title");
                put("from", "0");
                put("size", "5");
                put("sort", "author:asc,title:desc");
                put("_source", "true");
                put("terminate_after", "1");
                put("stats", "merge,refresh");
                put("timeout", "10s");
                put("track_scores", "true");
                put("track_total_hits", "true");
                put("version", "true");
                put("seq_no_primary_term", "true");
            }})
            .withContent(new BytesArray(body), XContentType.JSON)
            .build();

        MultiSearchRequest multiSearchRequest = RestMultiSearchAction.parseRequest(restRequest, true);

        assertEquals(2, multiSearchRequest.requests().size());

        // Query param settings will be applied to each individual searchRequest
        for(SearchRequest searchRequest : multiSearchRequest.requests()) {
            assertTrue(searchRequest.source().explain());
            assertEquals(List.of("author", "title"), searchRequest.source().storedFields().fieldNames());
            assertEquals(List.of("author", "title"), searchRequest.source().docValueFields()
                .stream().map(f -> f.field).collect(Collectors.toList()));
            assertEquals(0, searchRequest.source().from());
            assertEquals(5, searchRequest.source().size());
            assertEquals(List.of(SortBuilders.fieldSort("author").order(SortOrder.ASC),
                SortBuilders.fieldSort("title").order(SortOrder.DESC)),
                searchRequest.source().sorts());
            assertTrue(searchRequest.source().fetchSource().fetchSource());
            assertEquals(1, searchRequest.source().terminateAfter());
            assertEquals(List.of("merge", "refresh"), searchRequest.source().stats());
            assertEquals(new TimeValue(10, TimeUnit.SECONDS), searchRequest.source().timeout());
            assertTrue(searchRequest.source().trackScores());
            assertEquals(SearchContext.TRACK_TOTAL_HITS_ACCURATE, searchRequest.source().trackTotalHitsUpTo().intValue());
            assertTrue(searchRequest.source().version());
            assertTrue(searchRequest.source().seqNoAndPrimaryTerm());
        }
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(singletonList(new NamedXContentRegistry.Entry(QueryBuilder.class,
            new ParseField(MatchAllQueryBuilder.NAME), (p, c) -> MatchAllQueryBuilder.fromXContent(p))));
    }
}
