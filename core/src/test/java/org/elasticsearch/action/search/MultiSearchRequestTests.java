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

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryParser;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.search.RestMultiSearchAction;
import org.elasticsearch.search.SearchRequestParsers;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.StreamsUtils;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class MultiSearchRequestTests extends ESTestCase {
    public void testSimpleAdd() throws Exception {
        MultiSearchRequest request = parseMultiSearchRequest("/org/elasticsearch/action/search/simple-msearch1.json");
        assertThat(request.requests().size(),
                equalTo(8));
        assertThat(request.requests().get(0).indices()[0],
                equalTo("test"));
        assertThat(request.requests().get(0).indicesOptions(),
                equalTo(IndicesOptions.fromOptions(true, true, true, true, IndicesOptions.strictExpandOpenAndForbidClosed())));
        assertThat(request.requests().get(0).types().length,
                equalTo(0));
        assertThat(request.requests().get(1).indices()[0],
                equalTo("test"));
        assertThat(request.requests().get(1).indicesOptions(),
                equalTo(IndicesOptions.fromOptions(false, true, true, true, IndicesOptions.strictExpandOpenAndForbidClosed())));
        assertThat(request.requests().get(1).types()[0],
                equalTo("type1"));
        assertThat(request.requests().get(2).indices()[0],
                equalTo("test"));
        assertThat(request.requests().get(2).indicesOptions(),
                equalTo(IndicesOptions.fromOptions(false, true, true, false, IndicesOptions.strictExpandOpenAndForbidClosed())));
        assertThat(request.requests().get(3).indices()[0],
                equalTo("test"));
        assertThat(request.requests().get(3).indicesOptions(),
                equalTo(IndicesOptions.fromOptions(true, true, true, true, IndicesOptions.strictExpandOpenAndForbidClosed())));
        assertThat(request.requests().get(4).indices()[0],
                equalTo("test"));
        assertThat(request.requests().get(4).indicesOptions(),
                equalTo(IndicesOptions.fromOptions(true, false, false, true, IndicesOptions.strictExpandOpenAndForbidClosed())));

        assertThat(request.requests().get(5).indices(), is(Strings.EMPTY_ARRAY));
        assertThat(request.requests().get(5).types().length, equalTo(0));
        assertThat(request.requests().get(6).indices(), is(Strings.EMPTY_ARRAY));
        assertThat(request.requests().get(6).types().length, equalTo(0));
        assertThat(request.requests().get(6).searchType(), equalTo(SearchType.DFS_QUERY_THEN_FETCH));
        assertThat(request.requests().get(7).indices(), is(Strings.EMPTY_ARRAY));
        assertThat(request.requests().get(7).types().length, equalTo(0));
    }

    public void testSimpleAdd2() throws Exception {
        MultiSearchRequest request = parseMultiSearchRequest("/org/elasticsearch/action/search/simple-msearch2.json");
        assertThat(request.requests().size(), equalTo(5));
        assertThat(request.requests().get(0).indices()[0], equalTo("test"));
        assertThat(request.requests().get(0).types().length, equalTo(0));
        assertThat(request.requests().get(1).indices()[0], equalTo("test"));
        assertThat(request.requests().get(1).types()[0], equalTo("type1"));
        assertThat(request.requests().get(2).indices(), is(Strings.EMPTY_ARRAY));
        assertThat(request.requests().get(2).types().length, equalTo(0));
        assertThat(request.requests().get(3).indices(), is(Strings.EMPTY_ARRAY));
        assertThat(request.requests().get(3).types().length, equalTo(0));
        assertThat(request.requests().get(3).searchType(), equalTo(SearchType.DFS_QUERY_THEN_FETCH));
        assertThat(request.requests().get(4).indices(), is(Strings.EMPTY_ARRAY));
        assertThat(request.requests().get(4).types().length, equalTo(0));
    }

    public void testSimpleAdd3() throws Exception {
        MultiSearchRequest request = parseMultiSearchRequest("/org/elasticsearch/action/search/simple-msearch3.json");
        assertThat(request.requests().size(), equalTo(4));
        assertThat(request.requests().get(0).indices()[0], equalTo("test0"));
        assertThat(request.requests().get(0).indices()[1], equalTo("test1"));
        assertThat(request.requests().get(1).indices()[0], equalTo("test2"));
        assertThat(request.requests().get(1).indices()[1], equalTo("test3"));
        assertThat(request.requests().get(1).types()[0], equalTo("type1"));
        assertThat(request.requests().get(2).indices()[0], equalTo("test4"));
        assertThat(request.requests().get(2).indices()[1], equalTo("test1"));
        assertThat(request.requests().get(2).types()[0], equalTo("type2"));
        assertThat(request.requests().get(2).types()[1], equalTo("type1"));
        assertThat(request.requests().get(3).indices(), is(Strings.EMPTY_ARRAY));
        assertThat(request.requests().get(3).types().length, equalTo(0));
        assertThat(request.requests().get(3).searchType(), equalTo(SearchType.DFS_QUERY_THEN_FETCH));
    }

    public void testSimpleAdd4() throws Exception {
        MultiSearchRequest request = parseMultiSearchRequest("/org/elasticsearch/action/search/simple-msearch4.json");
        assertThat(request.requests().size(), equalTo(3));
        assertThat(request.requests().get(0).indices()[0], equalTo("test0"));
        assertThat(request.requests().get(0).indices()[1], equalTo("test1"));
        assertThat(request.requests().get(0).requestCache(), equalTo(true));
        assertThat(request.requests().get(0).preference(), nullValue());
        assertThat(request.requests().get(1).indices()[0], equalTo("test2"));
        assertThat(request.requests().get(1).indices()[1], equalTo("test3"));
        assertThat(request.requests().get(1).types()[0], equalTo("type1"));
        assertThat(request.requests().get(1).requestCache(), nullValue());
        assertThat(request.requests().get(1).preference(), equalTo("_local"));
        assertThat(request.requests().get(2).indices()[0], equalTo("test4"));
        assertThat(request.requests().get(2).indices()[1], equalTo("test1"));
        assertThat(request.requests().get(2).types()[0], equalTo("type2"));
        assertThat(request.requests().get(2).types()[1], equalTo("type1"));
        assertThat(request.requests().get(2).routing(), equalTo("123"));
    }

    public void testResponseErrorToXContent() throws IOException {
        MultiSearchResponse response = new MultiSearchResponse(
                new MultiSearchResponse.Item[]{
                    new MultiSearchResponse.Item(null, new IllegalStateException("foobar")),
                    new MultiSearchResponse.Item(null, new IllegalStateException("baaaaaazzzz"))
        });

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        assertEquals("{\"responses\":["
                        + "{"
                        + "\"error\":{\"root_cause\":[{\"type\":\"illegal_state_exception\",\"reason\":\"foobar\"}],"
                        + "\"type\":\"illegal_state_exception\",\"reason\":\"foobar\"},\"status\":500"
                        + "},"
                        + "{"
                        + "\"error\":{\"root_cause\":[{\"type\":\"illegal_state_exception\",\"reason\":\"baaaaaazzzz\"}],"
                        + "\"type\":\"illegal_state_exception\",\"reason\":\"baaaaaazzzz\"},\"status\":500"
                        + "}"
                        + "]}",
                builder.string());
    }

    public void testMaxConcurrentSearchRequests() {
        MultiSearchRequest request = new MultiSearchRequest();
        request.maxConcurrentSearchRequests(randomIntBetween(1, Integer.MAX_VALUE));
        expectThrows(IllegalArgumentException.class, () ->
                request.maxConcurrentSearchRequests(randomIntBetween(Integer.MIN_VALUE, 0)));
    }

    private MultiSearchRequest parseMultiSearchRequest(String sample) throws IOException {
        byte[] data = StreamsUtils.copyToBytesFromClasspath(sample);
        RestRequest restRequest = new FakeRestRequest.Builder().withContent(new BytesArray(data)).build();
        return RestMultiSearchAction.parseRequest(restRequest, true, parsers(), ParseFieldMatcher.EMPTY);
    }

    private SearchRequestParsers parsers() {
        IndicesQueriesRegistry registry = new IndicesQueriesRegistry();
        QueryParser<MatchAllQueryBuilder> parser = MatchAllQueryBuilder::fromXContent;
        registry.register(parser, MatchAllQueryBuilder.NAME);
        return new SearchRequestParsers(registry, null, null);
    }
}
