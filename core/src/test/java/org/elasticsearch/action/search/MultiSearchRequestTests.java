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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.search.RestMultiSearchAction;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.StreamsUtils;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static java.util.Collections.singletonList;
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

    public void testSimpleAddWithCarriageReturn() throws Exception {
        final String requestContent = "{\"index\":\"test\", \"ignore_unavailable\" : true, \"expand_wildcards\" : \"open,closed\"}}\r\n" +
            "{\"query\" : {\"match_all\" :{}}}\r\n";
        FakeRestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry())
            .withContent(new BytesArray(requestContent), XContentType.JSON).build();
        MultiSearchRequest request = RestMultiSearchAction.parseRequest(restRequest, true);
        assertThat(request.requests().size(), equalTo(1));
        assertThat(request.requests().get(0).indices()[0], equalTo("test"));
        assertThat(request.requests().get(0).indicesOptions(),
            equalTo(IndicesOptions.fromOptions(true, true, true, true, IndicesOptions.strictExpandOpenAndForbidClosed())));
        assertThat(request.requests().get(0).types().length, equalTo(0));
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
                Strings.toString(response));
    }

    public void testMaxConcurrentSearchRequests() {
        MultiSearchRequest request = new MultiSearchRequest();
        request.maxConcurrentSearchRequests(randomIntBetween(1, Integer.MAX_VALUE));
        expectThrows(IllegalArgumentException.class, () ->
                request.maxConcurrentSearchRequests(randomIntBetween(Integer.MIN_VALUE, 0)));
    }

    public void testMsearchTerminatedByNewline() throws Exception {
        String mserchAction = StreamsUtils.copyToStringFromClasspath("/org/elasticsearch/action/search/simple-msearch5.json");
        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry())
                .withContent(new BytesArray(mserchAction.getBytes(StandardCharsets.UTF_8)), XContentType.JSON).build();
        IllegalArgumentException expectThrows = expectThrows(IllegalArgumentException.class,
                () -> RestMultiSearchAction.parseRequest(restRequest, true));
        assertEquals("The msearch request must be terminated by a newline [\n]", expectThrows.getMessage());

        String mserchActionWithNewLine = mserchAction + "\n";
        RestRequest restRequestWithNewLine = new FakeRestRequest.Builder(xContentRegistry())
                .withContent(new BytesArray(mserchActionWithNewLine.getBytes(StandardCharsets.UTF_8)), XContentType.JSON).build();
        MultiSearchRequest msearchRequest = RestMultiSearchAction.parseRequest(restRequestWithNewLine, true);
        assertEquals(3, msearchRequest.requests().size());
    }

    private MultiSearchRequest parseMultiSearchRequest(String sample) throws IOException {
        byte[] data = StreamsUtils.copyToBytesFromClasspath(sample);
        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry())
            .withContent(new BytesArray(data), XContentType.JSON).build();
        return RestMultiSearchAction.parseRequest(restRequest, true);
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(singletonList(new NamedXContentRegistry.Entry(QueryBuilder.class,
                new ParseField(MatchAllQueryBuilder.NAME), (p, c) -> MatchAllQueryBuilder.fromXContent(p))));
    }
}
