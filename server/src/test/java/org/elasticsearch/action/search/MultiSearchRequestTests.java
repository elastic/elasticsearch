/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.search.RestMultiSearchAction;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.StreamsUtils;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.elasticsearch.search.RandomSearchRequestGenerator.randomSearchRequest;
import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class MultiSearchRequestTests extends ESTestCase {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(MultiSearchRequestTests.class);

    public void testSimpleAdd() throws Exception {
        MultiSearchRequest request = parseMultiSearchRequestFromFile("/org/elasticsearch/action/search/simple-msearch1.json");
        assertThat(request.requests().size(), equalTo(8));
        assertThat(request.requests().get(0).indices()[0], equalTo("test"));
        assertThat(
            request.requests().get(0).indicesOptions(),
            equalTo(IndicesOptions.fromOptions(true, true, true, true, SearchRequest.DEFAULT_INDICES_OPTIONS))
        );
        assertThat(request.requests().get(0).types().length, equalTo(0));
        assertThat(request.requests().get(1).indices()[0], equalTo("test"));
        assertThat(
            request.requests().get(1).indicesOptions(),
            equalTo(IndicesOptions.fromOptions(false, true, true, true, SearchRequest.DEFAULT_INDICES_OPTIONS))
        );
        assertThat(request.requests().get(1).types()[0], equalTo("type1"));
        assertThat(request.requests().get(2).indices()[0], equalTo("test"));
        assertThat(
            request.requests().get(2).indicesOptions(),
            equalTo(IndicesOptions.fromOptions(false, true, true, false, SearchRequest.DEFAULT_INDICES_OPTIONS))
        );
        assertThat(request.requests().get(3).indices()[0], equalTo("test"));
        assertThat(
            request.requests().get(3).indicesOptions(),
            equalTo(IndicesOptions.fromOptions(true, true, true, true, SearchRequest.DEFAULT_INDICES_OPTIONS))
        );
        assertThat(request.requests().get(4).indices()[0], equalTo("test"));
        assertThat(
            request.requests().get(4).indicesOptions(),
            equalTo(IndicesOptions.fromOptions(true, false, false, true, SearchRequest.DEFAULT_INDICES_OPTIONS))
        );

        assertThat(request.requests().get(5).indices(), is(Strings.EMPTY_ARRAY));
        assertThat(request.requests().get(5).types().length, equalTo(0));
        assertThat(request.requests().get(6).indices(), is(Strings.EMPTY_ARRAY));
        assertThat(request.requests().get(6).types().length, equalTo(0));
        assertThat(request.requests().get(6).searchType(), equalTo(SearchType.DFS_QUERY_THEN_FETCH));
        assertThat(request.requests().get(7).indices(), is(Strings.EMPTY_ARRAY));
        assertThat(request.requests().get(7).types().length, equalTo(0));
    }

    public void testFailWithUnknownKey() {
        final String requestContent = "{\"index\":\"test\", \"ignore_unavailable\" : true, \"unknown_key\" : \"open,closed\"}}\r\n"
            + "{\"query\" : {\"match_all\" :{}}}\r\n";
        FakeRestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray(requestContent),
            XContentType.JSON
        ).build();
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> RestMultiSearchAction.parseRequest(restRequest, null, true)
        );
        assertEquals("key [unknown_key] is not supported in the metadata section", ex.getMessage());
    }

    public void testSimpleAddWithCarriageReturn() throws Exception {
        final String requestContent = "{\"index\":\"test\", \"ignore_unavailable\" : true, \"expand_wildcards\" : \"open,closed\"}}\r\n"
            + "{\"query\" : {\"match_all\" :{}}}\r\n";
        FakeRestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray(requestContent),
            XContentType.JSON
        ).build();
        MultiSearchRequest request = RestMultiSearchAction.parseRequest(restRequest, null, true);
        assertThat(request.requests().size(), equalTo(1));
        assertThat(request.requests().get(0).indices()[0], equalTo("test"));
        assertThat(
            request.requests().get(0).indicesOptions(),
            equalTo(IndicesOptions.fromOptions(true, true, true, true, SearchRequest.DEFAULT_INDICES_OPTIONS))
        );
        assertThat(request.requests().get(0).types().length, equalTo(0));
    }

    public void testDefaultIndicesOptions() throws IOException {
        final String requestContent = "{\"index\":\"test\", \"expand_wildcards\" : \"open,closed\"}}\r\n"
            + "{\"query\" : {\"match_all\" :{}}}\r\n";
        FakeRestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray(requestContent),
            XContentType.JSON
        ).withParams(Collections.singletonMap("ignore_unavailable", "true")).build();
        MultiSearchRequest request = RestMultiSearchAction.parseRequest(restRequest, null, true);
        assertThat(request.requests().size(), equalTo(1));
        assertThat(request.requests().get(0).indices()[0], equalTo("test"));
        assertThat(
            request.requests().get(0).indicesOptions(),
            equalTo(IndicesOptions.fromOptions(true, true, true, true, SearchRequest.DEFAULT_INDICES_OPTIONS))
        );
        assertThat(request.requests().get(0).types().length, equalTo(0));
    }

    public void testSimpleAdd2() throws Exception {
        MultiSearchRequest request = parseMultiSearchRequestFromFile("/org/elasticsearch/action/search/simple-msearch2.json");
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
        MultiSearchRequest request = parseMultiSearchRequestFromFile("/org/elasticsearch/action/search/simple-msearch3.json");
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
        MultiSearchRequest request = parseMultiSearchRequestFromFile("/org/elasticsearch/action/search/simple-msearch4.json");
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

    public void testEmptyFirstLine1() throws Exception {
        MultiSearchRequest request = parseMultiSearchRequestFromString(
            "\n"
                + "\n"
                + "{ \"query\": {\"match_all\": {}}}\n"
                + "{}\n"
                + "{ \"query\": {\"match_all\": {}}}\n"
                + "\n"
                + "{ \"query\": {\"match_all\": {}}}\n"
                + "{}\n"
                + "{ \"query\": {\"match_all\": {}}}\n"
        );
        assertThat(request.requests().size(), equalTo(4));
        for (SearchRequest searchRequest : request.requests()) {
            assertThat(searchRequest.indices().length, equalTo(0));
            assertThat(searchRequest.source().query(), instanceOf(MatchAllQueryBuilder.class));
        }
        assertWarnings(
            "support for empty first line before any action metadata in msearch API is deprecated and will be removed "
                + "in the next major version"
        );
    }

    public void testEmptyFirstLine2() throws Exception {
        MultiSearchRequest request = parseMultiSearchRequestFromString(
            "\n"
                + "{}\n"
                + "{ \"query\": {\"match_all\": {}}}\n"
                + "\n"
                + "{ \"query\": {\"match_all\": {}}}\n"
                + "{}\n"
                + "{ \"query\": {\"match_all\": {}}}\n"
                + "\n"
                + "{ \"query\": {\"match_all\": {}}}\n"
        );
        assertThat(request.requests().size(), equalTo(4));
        for (SearchRequest searchRequest : request.requests()) {
            assertThat(searchRequest.indices().length, equalTo(0));
            assertThat(searchRequest.source().query(), instanceOf(MatchAllQueryBuilder.class));
        }
        assertWarnings(
            "support for empty first line before any action metadata in msearch API is deprecated and will be removed "
                + "in the next major version"
        );
    }

    public void testResponseErrorToXContent() {
        long tookInMillis = randomIntBetween(1, 1000);
        MultiSearchResponse response = new MultiSearchResponse(
            new MultiSearchResponse.Item[] {
                new MultiSearchResponse.Item(null, new IllegalStateException("foobar")),
                new MultiSearchResponse.Item(null, new IllegalStateException("baaaaaazzzz")) },
            tookInMillis
        );

        assertEquals(
            "{\"took\":"
                + tookInMillis
                + ",\"responses\":["
                + "{"
                + "\"error\":{\"root_cause\":[{\"type\":\"illegal_state_exception\",\"reason\":\"foobar\"}],"
                + "\"type\":\"illegal_state_exception\",\"reason\":\"foobar\"},\"status\":500"
                + "},"
                + "{"
                + "\"error\":{\"root_cause\":[{\"type\":\"illegal_state_exception\",\"reason\":\"baaaaaazzzz\"}],"
                + "\"type\":\"illegal_state_exception\",\"reason\":\"baaaaaazzzz\"},\"status\":500"
                + "}"
                + "]}",
            Strings.toString(response)
        );
    }

    public void testMaxConcurrentSearchRequests() {
        MultiSearchRequest request = new MultiSearchRequest();
        request.maxConcurrentSearchRequests(randomIntBetween(1, Integer.MAX_VALUE));
        expectThrows(IllegalArgumentException.class, () -> request.maxConcurrentSearchRequests(randomIntBetween(Integer.MIN_VALUE, 0)));
    }

    public void testMsearchTerminatedByNewline() throws Exception {
        String mserchAction = StreamsUtils.copyToStringFromClasspath("/org/elasticsearch/action/search/simple-msearch5.json");
        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray(mserchAction.getBytes(StandardCharsets.UTF_8)),
            XContentType.JSON
        ).build();
        IllegalArgumentException expectThrows = expectThrows(
            IllegalArgumentException.class,
            () -> RestMultiSearchAction.parseRequest(restRequest, null, true)
        );
        assertEquals("The msearch request must be terminated by a newline [\n]", expectThrows.getMessage());

        String mserchActionWithNewLine = mserchAction + "\n";
        RestRequest restRequestWithNewLine = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray(mserchActionWithNewLine.getBytes(StandardCharsets.UTF_8)),
            XContentType.JSON
        ).build();
        MultiSearchRequest msearchRequest = RestMultiSearchAction.parseRequest(restRequestWithNewLine, null, true);
        assertEquals(3, msearchRequest.requests().size());
    }

    private MultiSearchRequest parseMultiSearchRequestFromString(String request) throws IOException {
        return parseMultiSearchRequest(
            new FakeRestRequest.Builder(xContentRegistry()).withContent(new BytesArray(request), XContentType.JSON).build()
        );
    }

    private MultiSearchRequest parseMultiSearchRequestFromFile(String sample) throws IOException {
        byte[] data = StreamsUtils.copyToBytesFromClasspath(sample);
        return parseMultiSearchRequest(
            new FakeRestRequest.Builder(xContentRegistry()).withContent(new BytesArray(data), XContentType.JSON).build()
        );

    }

    private MultiSearchRequest parseMultiSearchRequest(RestRequest restRequest) throws IOException {
        MultiSearchRequest request = new MultiSearchRequest();
        RestMultiSearchAction.parseMultiLineRequest(restRequest, SearchRequest.DEFAULT_INDICES_OPTIONS, true, (searchRequest, parser) -> {
            searchRequest.source(SearchSourceBuilder.fromXContent(parser, false));
            request.add(searchRequest);
        });
        return request;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(
            singletonList(
                new NamedXContentRegistry.Entry(
                    QueryBuilder.class,
                    new ParseField(MatchAllQueryBuilder.NAME),
                    (p, c) -> MatchAllQueryBuilder.fromXContent(p)
                )
            )
        );
    }

    public void testMultiLineSerialization() throws IOException {
        int iters = 16;
        for (int i = 0; i < iters; i++) {
            // The only formats that support stream separator
            XContentType xContentType = randomFrom(XContentType.JSON, XContentType.SMILE);
            MultiSearchRequest originalRequest = createMultiSearchRequest();

            byte[] originalBytes = MultiSearchRequest.writeMultiLineFormat(originalRequest, xContentType.xContent());
            MultiSearchRequest parsedRequest = new MultiSearchRequest();
            CheckedBiConsumer<SearchRequest, XContentParser, IOException> consumer = (r, p) -> {
                SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(p, false);
                if (searchSourceBuilder.equals(new SearchSourceBuilder()) == false) {
                    r.source(searchSourceBuilder);
                }
                parsedRequest.add(r);
            };
            MultiSearchRequest.readMultiLineFormat(
                new BytesArray(originalBytes),
                xContentType.xContent(),
                consumer,
                null,
                null,
                null,
                null,
                null,
                null,
                xContentRegistry(),
                true,
                deprecationLogger
            );
            assertEquals(originalRequest, parsedRequest);
        }
    }

    public void testWritingExpandWildcards() throws IOException {
        assertExpandWildcardsValue(
            IndicesOptions.fromOptions(
                randomBoolean(),
                randomBoolean(),
                true,
                true,
                true,
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                randomBoolean()
            ),
            "all"
        );
        assertExpandWildcardsValue(
            IndicesOptions.fromOptions(
                randomBoolean(),
                randomBoolean(),
                true,
                true,
                false,
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                randomBoolean()
            ),
            "open,closed"
        );
        assertExpandWildcardsValue(
            IndicesOptions.fromOptions(
                randomBoolean(),
                randomBoolean(),
                true,
                false,
                true,
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                randomBoolean()
            ),
            "open,hidden"
        );
        assertExpandWildcardsValue(
            IndicesOptions.fromOptions(
                randomBoolean(),
                randomBoolean(),
                true,
                false,
                false,
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                randomBoolean()
            ),
            "open"
        );
        assertExpandWildcardsValue(
            IndicesOptions.fromOptions(
                randomBoolean(),
                randomBoolean(),
                false,
                true,
                true,
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                randomBoolean()
            ),
            "closed,hidden"
        );
        assertExpandWildcardsValue(
            IndicesOptions.fromOptions(
                randomBoolean(),
                randomBoolean(),
                false,
                true,
                false,
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                randomBoolean()
            ),
            "closed"
        );
        assertExpandWildcardsValue(
            IndicesOptions.fromOptions(
                randomBoolean(),
                randomBoolean(),
                false,
                false,
                true,
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                randomBoolean()
            ),
            "hidden"
        );
        assertExpandWildcardsValue(
            IndicesOptions.fromOptions(
                randomBoolean(),
                randomBoolean(),
                false,
                false,
                false,
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                randomBoolean()
            ),
            "none"
        );
    }

    private void assertExpandWildcardsValue(IndicesOptions options, String expectedValue) throws IOException {
        SearchRequest request = new SearchRequest();
        request.indicesOptions(options);
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            MultiSearchRequest.writeSearchRequestParams(request, builder);
            Map<String, Object> map = XContentHelper.convertToMap(
                XContentType.JSON.xContent(),
                BytesReference.bytes(builder).streamInput(),
                false
            );
            if (options.equals(SearchRequest.DEFAULT_INDICES_OPTIONS) == false) {
                final String value = (String) map.get("expand_wildcards");
                assertEquals(expectedValue, value);
            } else {
                assertNull(map.get("expand_wildcards"));
            }
        }
    }

    public void testEqualsAndHashcode() {
        checkEqualsAndHashCode(createMultiSearchRequest(), MultiSearchRequestTests::copyRequest, MultiSearchRequestTests::mutate);
    }

    public void testTaskDescription() {
        MultiSearchRequest request = new MultiSearchRequest();
        request.add(new SearchRequest().preference("abc"));
        request.add(new SearchRequest().routing("r").preference("xyz"));
        request.add(new SearchRequest().indices("index-1"));

        String description = request.createTask(0, "type", "action", TaskId.EMPTY_TASK_ID, Collections.emptyMap()).getDescription();
        assertThat(
            description,
            equalTo(
                "requests[3]: "
                    + "indices[], types[], search_type[QUERY_THEN_FETCH], source[], preference[abc] | "
                    + "indices[], types[], search_type[QUERY_THEN_FETCH], source[], routing[r], preference[xyz] | "
                    + "indices[index-1], types[], search_type[QUERY_THEN_FETCH], source[]"
            )
        );
    }

    private static MultiSearchRequest mutate(MultiSearchRequest searchRequest) throws IOException {
        MultiSearchRequest mutation = copyRequest(searchRequest);
        List<CheckedRunnable<IOException>> mutators = new ArrayList<>();
        mutators.add(
            () -> mutation.indicesOptions(
                randomValueOtherThan(
                    searchRequest.indicesOptions(),
                    () -> IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean())
                )
            )
        );
        mutators.add(() -> mutation.maxConcurrentSearchRequests(randomIntBetween(1, 32)));
        mutators.add(() -> mutation.add(createSimpleSearchRequest()));
        randomFrom(mutators).run();
        return mutation;
    }

    private static MultiSearchRequest copyRequest(MultiSearchRequest request) {
        MultiSearchRequest copy = new MultiSearchRequest();
        if (request.maxConcurrentSearchRequests() > 0) {
            copy.maxConcurrentSearchRequests(request.maxConcurrentSearchRequests());
        }
        copy.indicesOptions(request.indicesOptions());
        for (SearchRequest searchRequest : request.requests()) {
            copy.add(searchRequest);
        }
        return copy;
    }

    private static MultiSearchRequest createMultiSearchRequest() {
        int numSearchRequest = randomIntBetween(1, 128);
        MultiSearchRequest request = new MultiSearchRequest();
        for (int j = 0; j < numSearchRequest; j++) {
            SearchRequest searchRequest = createSimpleSearchRequest();

            if (randomBoolean()) {
                searchRequest.allowPartialSearchResults(true);
            }

            // scroll is not supported in the current msearch api, so unset it:
            searchRequest.scroll((Scroll) null);

            // only expand_wildcards, ignore_unavailable and allow_no_indices can be specified from msearch api, so unset other options:
            IndicesOptions randomlyGenerated = searchRequest.indicesOptions();
            IndicesOptions msearchDefault = SearchRequest.DEFAULT_INDICES_OPTIONS;
            searchRequest.indicesOptions(
                IndicesOptions.fromOptions(
                    randomlyGenerated.ignoreUnavailable(),
                    randomlyGenerated.allowNoIndices(),
                    randomlyGenerated.expandWildcardsOpen(),
                    randomlyGenerated.expandWildcardsClosed(),
                    msearchDefault.expandWildcardsHidden(),
                    msearchDefault.allowAliasesToMultipleIndices(),
                    msearchDefault.forbidClosedIndices(),
                    msearchDefault.ignoreAliases(),
                    msearchDefault.ignoreThrottled()
                )
            );

            request.add(searchRequest);
        }
        return request;
    }

    private static SearchRequest createSimpleSearchRequest() {
        return randomSearchRequest(() -> {
            // No need to return a very complex SearchSourceBuilder here, that is tested elsewhere
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.from(randomInt(10));
            searchSourceBuilder.size(randomIntBetween(20, 100));
            return searchSourceBuilder;
        });
    }

}
