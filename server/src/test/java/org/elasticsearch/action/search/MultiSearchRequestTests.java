/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.search.RestMultiSearchAction;
import org.elasticsearch.rest.action.search.SearchParamsParser;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.StreamsUtils;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.usage.UsageService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.singletonList;
import static org.elasticsearch.search.RandomSearchRequestGenerator.randomSearchRequest;
import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class MultiSearchRequestTests extends ESTestCase {
    public void testSimpleAdd() throws Exception {
        MultiSearchRequest request = parseMultiSearchRequest("/org/elasticsearch/action/search/simple-msearch1.json");
        assertThat(request.requests().size(), equalTo(8));
        assertThat(request.requests().get(0).indices()[0], equalTo("test"));
        assertThat(
            request.requests().get(0).indicesOptions(),
            equalTo(IndicesOptions.fromOptions(true, true, true, true, SearchRequest.DEFAULT_INDICES_OPTIONS))
        );
        assertThat(request.requests().get(1).indices()[0], equalTo("test"));
        assertThat(
            request.requests().get(1).indicesOptions(),
            equalTo(IndicesOptions.fromOptions(false, true, true, true, SearchRequest.DEFAULT_INDICES_OPTIONS))
        );
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
        assertThat(request.requests().get(6).indices(), is(Strings.EMPTY_ARRAY));
        assertThat(request.requests().get(6).searchType(), equalTo(SearchType.DFS_QUERY_THEN_FETCH));
        assertThat(request.requests().get(7).indices(), is(Strings.EMPTY_ARRAY));
    }

    public void testFailWithUnknownKey() {
        final String requestContent = """
            {"index":"test", "ignore_unavailable" : true, "unknown_key" : "open,closed"}
            {"query" : {"match_all" :{}}}
            """;
        FakeRestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray(requestContent),
            XContentType.JSON
        ).build();
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> RestMultiSearchAction.parseRequest(
                restRequest,
                true,
                new UsageService().getSearchUsageHolder(),
                nf -> false,
                Optional.empty()
            )
        );
        assertEquals("key [unknown_key] is not supported in the metadata section", ex.getMessage());
    }

    public void testSimpleAddWithCarriageReturn() throws Exception {
        final String requestContent = """
            {"index":"test", "ignore_unavailable" : true, "expand_wildcards" : "open,closed"}
            {"query" : {"match_all" :{}}}
            """;
        FakeRestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray(requestContent),
            XContentType.JSON
        ).build();
        MultiSearchRequest request = RestMultiSearchAction.parseRequest(
            restRequest,
            true,
            new UsageService().getSearchUsageHolder(),
            nf -> false,
            Optional.empty()
        );
        assertThat(request.requests().size(), equalTo(1));
        assertThat(request.requests().get(0).indices()[0], equalTo("test"));
        assertThat(
            request.requests().get(0).indicesOptions(),
            equalTo(IndicesOptions.fromOptions(true, true, true, true, SearchRequest.DEFAULT_INDICES_OPTIONS))
        );
    }

    public void testDefaultIndicesOptions() throws IOException {
        final String requestContent = """
            {"index":"test", "expand_wildcards" : "open,closed"}
            {"query" : {"match_all" :{}}}
            """;
        FakeRestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray(requestContent),
            XContentType.JSON
        ).withParams(Collections.singletonMap("ignore_unavailable", "true")).build();
        MultiSearchRequest request = RestMultiSearchAction.parseRequest(
            restRequest,
            true,
            new UsageService().getSearchUsageHolder(),
            nf -> false,
            Optional.empty()
        );
        assertThat(request.requests().size(), equalTo(1));
        assertThat(request.requests().get(0).indices()[0], equalTo("test"));
        assertThat(
            request.requests().get(0).indicesOptions(),
            equalTo(IndicesOptions.fromOptions(true, true, true, true, SearchRequest.DEFAULT_INDICES_OPTIONS))
        );
    }

    public void testSimpleAdd2() throws Exception {
        MultiSearchRequest request = parseMultiSearchRequest("/org/elasticsearch/action/search/simple-msearch2.json");
        assertThat(request.requests().size(), equalTo(5));
        assertThat(request.requests().get(0).indices()[0], equalTo("test"));
        assertThat(request.requests().get(1).indices()[0], equalTo("test"));
        assertThat(request.requests().get(2).indices(), is(Strings.EMPTY_ARRAY));
        assertThat(request.requests().get(3).indices(), is(Strings.EMPTY_ARRAY));
        assertThat(request.requests().get(3).searchType(), equalTo(SearchType.DFS_QUERY_THEN_FETCH));
        assertThat(request.requests().get(4).indices(), is(Strings.EMPTY_ARRAY));
    }

    public void testSimpleAdd3() throws Exception {
        MultiSearchRequest request = parseMultiSearchRequest("/org/elasticsearch/action/search/simple-msearch3.json");
        assertThat(request.requests().size(), equalTo(4));
        assertThat(request.requests().get(0).indices()[0], equalTo("test0"));
        assertThat(request.requests().get(0).indices()[1], equalTo("test1"));
        assertThat(request.requests().get(1).indices()[0], equalTo("test2"));
        assertThat(request.requests().get(1).indices()[1], equalTo("test3"));
        assertThat(request.requests().get(2).indices()[0], equalTo("test4"));
        assertThat(request.requests().get(2).indices()[1], equalTo("test1"));
        assertThat(request.requests().get(3).indices(), is(Strings.EMPTY_ARRAY));
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
        assertThat(request.requests().get(1).requestCache(), nullValue());
        assertThat(request.requests().get(1).preference(), equalTo("_local"));
        assertThat(request.requests().get(2).indices()[0], equalTo("test4"));
        assertThat(request.requests().get(2).indices()[1], equalTo("test1"));
        assertThat(request.requests().get(2).routing(), equalTo("123"));
    }

    public void testNoMetadata() throws Exception {
        MultiSearchRequest request = parseMultiSearchRequest("/org/elasticsearch/action/search/msearch-no-metadata.json");
        assertThat(request.requests().size(), equalTo(4));
        for (SearchRequest searchRequest : request.requests()) {
            assertThat(searchRequest.indices().length, equalTo(0));
            assertThat(searchRequest.source().query(), instanceOf(MatchAllQueryBuilder.class));
        }
    }

    public void testParseRequestWithTopLevelSliceParam() throws IOException {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        MultiSearchRequest request = parseMultiSearchRequestFromStringAndParams("""
            {}
            {"query":{"match_all":{}}}
            """, Map.of(SliceIndexing.PARAM_NAME, "s1,s2"));
        assertThat(request.requests().size(), equalTo(1));
        SearchRequest searchRequest = request.requests().getFirst();
        assertThat(searchRequest.routing(), equalTo("s1,s2"));
        assertTrue(searchRequest.isRoutingFromSlice());
        assertThat(searchRequest.searchSlice(), equalTo("s1,s2"));
    }

    public void testParseRequestRejectsRoutingAndSliceInSameMetadata() throws IOException {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> parseMultiSearchRequestFromString("""
            {"routing":"r1","_slice":"s1"}
            {"query":{"match_all":{}}}
            """));
        assertEquals("[routing] and [_slice] cannot be combined in the same _msearch request", ex.getMessage());
    }

    public void testParseRequestRejectsTopLevelRoutingWithMetadataSlice() throws IOException {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> parseMultiSearchRequestFromStringAndParams("""
            {"_slice":"s1"}
            {"query":{"match_all":{}}}
            """, Map.of("routing", "r1")));
        assertEquals("[routing] and [_slice] cannot be combined in the same _msearch request", ex.getMessage());
    }

    public void testParseRequestRejectsTopLevelSliceWithMetadataRouting() throws IOException {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> parseMultiSearchRequestFromStringAndParams("""
            {"routing":"r1"}
            {"query":{"match_all":{}}}
            """, Map.of(SliceIndexing.PARAM_NAME, "s1")));
        assertEquals("[routing] and [_slice] cannot be combined in the same _msearch request", ex.getMessage());
    }

    public void testParseRequestRejectsMetadataSliceWhenFeatureDisabled() throws IOException {
        assumeFalse("slice indexing feature flag must be disabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> parseMultiSearchRequestFromString("""
            {"_slice":"s1"}
            {"query":{"match_all":{}}}
            """));
        assertEquals("request does not support [_slice]", ex.getMessage());
    }

    public void testParseRequestAllowsDifferentRoutingModesPerSubRequestWithoutTopLevel() throws IOException {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        MultiSearchRequest request = parseMultiSearchRequestFromString("""
            {"routing":"r1"}
            {"query":{"match_all":{}}}
            {"_slice":"s1"}
            {"query":{"match_all":{}}}
            """);
        assertThat(request.requests().size(), equalTo(2));
        SearchRequest routingRequest = request.requests().get(0);
        assertThat(routingRequest.routing(), equalTo("r1"));
        assertFalse(routingRequest.isRoutingFromSlice());
        assertNull(routingRequest.searchSlice());
        SearchRequest sliceRequest = request.requests().get(1);
        assertThat(sliceRequest.routing(), equalTo("s1"));
        assertTrue(sliceRequest.isRoutingFromSlice());
        assertThat(sliceRequest.searchSlice(), equalTo("s1"));
    }

    public void testParseRequestIgnoresMrtInCpsAndWarns() throws IOException {
        final String requestContent = """
            {"index":"test","ccs_minimize_roundtrips":true}
            {"query":{"match_all":{}}}
            {"index":"test2"}
            {"query":{"match_all":{}}}
            """;
        FakeRestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray(requestContent),
            XContentType.JSON
        ).withParams(Map.of("ccs_minimize_roundtrips", "true")).build();

        MultiSearchRequest request = RestMultiSearchAction.parseRequest(
            restRequest,
            true,
            new UsageService().getSearchUsageHolder(),
            nf -> false,
            Optional.of(true)
        );
        assertThat(request.requests().size(), equalTo(2));
        for (SearchRequest searchRequest : request.requests()) {
            assertFalse(searchRequest.isCcsMinimizeRoundtrips());
        }
        assertWarnings(SearchParamsParser.MRT_SET_IN_CPS_WARN);
    }

    public void testWriteSearchRequestParamsUsesSliceFieldWhenRoutingFromSlice() throws IOException {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        SearchRequest request = new SearchRequest();
        request.routing("s1");
        request.searchSlice("s1");
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            MultiSearchRequest.writeSearchRequestParams(request, builder);
            Map<String, Object> map = XContentHelper.convertToMap(
                XContentType.JSON.xContent(),
                BytesReference.bytes(builder).streamInput(),
                false
            );
            assertThat(map.get(SliceIndexing.PARAM_NAME), equalTo("s1"));
            assertNull(map.get("routing"));
        }
    }

    public void testResponseErrorToXContent() throws IOException {
        long tookInMillis = randomIntBetween(1, 1000);
        MultiSearchResponse response = new MultiSearchResponse(
            new MultiSearchResponse.Item[] {
                new MultiSearchResponse.Item(null, new IllegalStateException("foobar")),
                new MultiSearchResponse.Item(null, new IllegalStateException("baaaaaazzzz")) },
            tookInMillis
        );
        try {

            assertEquals(XContentHelper.stripWhitespace(Strings.format("""
                {
                  "took": %s,
                  "responses": [
                    {
                      "error": {
                        "root_cause": [ { "type": "illegal_state_exception", "reason": "foobar" } ],
                        "type": "illegal_state_exception",
                        "reason": "foobar"
                      },
                      "status": 500
                    },
                    {
                      "error": {
                        "root_cause": [ { "type": "illegal_state_exception", "reason": "baaaaaazzzz" } ],
                        "type": "illegal_state_exception",
                        "reason": "baaaaaazzzz"
                      },
                      "status": 500
                    }
                  ]
                }""", tookInMillis)), Strings.toTruncatedString(response));
        } finally {
            response.decRef();
        }
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
            () -> RestMultiSearchAction.parseRequest(
                restRequest,
                true,
                new UsageService().getSearchUsageHolder(),
                nf -> false,
                Optional.empty()
            )
        );
        assertEquals("The msearch request must be terminated by a newline [\n]", expectThrows.getMessage());

        String mserchActionWithNewLine = mserchAction + "\n";
        RestRequest restRequestWithNewLine = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray(mserchActionWithNewLine.getBytes(StandardCharsets.UTF_8)),
            XContentType.JSON
        ).build();
        MultiSearchRequest msearchRequest = RestMultiSearchAction.parseRequest(
            restRequestWithNewLine,
            true,
            new UsageService().getSearchUsageHolder(),
            nf -> false,
            Optional.empty()
        );
        assertEquals(3, msearchRequest.requests().size());
    }

    private MultiSearchRequest parseMultiSearchRequestFromString(String request) throws IOException {
        return parseMultiSearchRequest(createRestRequest(request.getBytes(StandardCharsets.UTF_8)));
    }

    private MultiSearchRequest parseMultiSearchRequestFromStringAndParams(String request, Map<String, String> params) throws IOException {
        return parseMultiSearchRequest(createRestRequest(request.getBytes(StandardCharsets.UTF_8), params));
    }

    private MultiSearchRequest parseMultiSearchRequest(String sample) throws IOException {
        return parseMultiSearchRequest(createRestRequest(sample));
    }

    private MultiSearchRequest parseMultiSearchRequest(RestRequest restRequest) throws IOException {

        MultiSearchRequest request = new MultiSearchRequest();
        RestMultiSearchAction.parseMultiLineRequest(restRequest, SearchRequest.DEFAULT_INDICES_OPTIONS, true, (searchRequest, parser) -> {
            searchRequest.source(
                new SearchSourceBuilder().parseXContent(parser, false, new UsageService().getSearchUsageHolder(), nf -> false)
            );
            request.add(searchRequest);
        }, Optional.empty());
        return request;
    }

    private RestRequest createRestRequest(String sample) throws IOException {
        byte[] data = StreamsUtils.copyToBytesFromClasspath(sample);
        return createRestRequest(data);
    }

    private FakeRestRequest createRestRequest(byte[] data) {
        return new FakeRestRequest.Builder(xContentRegistry()).withContent(new BytesArray(data), XContentType.JSON).build();
    }

    private FakeRestRequest createRestRequest(byte[] data, Map<String, String> params) {
        return new FakeRestRequest.Builder(xContentRegistry()).withContent(new BytesArray(data), XContentType.JSON)
            .withParams(params)
            .build();
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
                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().parseXContent(
                    p,
                    false,
                    new UsageService().getSearchUsageHolder(),
                    nf -> false
                );
                if (searchSourceBuilder.equals(new SearchSourceBuilder()) == false) {
                    r.source(searchSourceBuilder);
                }
                parsedRequest.add(r);
            };
            MultiSearchRequest.readMultiLineFormat(
                xContentType.xContent(),
                parserConfig(),
                new BytesArray(originalBytes),
                consumer,
                null,
                null,
                null,
                null,
                null,
                true,
                Optional.empty(),
                null
            );
            assertEquals(originalRequest, parsedRequest);
        }
    }

    public void testMultiLineSerializationPreservesSliceProvenance() throws IOException {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        MultiSearchRequest originalRequest = new MultiSearchRequest();
        originalRequest.add(new SearchRequest("index-1").routing("s1").searchSlice("s1"));
        originalRequest.add(new SearchRequest("index-2").searchSlice(SliceIndexing.SLICE_ALL));

        byte[] bytes = MultiSearchRequest.writeMultiLineFormat(originalRequest, XContentType.JSON.xContent());
        MultiSearchRequest parsedRequest = new MultiSearchRequest();
        CheckedBiConsumer<SearchRequest, XContentParser, IOException> consumer = (r, p) -> {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().parseXContent(
                p,
                false,
                new UsageService().getSearchUsageHolder(),
                nf -> false
            );
            if (searchSourceBuilder.equals(new SearchSourceBuilder()) == false) {
                r.source(searchSourceBuilder);
            }
            parsedRequest.add(r);
        };
        MultiSearchRequest.readMultiLineFormat(
            XContentType.JSON.xContent(),
            parserConfig(),
            new BytesArray(bytes),
            consumer,
            null,
            null,
            null,
            null,
            null,
            true,
            Optional.empty(),
            null
        );
        assertThat(parsedRequest.requests().size(), equalTo(2));
        SearchRequest parsedSlice = parsedRequest.requests().get(0);
        assertThat(parsedSlice.routing(), equalTo("s1"));
        assertTrue(parsedSlice.isRoutingFromSlice());
        assertThat(parsedSlice.searchSlice(), equalTo("s1"));
        SearchRequest parsedAll = parsedRequest.requests().get(1);
        assertNull(parsedAll.routing());
        assertTrue(parsedAll.isRoutingFromSlice());
        assertThat(parsedAll.searchSlice(), equalTo(SliceIndexing.SLICE_ALL));
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

    public void testTaskDescription() {
        MultiSearchRequest request = new MultiSearchRequest();
        request.add(new SearchRequest().preference("abc"));
        request.add(new SearchRequest().routing("r").preference("xyz"));
        request.add(new SearchRequest().indices("index-1"));

        String description = request.createTask(0, "type", "action", TaskId.EMPTY_TASK_ID, Map.of()).getDescription();
        assertThat(
            description,
            equalTo(
                "requests[3]: "
                    + "indices[], search_type[QUERY_THEN_FETCH], source[], preference[abc] | "
                    + "indices[], search_type[QUERY_THEN_FETCH], source[], routing[r], preference[xyz] | "
                    + "indices[index-1], search_type[QUERY_THEN_FETCH], source[]"
            )
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

    public void testFailOnExtraCharacters() throws IOException {
        try {
            parseMultiSearchRequestFromString("""
                {"index": "test"}{{{{{extra chars that shouldn't be here
                { "query": {"match_all": {}}}
                """);
            fail("should have caught first line; extra open brackets");
        } catch (XContentParseException e) {
            assertEquals("[1:18] Unexpected token after end of object", e.getMessage());
        }
        try {
            parseMultiSearchRequestFromString("""
                {"index": "test"}
                { "query": {"match_all": {}}}{{{{even more chars
                """);
            fail("should have caught second line");
        } catch (XContentParseException e) {
            assertEquals("[1:30] Unexpected token after end of object", e.getMessage());
        }
        try {
            parseMultiSearchRequestFromString("""
                {}
                { "query": {"match_all": {}}}}}}different error message
                """);
            fail("should have caught second line; extra closing brackets");
        } catch (XContentParseException e) {
            assertThat(
                e.getMessage(),
                containsString(
                    "Unexpected close marker '}': expected ']' (for root starting at "
                        + "[Source: (byte[])\"{ \"query\": {\"match_all\": {}}}}}}different error message\""
                )
            );
        }
    }

    public void testNullIndex() throws IOException {
        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class, () -> parseMultiSearchRequestFromString("""
            {"index": null}
            { "query": {"match_all": {}}}
            """));
        assertThat(e.getMessage(), containsString("Expected a list of strings but got null"));
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
            searchRequest.scroll(null);

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
            searchRequest.setForceSyntheticSource(false);

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
