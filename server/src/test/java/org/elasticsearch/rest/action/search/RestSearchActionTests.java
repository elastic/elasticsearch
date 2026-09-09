/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.rest.action.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.usage.UsageService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.greaterThan;
import static org.mockito.Mockito.mock;

public final class RestSearchActionTests extends RestActionTestCase {
    private RestSearchAction action;
    private NamedXContentRegistry searchRegistry;

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        if (searchRegistry == null) {
            searchRegistry = new NamedXContentRegistry(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents());
        }
        return searchRegistry;
    }

    @Before
    public void setUpAction() {
        action = new RestSearchAction(new UsageService().getSearchUsageHolder(), nf -> false, CrossProjectModeDecider.NOOP);
        controller().registerHandler(action);
        verifyingClient.setExecuteVerifier((actionType, request) -> mock(SearchResponse.class));
        verifyingClient.setExecuteAndReturnTaskVerifier((actionType, request) -> mock(SearchResponse.class));
    }

    /**
     * The "enable_fields_emulation" flag on search requests is a no-op but should not raise an error
     */
    public void testEnableFieldsEmulationNoErrors() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("enable_fields_emulation", "true");

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/some_index/_search")
            .withParams(params)
            .build();

        action.handleRequest(request, new FakeRestChannel(request, randomBoolean()), verifyingClient);
    }

    public void testValidateSearchRequest() {
        {
            Map<String, String> params = new HashMap<>();
            params.put("rest_total_hits_as_int", "true");

            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
                .withPath("/some_index/_search")
                .withParams(params)
                .build();

            SearchRequest searchRequest = new SearchRequest();
            searchRequest.source(new SearchSourceBuilder().trackTotalHitsUpTo(100));

            Exception ex = expectThrows(
                IllegalArgumentException.class,
                () -> RestSearchAction.validateSearchRequest(request, searchRequest)
            );
            assertEquals("[rest_total_hits_as_int] cannot be used if the tracking of total hits is not accurate, got 100", ex.getMessage());
        }
        {
            Map<String, String> params = new HashMap<>();
            params.put("search_type", randomFrom(SearchType.values()).name());

            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
                .withPath("/some_index/_search")
                .withParams(params)
                .build();

            SearchRequest searchRequest = new SearchRequest();
            KnnSearchBuilder knnSearch = new KnnSearchBuilder("vector", new float[] { 1, 1, 1 }, 10, 100, 10f, null, null);
            searchRequest.source(new SearchSourceBuilder().knnSearch(List.of(knnSearch)));

            Exception ex = expectThrows(
                IllegalArgumentException.class,
                () -> RestSearchAction.validateSearchRequest(request, searchRequest)
            );
            assertEquals(
                "cannot set [search_type] when using [knn] search, since the search type is determined automatically",
                ex.getMessage()
            );
        }
    }

    /**
     * Using an illegal search type on the request should throw an error
     */
    public void testIllegalSearchType() {
        Map<String, String> params = new HashMap<>();
        params.put("search_type", "some_search_type");

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/some_index/_search")
            .withParams(params)
            .build();

        Exception ex = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, verifyingClient));
        assertEquals("No search type for [some_search_type]", ex.getMessage());
    }

    public void testParseSuggestParameters() {
        assertNull(RestSearchAction.parseSuggestUrlParameters(new FakeRestRequest()));

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(
            Map.of("suggest_field", "field", "suggest_text", "text", "suggest_size", "3", "suggest_mode", "missing")
        ).build();
        SuggestBuilder suggestBuilder = RestSearchAction.parseSuggestUrlParameters(request);
        TermSuggestionBuilder builder = (TermSuggestionBuilder) suggestBuilder.getSuggestions().get("field");
        assertNotNull(builder);
        assertEquals("text", builder.text());
        assertEquals("field", builder.field());
        assertEquals(3, builder.size().intValue());
        assertEquals(TermSuggestionBuilder.SuggestMode.MISSING, builder.suggestMode());
    }

    public void testParseSuggestParametersError() {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(
            Map.of("suggest_text", "text", "suggest_size", "3", "suggest_mode", "missing")
        ).build();
        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> RestSearchAction.parseSuggestUrlParameters(request)
        );
        assertEquals(
            "request [/] contains parameters [suggest_text, suggest_size, suggest_mode] but missing 'suggest_field' parameter.",
            iae.getMessage()
        );
    }

    public void testParseSearchRequestWithSliceParam() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_search")
            .withParams(Map.of(SliceIndexing.PARAM_NAME, "s1,s2"))
            .build();
        SearchRequest searchRequest = new SearchRequest();
        RestSearchAction.parseSearchRequest(searchRequest, request, null, nf -> false, size -> searchRequest.source().size(size));
        assertEquals("s1,s2", searchRequest.routing());
        assertTrue(searchRequest.isRoutingFromSlice());
        assertEquals("s1,s2", searchRequest.searchSlice());
    }

    public void testParseSearchRequestWithSliceAllParam() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_search")
            .withParams(Map.of(SliceIndexing.PARAM_NAME, SliceIndexing.SLICE_ALL))
            .build();
        SearchRequest searchRequest = new SearchRequest();
        RestSearchAction.parseSearchRequest(searchRequest, request, null, nf -> false, size -> searchRequest.source().size(size));
        assertNull(searchRequest.routing());
        assertTrue(searchRequest.isRoutingFromSlice());
        assertEquals(SliceIndexing.SLICE_ALL, searchRequest.searchSlice());
    }

    public void testParseSearchRequestRejectsRoutingAndSliceTogether() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_search")
            .withParams(Map.of(SliceIndexing.PARAM_NAME, "s1", "routing", "r1"))
            .build();
        SearchRequest searchRequest = new SearchRequest();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> RestSearchAction.parseSearchRequest(searchRequest, request, null, nf -> false, size -> searchRequest.source().size(size))
        );
        assertEquals("[routing] is not allowed together with [slice]", e.getMessage());
    }

    public void testParseSearchRequestRejectsSliceWhenFeatureDisabled() {
        assumeFalse("slice indexing feature flag must be disabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_search")
            .withParams(Map.of(SliceIndexing.PARAM_NAME, "s1"))
            .build();
        SearchRequest searchRequest = new SearchRequest();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> RestSearchAction.parseSearchRequest(searchRequest, request, null, nf -> false, size -> searchRequest.source().size(size))
        );
        assertEquals("request does not support [slice]", e.getMessage());
    }

    public void testParseSearchRequestAllowsSliceWithPointInTime() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_search")
            .withParams(Map.of(SliceIndexing.PARAM_NAME, "s1"))
            .build();
        SearchRequest searchRequest = new SearchRequest().source(
            new SearchSourceBuilder().pointInTimeBuilder(new PointInTimeBuilder(BytesArray.EMPTY))
        );
        RestSearchAction.parseSearchRequest(searchRequest, request, null, nf -> false, size -> searchRequest.source().size(size));
        assertEquals("s1", searchRequest.routing());
        assertTrue(searchRequest.isRoutingFromSlice());
        assertEquals("s1", searchRequest.searchSlice());
    }

    public void testParseSearchRequestDefaultsMrtToFalseInCps() throws Exception {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_search")
            .build();
        SearchRequest searchRequest = new SearchRequest();
        RestSearchAction.parseSearchRequest(
            searchRequest,
            request,
            null,
            nf -> false,
            size -> searchRequest.source().size(size),
            null,
            Optional.of(true)
        );
        assertFalse(searchRequest.isCcsMinimizeRoundtrips());
    }

    public void testParseSearchRequestRejectsMrtTrueWithPitInCps() {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_search")
            .withParams(Map.of("ccs_minimize_roundtrips", "true"))
            .build();
        SearchRequest searchRequest = new SearchRequest().source(
            new SearchSourceBuilder().pointInTimeBuilder(new PointInTimeBuilder(BytesArray.EMPTY))
        );
        ActionRequestValidationException ex = expectThrows(
            ActionRequestValidationException.class,
            () -> RestSearchAction.parseSearchRequest(
                searchRequest,
                request,
                null,
                nf -> false,
                size -> searchRequest.source().size(size),
                null,
                Optional.of(true)
            )
        );
        assertThat(ex.getMessage(), org.hamcrest.Matchers.containsString("[ccs_minimize_roundtrips] cannot be used with point in time"));
        assertFalse(searchRequest.isCcsMinimizeRoundtrips());
    }

    public void testParseSearchRequestIgnoresMrtParamInCps() throws Exception {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_search")
            .withParams(Map.of("ccs_minimize_roundtrips", "true"))
            .build();
        SearchRequest searchRequest = new SearchRequest();
        RestSearchAction.parseSearchRequest(
            searchRequest,
            request,
            null,
            nf -> false,
            size -> searchRequest.source().size(size),
            null,
            Optional.of(true)
        );
        assertFalse(searchRequest.isCcsMinimizeRoundtrips());
        assertWarnings(SearchParamsParser.MRT_SET_IN_CPS_WARN);
    }

    /**
     * An unrecognised REST parameter causes {@link org.elasticsearch.rest.BaseRestHandler} to reject the request
     * before dispatching. The parse-time breaker charge must be released via the consumer's {@code close()}
     * (dispatched == false) path.
     */
    public void testBreakerReleasedOnUnknownRestParam() throws Exception {
        LimitedBreaker breaker = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofBytes(Long.MAX_VALUE / 2));
        AbstractQueryBuilder.setQueryParsingBreaker(breaker);
        try {
            Map<String, String> params = new HashMap<>();
            params.put("bogus_unknown_param", "true");
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
                .withPath("/_search")
                .withParams(params)
                .withContent(new BytesArray("{\"query\":{\"match_all\":{}}}"), XContentType.JSON)
                .build();
            expectThrows(
                IllegalArgumentException.class,
                () -> action.handleRequest(request, new FakeRestChannel(request, randomBoolean()), verifyingClient)
            );
            assertEquals("breaker must be fully released after unknown-param rejection", 0L, breaker.getUsed());
        } finally {
            AbstractQueryBuilder.setQueryParsingBreaker(null);
        }
    }

    /**
     * An action filter (or any component that calls {@code listener.onFailure} before
     * {@code TransportSearchAction.doExecute} runs) must not leak the parse-time breaker charge.
     * The charge is released via the {@code runAfter} wrapper installed on the completion listener.
     */
    public void testBreakerReleasedOnDispatchFailure() throws Exception {
        LimitedBreaker breaker = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofBytes(Long.MAX_VALUE / 2));
        AbstractQueryBuilder.setQueryParsingBreaker(breaker);
        try {
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
                .withPath("/_search")
                .withContent(new BytesArray("{\"query\":{\"match_all\":{}}}"), XContentType.JSON)
                .build();
            NoOpNodeClient rejectingClient = new NoOpNodeClient(verifyingClient.threadPool()) {
                @Override
                public String getLocalNodeId() {
                    return "test-node";
                }

                @Override
                public <Request extends ActionRequest, Response extends ActionResponse> Task executeAndReturnTask(
                    ActionType<Response> actionType,
                    Request req,
                    ActionListener<Response> listener
                ) {
                    listener.onFailure(new RuntimeException("simulated filter rejection"));
                    // RestCancellableNodeClient asserts the returned task is CancellableTask;
                    // SearchRequest.createTask() returns SearchTask which satisfies that.
                    return req.createTask(1L, "transport", actionType.name(), TaskId.EMPTY_TASK_ID, Collections.emptyMap());
                }
            };
            action.handleRequest(request, new FakeRestChannel(request, randomBoolean()), rejectingClient);
            assertEquals("breaker must be fully released after dispatch failure", 0L, breaker.getUsed());
        } finally {
            AbstractQueryBuilder.setQueryParsingBreaker(null);
        }
    }

    /**
     * The parse-time breaker charge must remain positive while the search is outstanding (i.e.
     * after {@code accept()} returns but before the completion listener fires) and must drop to
     * zero only once the listener fires.
     */
    public void testBreakerHeldDuringOutstandingSearch() throws Exception {
        LimitedBreaker breaker = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofBytes(Long.MAX_VALUE / 2));
        AbstractQueryBuilder.setQueryParsingBreaker(breaker);
        try {
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
                .withPath("/_search")
                .withContent(new BytesArray("{\"query\":{\"match_all\":{}}}"), XContentType.JSON)
                .build();
            // triggerCompletion.run() fires the pending listener, simulating an async search completing.
            AtomicReference<Runnable> triggerCompletion = new AtomicReference<>();
            NoOpNodeClient holdingClient = new NoOpNodeClient(verifyingClient.threadPool()) {
                @Override
                public String getLocalNodeId() {
                    return "test-node";
                }

                @Override
                public <Request extends ActionRequest, Response extends ActionResponse> Task executeAndReturnTask(
                    ActionType<Response> actionType,
                    Request req,
                    ActionListener<Response> listener
                ) {
                    // Store a callback that fires onFailure (simulating task cancellation).
                    triggerCompletion.set(() -> listener.onFailure(new RuntimeException("task cancelled mid-search")));
                    return req.createTask(2L, "transport", actionType.name(), TaskId.EMPTY_TASK_ID, Collections.emptyMap());
                }
            };
            action.handleRequest(request, new FakeRestChannel(request, randomBoolean()), holdingClient);
            assertThat("breaker must be held while search is outstanding", breaker.getUsed(), greaterThan(0L));
            triggerCompletion.get().run();
            assertEquals("breaker must be released once the search completes", 0L, breaker.getUsed());
        } finally {
            AbstractQueryBuilder.setQueryParsingBreaker(null);
        }
    }

    public void testParseSearchRequestDefaultsMrtToTrueOutsideCps() throws Exception {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_search")
            .build();
        SearchRequest searchRequest = new SearchRequest();
        RestSearchAction.parseSearchRequest(
            searchRequest,
            request,
            null,
            nf -> false,
            size -> searchRequest.source().size(size),
            null,
            Optional.of(false)
        );
        assertTrue(searchRequest.isCcsMinimizeRoundtrips());
    }

    public void testParseSearchRequestUsesProvidedMrtOutsideCps() throws Exception {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_search")
            .withParams(Map.of("ccs_minimize_roundtrips", "false"))
            .build();
        SearchRequest searchRequest = new SearchRequest();
        RestSearchAction.parseSearchRequest(
            searchRequest,
            request,
            null,
            nf -> false,
            size -> searchRequest.source().size(size),
            null,
            Optional.of(false)
        );
        assertFalse(searchRequest.isCcsMinimizeRoundtrips());
    }
}
