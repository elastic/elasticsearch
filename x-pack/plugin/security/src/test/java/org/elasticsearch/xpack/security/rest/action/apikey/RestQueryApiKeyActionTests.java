/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.apikey;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.PrefixQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.AbstractRestChannel;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.searchafter.SearchAfterBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.DefaultBuiltInExecutorBuilders;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKey;
import org.elasticsearch.xpack.core.security.action.apikey.QueryApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.QueryApiKeyResponse;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class RestQueryApiKeyActionTests extends ESTestCase {

    private final XPackLicenseState mockLicenseState = mock(XPackLicenseState.class);
    private Settings settings;
    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        settings = Settings.builder()
            .put("path.home", createTempDir().toString())
            .put("node.name", "test-" + getTestName())
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
        threadPool = new ThreadPool(settings, MeterRegistry.NOOP, new DefaultBuiltInExecutorBuilders());
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        terminate(threadPool);
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        final SearchModule searchModule = new SearchModule(Settings.EMPTY, List.of());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    public void testQueryParsing() throws Exception {
        final String query1 = """
            {
              "query": {
                "bool": {
                  "must": [
                    {
                      "terms": {
                        "name": [ "k1", "k2" ]
                      }
                    }
                  ],
                  "should": [ { "prefix": { "metadata.environ": "prod" } } ]
                }
              }
            }""";
        final FakeRestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray(query1),
            XContentType.JSON
        ).build();

        final SetOnce<RestResponse> responseSetOnce = new SetOnce<>();
        final RestChannel restChannel = new AbstractRestChannel(restRequest, randomBoolean()) {
            @Override
            public void sendResponse(RestResponse restResponse) {
                responseSetOnce.set(restResponse);
            }
        };

        final var client = new NodeClient(Settings.EMPTY, threadPool, TestProjectResolvers.mustExecuteFirst()) {
            @SuppressWarnings("unchecked")
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                QueryApiKeyRequest queryApiKeyRequest = (QueryApiKeyRequest) request;
                final QueryBuilder queryBuilder = queryApiKeyRequest.getQueryBuilder();
                assertNotNull(queryBuilder);
                assertThat(queryBuilder.getClass(), is(BoolQueryBuilder.class));
                final BoolQueryBuilder boolQueryBuilder = (BoolQueryBuilder) queryBuilder;
                assertTrue(boolQueryBuilder.filter().isEmpty());
                assertTrue(boolQueryBuilder.mustNot().isEmpty());
                assertThat(boolQueryBuilder.must(), hasSize(1));
                final QueryBuilder mustQueryBuilder = boolQueryBuilder.must().get(0);
                assertThat(mustQueryBuilder.getClass(), is(TermsQueryBuilder.class));
                assertThat(((TermsQueryBuilder) mustQueryBuilder).fieldName(), equalTo("name"));
                assertThat(boolQueryBuilder.should(), hasSize(1));
                final QueryBuilder shouldQueryBuilder = boolQueryBuilder.should().get(0);
                assertThat(shouldQueryBuilder.getClass(), is(PrefixQueryBuilder.class));
                assertThat(((PrefixQueryBuilder) shouldQueryBuilder).fieldName(), equalTo("metadata.environ"));
                listener.onResponse((Response) QueryApiKeyResponse.EMPTY);
            }
        };
        final RestQueryApiKeyAction restQueryApiKeyAction = new RestQueryApiKeyAction(Settings.EMPTY, mockLicenseState);
        restQueryApiKeyAction.handleRequest(restRequest, restChannel, client);

        assertNotNull(responseSetOnce.get());
    }

    public void testAggsAndAggregationsTogether() {
        String agg1;
        String agg2;
        if (randomBoolean()) {
            agg1 = "aggs";
            agg2 = "aggregations";
        } else {
            agg1 = "aggregations";
            agg2 = "aggs";
        }
        final String requestBody = Strings.format("""
            {
              "%s": {
                "all_keys_by_type": {
                  "composite": {
                    "sources": [
                      { "type": { "terms": { "field": "type" } } }
                    ]
                  }
                }
              },
              "%s": {
                "type_cardinality": {
                  "cardinality": {
                    "field": "type"
                  }
                }
              }
            }""", agg1, agg2);

        final FakeRestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray(requestBody),
            XContentType.JSON
        ).build();
        final SetOnce<RestResponse> responseSetOnce = new SetOnce<>();
        final RestChannel restChannel = new AbstractRestChannel(restRequest, randomBoolean()) {
            @Override
            public void sendResponse(RestResponse restResponse) {
                responseSetOnce.set(restResponse);
            }
        };
        final var client = new NodeClient(Settings.EMPTY, threadPool, TestProjectResolvers.mustExecuteFirst()) {
            @SuppressWarnings("unchecked")
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                fail("TEST failed, request parsing should've failed");
                listener.onResponse((Response) QueryApiKeyResponse.EMPTY);
            }
        };
        RestQueryApiKeyAction restQueryApiKeyAction = new RestQueryApiKeyAction(Settings.EMPTY, mockLicenseState);
        XContentParseException ex = expectThrows(
            XContentParseException.class,
            () -> restQueryApiKeyAction.handleRequest(restRequest, restChannel, client)
        );
        assertThat(ex.getCause().getMessage(), containsString("Duplicate 'aggs' or 'aggregations' field"));
        assertThat(ex.getMessage(), containsString("Failed to build [query_api_key_request_payload]"));
        assertNull(responseSetOnce.get());
    }

    public void testParsingSearchParameters() throws Exception {
        final String requestBody = """
            {
              "query": {
                "match_all": {}
              },
              "from": 42,
              "size": 20,
              "sort": [ "name", { "creation_time": { "order": "desc", "format": "strict_date_time" } }, "username" ],
              "search_after": [ "key-2048", "2021-07-01T00:00:59.000Z" ]
            }""";

        final FakeRestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray(requestBody),
            XContentType.JSON
        ).build();

        final SetOnce<RestResponse> responseSetOnce = new SetOnce<>();
        final RestChannel restChannel = new AbstractRestChannel(restRequest, randomBoolean()) {
            @Override
            public void sendResponse(RestResponse restResponse) {
                responseSetOnce.set(restResponse);
            }
        };

        final var client = new NodeClient(Settings.EMPTY, threadPool, TestProjectResolvers.mustExecuteFirst()) {
            @SuppressWarnings("unchecked")
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                QueryApiKeyRequest queryApiKeyRequest = (QueryApiKeyRequest) request;
                final QueryBuilder queryBuilder = queryApiKeyRequest.getQueryBuilder();
                assertNotNull(queryBuilder);
                assertThat(queryBuilder.getClass(), is(MatchAllQueryBuilder.class));
                assertThat(queryApiKeyRequest.getFrom(), equalTo(42));
                assertThat(queryApiKeyRequest.getSize(), equalTo(20));
                final List<FieldSortBuilder> fieldSortBuilders = queryApiKeyRequest.getFieldSortBuilders();
                assertThat(fieldSortBuilders, hasSize(3));

                assertThat(fieldSortBuilders.get(0), equalTo(new FieldSortBuilder("name")));
                assertThat(
                    fieldSortBuilders.get(1),
                    equalTo(new FieldSortBuilder("creation_time").setFormat("strict_date_time").order(SortOrder.DESC))
                );
                assertThat(fieldSortBuilders.get(2), equalTo(new FieldSortBuilder("username")));

                final SearchAfterBuilder searchAfterBuilder = queryApiKeyRequest.getSearchAfterBuilder();
                assertThat(
                    searchAfterBuilder,
                    equalTo(new SearchAfterBuilder().setSortValues(new String[] { "key-2048", "2021-07-01T00:00:59.000Z" }))
                );

                listener.onResponse((Response) QueryApiKeyResponse.EMPTY);
            }
        };

        final RestQueryApiKeyAction restQueryApiKeyAction = new RestQueryApiKeyAction(Settings.EMPTY, mockLicenseState);
        restQueryApiKeyAction.handleRequest(restRequest, restChannel, client);

        assertNotNull(responseSetOnce.get());
    }

    @SuppressWarnings("unchecked")
    public void testQueryApiKeyWithProfileUid() throws Exception {
        final boolean isQueryRequestWithProfileUid = randomBoolean();
        Map<String, String> param = new HashMap<>();
        if (isQueryRequestWithProfileUid) {
            param.put("with_profile_uid", Boolean.TRUE.toString());
        } else {
            if (randomBoolean()) {
                param.put("with_profile_uid", Boolean.FALSE.toString());
            }
        }
        FakeRestRequest restRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(param).build();
        SetOnce<RestResponse> responseSetOnce = new SetOnce<>();
        RestChannel restChannel = new AbstractRestChannel(restRequest, randomBoolean()) {
            @Override
            public void sendResponse(RestResponse restResponse) {
                responseSetOnce.set(restResponse);
            }
        };
        ApiKey apiKey1 = new ApiKey(
            randomAlphaOfLength(4),
            randomAlphaOfLength(4),
            randomFrom(ApiKey.Type.values()),
            Instant.now(),
            Instant.now(),
            randomBoolean(),
            null,
            randomAlphaOfLength(4),
            randomAlphaOfLength(4),
            null,
            null,
            null,
            null
        );
        final List<String> profileUids;
        if (randomBoolean()) {
            if (randomBoolean()) {
                profileUids = null;
            } else {
                profileUids = new ArrayList<>(1);
                profileUids.add(null);
            }
        } else {
            profileUids = new ArrayList<>(1);
            profileUids.add(randomAlphaOfLength(8));
        }
        var client = new NodeClient(Settings.EMPTY, threadPool, TestProjectResolvers.mustExecuteFirst()) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                QueryApiKeyRequest queryApiKeyRequest = (QueryApiKeyRequest) request;
                if (queryApiKeyRequest.withProfileUid()) {
                    listener.onResponse(
                        (Response) new QueryApiKeyResponse(
                            1,
                            List.of(apiKey1),
                            List.<Object[]>of(new Object[] { "last" }),
                            profileUids,
                            null
                        )
                    );
                } else {
                    listener.onResponse(
                        (Response) new QueryApiKeyResponse(1, List.of(apiKey1), List.<Object[]>of(new Object[] { "last" }), null, null)
                    );
                }
            }
        };
        RestQueryApiKeyAction restQueryApiKeyAction = new RestQueryApiKeyAction(Settings.EMPTY, mockLicenseState);
        restQueryApiKeyAction.handleRequest(restRequest, restChannel, client);
        RestResponse restResponse = responseSetOnce.get();
        assertNotNull(restResponse);
        assertThat(restResponse.status(), is(RestStatus.OK));
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), restResponse.content())) {
            Map<String, Object> queryApiKeyResponseMap = parser.map();
            assertThat((List<Map<String, Object>>) queryApiKeyResponseMap.get("api_keys"), iterableWithSize(1));
            assertThat(((List<Map<String, Object>>) queryApiKeyResponseMap.get("api_keys")).get(0).get("id"), is(apiKey1.getId()));
            assertThat(
                (List<Object[]>) ((List<Map<String, Object>>) queryApiKeyResponseMap.get("api_keys")).get(0).get("_sort"),
                contains("last")
            );
            if (isQueryRequestWithProfileUid && profileUids != null && profileUids.get(0) != null) {
                assertThat(
                    ((List<Map<String, Object>>) queryApiKeyResponseMap.get("api_keys")).get(0).get("profile_uid"),
                    is(profileUids.get(0))
                );
            } else {
                assertThat(((List<Map<String, Object>>) queryApiKeyResponseMap.get("api_keys")).get(0).get("profile_uid"), nullValue());
            }
        }
    }
}
