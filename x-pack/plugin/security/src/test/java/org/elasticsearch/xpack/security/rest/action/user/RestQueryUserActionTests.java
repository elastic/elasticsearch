/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.user;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.PrefixQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.AbstractRestChannel;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.searchafter.SearchAfterBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.action.user.QueryUserRequest;
import org.elasticsearch.xpack.core.security.action.user.QueryUserResponse;

import java.util.List;

import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Mockito.mock;

public class RestQueryUserActionTests extends ESTestCase {

    private final XPackLicenseState mockLicenseState = mock(XPackLicenseState.class);

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
                        "username": [ "bart", "homer" ]
                      }
                    }
                  ],
                  "should": [ { "prefix": { "username": "ba" } } ]
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

        try (var threadPool = createThreadPool()) {
            final var client = new NodeClient(Settings.EMPTY, threadPool) {
                @SuppressWarnings("unchecked")
                @Override
                public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                    ActionType<Response> action,
                    Request request,
                    ActionListener<Response> listener
                ) {
                    QueryUserRequest queryUserRequest = (QueryUserRequest) request;
                    final QueryBuilder queryBuilder = queryUserRequest.getQueryBuilder();
                    assertNotNull(queryBuilder);
                    assertThat(queryBuilder.getClass(), is(BoolQueryBuilder.class));
                    final BoolQueryBuilder boolQueryBuilder = (BoolQueryBuilder) queryBuilder;
                    assertTrue(boolQueryBuilder.filter().isEmpty());
                    assertTrue(boolQueryBuilder.mustNot().isEmpty());
                    assertThat(boolQueryBuilder.must(), hasSize(1));
                    final QueryBuilder mustQueryBuilder = boolQueryBuilder.must().get(0);
                    assertThat(mustQueryBuilder.getClass(), is(TermsQueryBuilder.class));
                    assertThat(((TermsQueryBuilder) mustQueryBuilder).fieldName(), equalTo("username"));
                    assertThat(boolQueryBuilder.should(), hasSize(1));
                    final QueryBuilder shouldQueryBuilder = boolQueryBuilder.should().get(0);
                    assertThat(shouldQueryBuilder.getClass(), is(PrefixQueryBuilder.class));
                    assertThat(((PrefixQueryBuilder) shouldQueryBuilder).fieldName(), equalTo("username"));
                    listener.onResponse((Response) new QueryUserResponse(0, List.of()));
                }
            };
            final RestQueryUserAction restQueryUserAction = new RestQueryUserAction(Settings.EMPTY, mockLicenseState);
            restQueryUserAction.handleRequest(restRequest, restChannel, client);
        }

        assertNotNull(responseSetOnce.get());
    }

    public void testParsingSearchParameters() throws Exception {
        final String requestBody = """
            {
              "query": {
                "match_all": {}
              },
              "from": 42,
              "size": 20,
              "sort": [ "username", "full_name"],
              "search_after": [ "bart" ]
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

        try (var threadPool = createThreadPool()) {
            final var client = new NodeClient(Settings.EMPTY, threadPool) {
                @SuppressWarnings("unchecked")
                @Override
                public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                    ActionType<Response> action,
                    Request request,
                    ActionListener<Response> listener
                ) {
                    QueryUserRequest queryUserRequest = (QueryUserRequest) request;
                    final QueryBuilder queryBuilder = queryUserRequest.getQueryBuilder();
                    assertNotNull(queryBuilder);
                    assertThat(queryBuilder.getClass(), is(MatchAllQueryBuilder.class));
                    assertThat(queryUserRequest.getFrom(), equalTo(42));
                    assertThat(queryUserRequest.getSize(), equalTo(20));
                    final List<FieldSortBuilder> fieldSortBuilders = queryUserRequest.getFieldSortBuilders();
                    assertThat(fieldSortBuilders, hasSize(2));

                    assertThat(fieldSortBuilders.get(0), equalTo(new FieldSortBuilder("username")));
                    assertThat(fieldSortBuilders.get(1), equalTo(new FieldSortBuilder("full_name")));

                    final SearchAfterBuilder searchAfterBuilder = queryUserRequest.getSearchAfterBuilder();
                    assertThat(searchAfterBuilder, equalTo(new SearchAfterBuilder().setSortValues(new String[] { "bart" })));

                    listener.onResponse((Response) new QueryUserResponse(0, List.of()));
                }
            };

            final RestQueryUserAction queryUserAction = new RestQueryUserAction(Settings.EMPTY, mockLicenseState);
            queryUserAction.handleRequest(restRequest, restChannel, client);
        }
        assertNotNull(responseSetOnce.get());
    }
}
