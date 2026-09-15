/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.service;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.project.TestProjectResolvers;
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
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.searchafter.SearchAfterBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.action.service.QueryServiceAccountAction;
import org.elasticsearch.xpack.core.security.action.service.QueryServiceAccountRequest;
import org.elasticsearch.xpack.core.security.action.service.QueryServiceAccountResponse;

import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class RestQueryServiceAccountActionTests extends ESTestCase {

    private final XPackLicenseState mockLicenseState = mock(XPackLicenseState.class);

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        final SearchModule searchModule = new SearchModule(Settings.EMPTY, List.of());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    public void testQueryParsing() throws Exception {
        final String body = """
            {
              "query": {
                "bool": {
                  "must": [ { "terms": { "roles": [ "role-a", "role-b" ] } } ],
                  "should": [ { "prefix": { "username": "apps/" } } ]
                }
              }
            }""";
        handle(body, request -> {
            final QueryBuilder queryBuilder = request.getQueryBuilder();
            assertThat(queryBuilder, instanceOf(BoolQueryBuilder.class));
            final BoolQueryBuilder boolQueryBuilder = (BoolQueryBuilder) queryBuilder;
            assertThat(boolQueryBuilder.filter(), empty());
            assertThat(boolQueryBuilder.mustNot(), empty());
            assertThat(boolQueryBuilder.must(), hasSize(1));
            assertThat(boolQueryBuilder.must().get(0), instanceOf(TermsQueryBuilder.class));
            assertThat(((TermsQueryBuilder) boolQueryBuilder.must().get(0)).fieldName(), equalTo("roles"));
            assertThat(boolQueryBuilder.should(), hasSize(1));
            assertThat(boolQueryBuilder.should().get(0), instanceOf(PrefixQueryBuilder.class));
            assertThat(((PrefixQueryBuilder) boolQueryBuilder.should().get(0)).fieldName(), equalTo("username"));
            assertThat(request.getFrom(), nullValue());
            assertThat(request.getSize(), nullValue());
            assertThat(request.getFieldSortBuilders(), nullValue());
            assertThat(request.getSearchAfterBuilder(), nullValue());
        });
    }

    public void testSearchParametersParsing() throws Exception {
        final String body = """
            {
              "query": { "match_all": {} },
              "from": 42,
              "size": 20,
              "sort": [ "username", { "enabled": { "order": "desc" } } ],
              "search_after": [ "apps/worker_1", true ]
            }""";
        handle(body, request -> {
            assertThat(request.getQueryBuilder(), instanceOf(MatchAllQueryBuilder.class));
            assertThat(request.getFrom(), equalTo(42));
            assertThat(request.getSize(), equalTo(20));
            assertThat(
                request.getFieldSortBuilders(),
                contains(new FieldSortBuilder("username"), new FieldSortBuilder("enabled").order(SortOrder.DESC))
            );
            assertThat(
                request.getSearchAfterBuilder(),
                equalTo(new SearchAfterBuilder().setSortValues(new Object[] { "apps/worker_1", true }))
            );
        });
    }

    public void testNoBodySelectsEveryAccount() throws Exception {
        handle(null, request -> {
            assertThat(request.getQueryBuilder(), nullValue());
            assertThat(request.getFrom(), nullValue());
            assertThat(request.getSize(), nullValue());
            assertThat(request.getFieldSortBuilders(), nullValue());
            assertThat(request.getSearchAfterBuilder(), nullValue());
        });
    }

    public void testTheCapabilityIsReportedOnlyWhereUserManagedAccountsAreAvailable() {
        assertThat(
            new RestQueryServiceAccountAction(Settings.EMPTY, mockLicenseState, true).supportedCapabilities(),
            contains(UserManagedServiceAccountRestCapabilities.USER_MANAGED_SERVICE_ACCOUNTS)
        );
        assertThat(new RestQueryServiceAccountAction(Settings.EMPTY, mockLicenseState, false).supportedCapabilities(), empty());
    }

    private void handle(String body, Consumer<QueryServiceAccountRequest> requestAssertions) throws Exception {
        final FakeRestRequest.Builder requestBuilder = new FakeRestRequest.Builder(xContentRegistry()).withMethod(
            randomFrom(RestRequest.Method.GET, RestRequest.Method.POST)
        ).withPath("/_security/_query/service_account");
        if (body != null) {
            requestBuilder.withContent(new BytesArray(body), XContentType.JSON);
        }
        final FakeRestRequest restRequest = requestBuilder.build();

        final SetOnce<RestResponse> responseSetOnce = new SetOnce<>();
        final RestChannel restChannel = new AbstractRestChannel(restRequest, randomBoolean()) {
            @Override
            public void sendResponse(RestResponse restResponse) {
                responseSetOnce.set(restResponse);
            }
        };

        final SetOnce<QueryServiceAccountRequest> executed = new SetOnce<>();
        try (var threadPool = createThreadPool()) {
            final var client = new NodeClient(Settings.EMPTY, threadPool, TestProjectResolvers.alwaysThrow()) {
                @SuppressWarnings("unchecked")
                @Override
                public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                    ActionType<Response> action,
                    Request request,
                    ActionListener<Response> listener
                ) {
                    assertThat(action, is(QueryServiceAccountAction.INSTANCE));
                    executed.set((QueryServiceAccountRequest) request);
                    listener.onResponse((Response) QueryServiceAccountResponse.EMPTY);
                }
            };
            new RestQueryServiceAccountAction(Settings.EMPTY, mockLicenseState, true).handleRequest(restRequest, restChannel, client);
        }

        assertThat(responseSetOnce.get(), notNullValue());
        assertThat(executed.get(), notNullValue());
        requestAssertions.accept(executed.get());
    }
}
