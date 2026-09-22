/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.service;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.searchafter.SearchAfterBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.NestedSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockUtils;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.service.QueryServiceAccountRequest;
import org.elasticsearch.xpack.core.security.action.service.QueryServiceAccountResponse;
import org.elasticsearch.xpack.core.security.action.service.ServiceAccountInfo;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;
import org.elasticsearch.xpack.security.support.ServiceAccountBoolQueryBuilder;
import org.junit.Before;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.security.authc.service.UserManagedServiceAccountStore.SERVICE_ACCOUNT_DOC_TYPE;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class TransportQueryServiceAccountActionTests extends ESTestCase {

    private ServiceAccountService serviceAccountService;
    private TransportQueryServiceAccountAction action;
    private final AtomicReference<SearchSourceBuilder> searchedSource = new AtomicReference<>();

    @Before
    public void init() {
        final TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor();
        serviceAccountService = mock(ServiceAccountService.class);
        stubQueryResponse(QueryServiceAccountResponse.EMPTY);
        action = new TransportQueryServiceAccountAction(transportService, ActionFilters.EMPTY, serviceAccountService);
    }

    public void testARequestWithNothingSetSearchesEveryAccountWithSearchDefaults() {
        assertThat(execute(new QueryServiceAccountRequest(null, null, null, null, null)), is(QueryServiceAccountResponse.EMPTY));

        final SearchSourceBuilder source = searchedSource.get();
        assertThat(source.from(), equalTo(-1));
        assertThat(source.size(), equalTo(-1));
        assertThat(source.sorts(), nullValue());
        assertThat(source.searchAfter(), nullValue());
        assertThat(source.trackTotalHitsUpTo(), equalTo(SearchContext.TRACK_TOTAL_HITS_ACCURATE));
        assertThat(source.fetchSource().fetchSource(), is(true));
        assertThat(source.version(), is(false));
        assertThat(source.query(), instanceOf(ServiceAccountBoolQueryBuilder.class));
        final BoolQueryBuilder query = (BoolQueryBuilder) source.query();
        assertThat(query.must(), empty());
        assertThat(query.filter(), contains(QueryBuilders.termQuery("doc_type", SERVICE_ACCOUNT_DOC_TYPE)));
    }

    public void testTheCallersQueryPaginationAndSortAreCarriedIntoTheSearch() {
        final QueryServiceAccountRequest request = new QueryServiceAccountRequest(
            QueryBuilders.prefixQuery("username", "apps/"),
            5,
            10,
            List.of(new FieldSortBuilder("username").order(SortOrder.DESC), new FieldSortBuilder("enabled")),
            new SearchAfterBuilder().setSortValues(new Object[] { "apps/worker_1", true })
        );
        final QueryServiceAccountResponse response = new QueryServiceAccountResponse(
            1,
            List.of(
                new QueryServiceAccountResponse.Item(
                    new ServiceAccountInfo.UserManaged("apps/worker_0", List.of("role-a"), true),
                    new Object[] { "apps/worker_0", true }
                )
            )
        );
        stubQueryResponse(response);

        assertThat(execute(request), equalTo(response));

        final SearchSourceBuilder source = searchedSource.get();
        assertThat(source.from(), equalTo(5));
        assertThat(source.size(), equalTo(10));
        assertThat(source.searchAfter(), arrayContaining("apps/worker_1", true));
        assertThat(source.sorts(), contains(new FieldSortBuilder("username").order(SortOrder.DESC), new FieldSortBuilder("enabled")));
        final BoolQueryBuilder query = (BoolQueryBuilder) source.query();
        assertThat(query.must(), contains(QueryBuilders.prefixQuery("username", "apps/")));
        assertThat(query.filter(), contains(QueryBuilders.termQuery("doc_type", SERVICE_ACCOUNT_DOC_TYPE)));
    }

    public void testAQueryOnAFieldOutsideTheAllowlistFailsBeforeAnySearchRuns() {
        final QueryServiceAccountRequest request = new QueryServiceAccountRequest(
            QueryBuilders.termQuery(randomFrom("version", "doc_type", "password"), "x"),
            null,
            null,
            null,
            null
        );
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> execute(request));
        assertThat(e.getMessage(), containsString("is not allowed for querying or aggregation"));
        verify(serviceAccountService, never()).queryUserManagedAccounts(any(), any());
    }

    public void testASortOnAFieldOutsideTheAllowlistFailsBeforeAnySearchRuns() {
        final QueryServiceAccountRequest request = new QueryServiceAccountRequest(
            null,
            null,
            null,
            List.of(new FieldSortBuilder(randomFrom("version", "doc_type", "full_name"))),
            null
        );
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> execute(request));
        assertThat(e.getMessage(), containsString("is not allowed for querying or aggregation"));
        verify(serviceAccountService, never()).queryUserManagedAccounts(any(), any());
    }

    public void testNestedSortingIsRejected() {
        final FieldSortBuilder nested = new FieldSortBuilder("roles").setNestedSort(new NestedSortBuilder("roles"));
        final QueryServiceAccountRequest request = new QueryServiceAccountRequest(null, null, null, List.of(nested), null);
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> execute(request));
        assertThat(e.getMessage(), equalTo("nested sorting is not currently supported in this context"));
    }

    public void testAFailedSearchFailsTheRequest() {
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final ActionListener<QueryServiceAccountResponse> listener = (ActionListener<QueryServiceAccountResponse>) invocation
                .getArguments()[1];
            listener.onFailure(new ElasticsearchException("account store unavailable"));
            return null;
        }).when(serviceAccountService).queryUserManagedAccounts(any(), any());

        final ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> execute(new QueryServiceAccountRequest(null, null, null, null, null))
        );
        assertThat(e.getMessage(), equalTo("account store unavailable"));
    }

    private QueryServiceAccountResponse execute(QueryServiceAccountRequest request) {
        final PlainActionFuture<QueryServiceAccountResponse> future = new PlainActionFuture<>();
        action.doExecute(mock(Task.class), request, future);
        return future.actionGet();
    }

    private void stubQueryResponse(QueryServiceAccountResponse response) {
        doAnswer(invocation -> {
            searchedSource.set((SearchSourceBuilder) invocation.getArguments()[0]);
            @SuppressWarnings("unchecked")
            final ActionListener<QueryServiceAccountResponse> listener = (ActionListener<QueryServiceAccountResponse>) invocation
                .getArguments()[1];
            listener.onResponse(response);
            return null;
        }).when(serviceAccountService).queryUserManagedAccounts(any(), any());
    }
}
