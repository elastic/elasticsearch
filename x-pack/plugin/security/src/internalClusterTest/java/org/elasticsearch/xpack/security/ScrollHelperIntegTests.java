/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.ScrollHelper;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class ScrollHelperIntegTests extends ESSingleNodeTestCase {

    public void testFetchAllEntities() throws ExecutionException, InterruptedException {
        Client client = client();
        int numDocs = randomIntBetween(5, 30);
        for (int i = 0; i < numDocs; i++) {
            client.prepareIndex("foo").setSource(Collections.singletonMap("number", i)).get();
        }
        client.admin().indices().prepareRefresh("foo").get();
        SearchRequest request = client.prepareSearch()
                .setScroll(TimeValue.timeValueHours(10L))
                .setQuery(QueryBuilders.matchAllQuery())
                .setSize(randomIntBetween(1, 10))
                .setFetchSource(true)
                .request();
        request.indicesOptions().ignoreUnavailable();
        PlainActionFuture<Collection<Integer>> future = new PlainActionFuture<>();
        ScrollHelper.fetchAllByEntity(client(), request, future,
                (hit) -> Integer.parseInt(hit.getSourceAsMap().get("number").toString()));
        Collection<Integer> integers = future.actionGet();
        ArrayList<Integer> list = new ArrayList<>(integers);
        CollectionUtil.timSort(list);
        assertEquals(numDocs, list.size());
        for (int i = 0; i < numDocs; i++) {
            assertEquals(list.get(i).intValue(), i);
        }
    }

    /**
     * Tests that
     * {@link ScrollHelper#fetchAllByEntity(Client, SearchRequest, ActionListener, Function)}
     * defends against scrolls broken in such a way that the remote Elasticsearch returns infinite results. While Elasticsearch
     * <strong>shouldn't</strong> do this it has in the past and it is <strong>very</strong> when it does. It takes out the whole node. So
     * this makes sure we defend against it properly.
     */
    public void testFetchAllByEntityWithBrokenScroll() {
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        SearchRequest request = new SearchRequest();
        request.scroll(TimeValue.timeValueHours(10L));

        String scrollId = randomAlphaOfLength(5);
        SearchHit[] hits = new SearchHit[] {new SearchHit(1), new SearchHit(2)};
        InternalSearchResponse internalResponse = new InternalSearchResponse(new SearchHits(hits,
            new TotalHits(3, TotalHits.Relation.EQUAL_TO), 1),
            null,
            null,
            null, false,
            false,
            1);
        SearchResponse response = new SearchResponse(internalResponse, scrollId, 1, 1, 0, 0, ShardSearchFailure.EMPTY_ARRAY,
                SearchResponse.Clusters.EMPTY);

        Answer<?> returnResponse = invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) invocation.getArguments()[1];
            listener.onResponse(response);
            return null;
        };
        doAnswer(returnResponse).when(client).search(eq(request), anyObject());
        /* The line below simulates the evil cluster. A working cluster would return
         * a response with 0 hits. Our simulated broken cluster returns the same
         * response over and over again. */
        doAnswer(returnResponse).when(client).searchScroll(anyObject(), anyObject());

        AtomicReference<Exception> failure = new AtomicReference<>();
        ScrollHelper.fetchAllByEntity(client, request, new ActionListener<Collection<SearchHit>>() {
            @Override
            public void onResponse(Collection<SearchHit> response) {
                fail("This shouldn't succeed.");
            }

            @Override
            public void onFailure(Exception e) {
                failure.set(e);
            }
        }, Function.identity());

        assertNotNull("onFailure wasn't called", failure.get());
        assertEquals("scrolling returned more hits [4] than expected [3] so bailing out to prevent unbounded memory consumption.",
                failure.get().getMessage());
    }
}
