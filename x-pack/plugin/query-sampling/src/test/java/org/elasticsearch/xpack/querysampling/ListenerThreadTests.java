/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;

public class ListenerThreadTests extends ESTestCase {

    public void testAResponseIsStillAliveWhenTheExecutorHandlesItAfterTheSenderHasLetGo() {
        Deque<Runnable> queue = new ArrayDeque<>();
        AtomicInteger handled = new AtomicInteger();
        AtomicBoolean aliveWhenHandled = new AtomicBoolean();

        ListenerThread.<SearchRequest, SearchResponse>on(queue::add, (request, listener) -> {
            SearchHits hits = SearchHits.empty(new TotalHits(0, TotalHits.Relation.EQUAL_TO), 1f);
            SearchResponse response = SearchResponseUtils.successfulResponse(hits);
            hits.decRef();
            try {
                listener.onResponse(response);
            } finally {
                response.decRef(); // what the sender does as soon as the listener returns
            }
        }).accept(new SearchRequest(), org.elasticsearch.action.ActionListener.wrap(response -> {
            handled.incrementAndGet();
            aliveWhenHandled.set(response.hasReferences());
        }, e -> fail(e)));

        assertThat("nothing is handled by the thread of the sender", handled.get(), equalTo(0));
        assertThat(queue.size(), equalTo(1));
        queue.poll().run();
        assertThat(handled.get(), equalTo(1));
        assertTrue("the response was not released before it was handled", aliveWhenHandled.get());
    }
}
