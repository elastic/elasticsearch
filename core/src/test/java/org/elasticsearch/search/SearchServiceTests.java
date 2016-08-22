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
package org.elasticsearch.search;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.search.fetch.ShardFetchSearchRequest;
import org.elasticsearch.test.ESSingleNodeTestCase;

import com.carrotsearch.hppc.IntArrayList;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.internal.ShardSearchLocalRequest;
import org.elasticsearch.search.query.QuerySearchResultProvider;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class SearchServiceTests extends ESSingleNodeTestCase {

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    public void testClearOnClose() throws ExecutionException, InterruptedException {
        createIndex("index");
        client().prepareIndex("index", "type", "1").setSource("field", "value").setRefresh(true).get();
        SearchResponse searchResponse = client().prepareSearch("index").setSize(1).setScroll("1m").get();
        assertThat(searchResponse.getScrollId(), is(notNullValue()));
        SearchService service = getInstanceFromNode(SearchService.class);

        assertEquals(1, service.getActiveContexts());
        service.doClose(); // this kills the keep-alive reaper we have to reset the node after this test
        assertEquals(0, service.getActiveContexts());
    }

    public void testClearOnStop() throws ExecutionException, InterruptedException {
        createIndex("index");
        client().prepareIndex("index", "type", "1").setSource("field", "value").setRefresh(true).get();
        SearchResponse searchResponse = client().prepareSearch("index").setSize(1).setScroll("1m").get();
        assertThat(searchResponse.getScrollId(), is(notNullValue()));
        SearchService service = getInstanceFromNode(SearchService.class);

        assertEquals(1, service.getActiveContexts());
        service.doStop();
        assertEquals(0, service.getActiveContexts());
    }

    public void testClearIndexDelete() throws ExecutionException, InterruptedException {
        createIndex("index");
        client().prepareIndex("index", "type", "1").setSource("field", "value").setRefresh(true).get();
        SearchResponse searchResponse = client().prepareSearch("index").setSize(1).setScroll("1m").get();
        assertThat(searchResponse.getScrollId(), is(notNullValue()));
        SearchService service = getInstanceFromNode(SearchService.class);

        assertEquals(1, service.getActiveContexts());
        assertAcked(client().admin().indices().prepareDelete("index"));
        assertEquals(0, service.getActiveContexts());
    }

    public void testSearchWhileContextIsFreed() throws IOException, InterruptedException {
        createIndex("index");
        client().prepareIndex("index", "type", "1").setSource("field", "value").setRefresh(true).get();

        final SearchService service = getInstanceFromNode(SearchService.class);
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe("index");
        final IndexShard indexShard = indexService.shard(0);
        final AtomicBoolean running = new AtomicBoolean(true);
        final CountDownLatch startGun = new CountDownLatch(1);
        final Semaphore semaphore = new Semaphore(Integer.MAX_VALUE);
        final AtomicLong contextId = new AtomicLong(0);
        final Thread thread = new Thread() {
            @Override
            public void run() {
                startGun.countDown();
                while(running.get()) {
                    service.freeContext(contextId.get());
                    if (randomBoolean()) {
                        // here we trigger some refreshes to ensure the IR go out of scope such that we hit ACE if we access a search
                        // context in a non-sane way.
                        try {
                            semaphore.acquire();
                        } catch (InterruptedException e) {
                            throw new AssertionError(e);
                        }
                        client().prepareIndex("index", "type").setSource("field", "value")
                            .setRefresh(randomBoolean()).execute(new ActionListener<IndexResponse>() {
                            @Override
                            public void onResponse(IndexResponse indexResponse) {
                                semaphore.release();
                            }

                            @Override
                            public void onFailure(Throwable e) {
                                semaphore.release();
                            }
                        });
                    }
                }
            }
        };
        thread.start();
        startGun.await();
        try {
            final int rounds = scaledRandomIntBetween(100, 10000);
            for (int i = 0; i < rounds; i++) {
                try {
                    QuerySearchResultProvider querySearchResultProvider = service.executeQueryPhase(
                        new ShardSearchLocalRequest(indexShard.shardId(), 1, SearchType.DEFAULT,
                            new BytesArray(""), new String[0], false));
                    contextId.set(querySearchResultProvider.id());
                    IntArrayList intCursors = new IntArrayList(1);
                    intCursors.add(0);
                    ShardFetchSearchRequest req = new ShardFetchSearchRequest(new SearchRequest()
                        ,querySearchResultProvider.id(), intCursors, null /* not a scroll */);
                    service.executeFetchPhase(req);
                } catch (AlreadyClosedException ex) {
                    throw ex;
                } catch (IllegalStateException ex) {
                    assertEquals("search context is already closed can't increment refCount current count [0]", ex.getMessage());
                } catch (SearchContextMissingException ex) {
                    // that's fine
                }
            }
        } finally {
            running.set(false);
            thread.join();
            semaphore.acquire(Integer.MAX_VALUE);
        }
    }

}
