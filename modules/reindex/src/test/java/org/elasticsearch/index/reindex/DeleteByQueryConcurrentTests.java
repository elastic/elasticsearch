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

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;

public class DeleteByQueryConcurrentTests extends ReindexTestCase {

    public void testConcurrentDeleteByQueriesOnDifferentDocs() throws Throwable {
        final Thread[] threads =  new Thread[scaledRandomIntBetween(2, 5)];
        final long docs = randomIntBetween(1, 50);

        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < docs; i++) {
            for (int t = 0; t < threads.length; t++) {
                builders.add(client().prepareIndex("test", "doc").setSource("field", t));
            }
        }
        indexRandom(true, true, true, builders);

        final CountDownLatch start = new CountDownLatch(1);
        for (int t = 0; t < threads.length; t++) {
            final int threadNum = t;
            assertHitCount(client().prepareSearch("test").setSize(0).setQuery(QueryBuilders.termQuery("field", threadNum)).get(), docs);

            Runnable r = () -> {
                try {
                    start.await();

                    assertThat(deleteByQuery().source("_all").filter(termQuery("field", threadNum)).refresh(true).get(),
                            matcher().deleted(docs));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            };
            threads[t] = new Thread(r);
            threads[t].start();
        }

        start.countDown();
        for (Thread thread : threads) {
            thread.join();
        }

        for (int t = 0; t < threads.length; t++) {
            assertHitCount(client().prepareSearch("test").setSize(0).setQuery(QueryBuilders.termQuery("field", t)).get(), 0);
        }
    }

    public void testConcurrentDeleteByQueriesOnSameDocs() throws Throwable {
        final long docs = randomIntBetween(50, 100);

        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < docs; i++) {
            builders.add(client().prepareIndex("test", "doc", String.valueOf(i)).setSource("foo", "bar"));
        }
        indexRandom(true, true, true, builders);

        final Thread[] threads =  new Thread[scaledRandomIntBetween(2, 9)];

        final CountDownLatch start = new CountDownLatch(1);
        final MatchQueryBuilder query = matchQuery("foo", "bar");
        final AtomicLong deleted = new AtomicLong(0);

        for (int t = 0; t < threads.length; t++) {
            Runnable r = () -> {
                try {
                    start.await();

                    BulkByScrollResponse response = deleteByQuery().source("test").filter(query).refresh(true).get();
                    // Some deletions might fail due to version conflict, but
                    // what matters here is the total of successful deletions
                    deleted.addAndGet(response.getDeleted());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            };
            threads[t] = new Thread(r);
            threads[t].start();
        }

        start.countDown();
        for (Thread thread : threads) {
            thread.join();
        }

        assertHitCount(client().prepareSearch("test").setSize(0).get(), 0L);
        assertThat(deleted.get(), equalTo(docs));
    }
}
