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

package org.elasticsearch.index;

import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.indices.migrate.MigrateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.migrate.MigrateIndexResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

/**
 * Common superclass for integration tests for migrating indexes.
 */
public abstract class MigrateIndexTestCase extends ESIntegTestCase {
    public void testMigrateFromEmptyIndex() throws InterruptedException, ExecutionException, TimeoutException {
        migrateIndexTestCase(0, new Script("ctx._source.foo += ' cat'", ScriptType.INLINE, "doesn't matter, not used", emptyMap()));
    }

    public void testMigrateFromNonExistentIndex() {
        client().admin().indices().prepareMigrateIndex("dontexist", "test_2").setAliases("test").get();
        assertTrue(client().admin().indices().prepareExists("test_2").get().isExists());
        assertTrue(client().admin().indices().prepareAliasesExist("test").get().isExists());
    }

    protected void migrateIndexTestCase(int docCount, Script script) throws InterruptedException, ExecutionException, TimeoutException {
        if (docCount > 0) {
            List<IndexRequestBuilder> docs = new ArrayList<>(docCount);
            for (int i = 0; i < docCount; i++) {
                docs.add(client().prepareIndex("test_0", "test").setSource("foo", "bar", "i", i));
            }
            indexRandom(true, docs);
        } else {
            client().admin().indices().prepareCreate("test_0").get();
        }
        client().admin().indices().prepareAliases().addAlias("test_0", "test").get();

        // They are there, great
        SearchResponse searchResponse = client().prepareSearch("test_0").setSize(0).get();
        assertHitCount(searchResponse, docCount);
        // And you can search for them by alias
        searchResponse = client().prepareSearch("test").setSize(0).get();
        assertHitCount(searchResponse, docCount);

        // Migrate them from "test_0" to "test_1"
        assertFalse(client().admin().indices().prepareMigrateIndex("test_0", "test_1").setAliases("test").get().isNoop());
        // That removes the source index
        assertFalse(client().admin().indices().prepareExists("test_0").get().isExists());

        // But the documents are still there, just in the new index
        searchResponse = client().prepareSearch("test_1").setSize(0).get();
        assertHitCount(searchResponse, docCount);
        searchResponse = client().prepareSearch("test").setSize(0).get();
        assertHitCount(searchResponse, docCount);

        // Doing it again from and to the same index does nothing
        assertTrue(client().admin().indices().prepareMigrateIndex("test_0", "test_1").setAliases("test").get().isNoop());

        // We can also migrate to a new index and actually apply a script
        {
            MigrateIndexRequestBuilder migrate = client().admin().indices().prepareMigrateIndex("test_1", "test_2")
                    .setAliases("test").setScript(script);
            assertFalse(migrate.get().isNoop());
            assertFalse(client().admin().indices().prepareExists("test_1").get().isExists());
            assertTrue(client().admin().indices().prepareExists("test_2").get().isExists());
        }

        /* We could migrate to yet another index lots of time concurrently. This is important because masterless systems like logstash need
         * to be able to consistently use this API on startup in all nodes.*/ 
        MigrateIndexRequestBuilder migrate = client().admin().indices().prepareMigrateIndex("test_2", "test_3")
                .setAliases("test").setScript(script);
        int concurrentRequests = between(2, Runtime.getRuntime().availableProcessors());
        CyclicBarrier latch = new CyclicBarrier(concurrentRequests);
        int requestsPerThread = between(5, 100);
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < concurrentRequests; i++) {
            Thread t = new Thread(() ->{
                for (int r = 0; r < requestsPerThread; r++) {
                    try {
                        latch.await();
                    } catch (InterruptedException | BrokenBarrierException e) {
                        throw new RuntimeException(e);
                    }
                    MigrateIndexResponse response = migrate.get();
                    assertTrue(response.isAcknowledged());
                    assertFalse(client().admin().indices().prepareExists("test_2").get().isExists());
                    assertTrue(client().admin().indices().prepareExists("test_3").get().isExists());
                }
            });
            t.setName(getTestName() + "#" + i);
            threads.add(t);
            t.start();
        }
        try {
            for (Thread thread : threads) {
                thread.join(TimeUnit.SECONDS.toMillis(30));
            }
        } catch (InterruptedException e) {
            ListTasksResponse listTasks = client().admin().cluster().prepareListTasks().get();
            throw new RuntimeException("Failed while waiting for migrations. These are the running tasks: " + listTasks, e);
        }
    }
}
