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

package org.elasticsearch.stresstest.indexing;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * Checks that index operation does not create duplicate documents.
 */
public class ConcurrentIndexingVersioningStressTest {

    public static void main(String[] args) throws Exception {

        Settings settings = ImmutableSettings.EMPTY;

        Node node1 = nodeBuilder().settings(settings).node();
        Node node2 = nodeBuilder().settings(settings).node();
        final Node client = nodeBuilder().settings(settings).client(true).node();

        final int NUMBER_OF_DOCS = 10000;
        final int NUMBER_OF_THREADS = 10;
        final long NUMBER_OF_ITERATIONS = SizeValue.parseSizeValue("10k").singles();
        final long DELETE_EVERY = 10;

        final CountDownLatch latch = new CountDownLatch(NUMBER_OF_THREADS);
        Thread[] threads = new Thread[NUMBER_OF_THREADS];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread() {
                @Override
                public void run() {
                    try {
                        for (long i = 0; i < NUMBER_OF_ITERATIONS; i++) {
                            if ((i % DELETE_EVERY) == 0) {
                                client.client().prepareDelete("test", "type1", Integer.toString(ThreadLocalRandom.current().nextInt(NUMBER_OF_DOCS))).execute().actionGet();
                            } else {
                                client.client().prepareIndex("test", "type1", Integer.toString(ThreadLocalRandom.current().nextInt(NUMBER_OF_DOCS))).setSource("field1", "value1").execute().actionGet();
                            }
                        }
                    } finally {
                        latch.countDown();
                    }
                }
            };
        }

        for (Thread thread : threads) {
            thread.start();
        }

        latch.await();
        System.out.println("done indexing, verifying docs");
        client.client().admin().indices().prepareRefresh().execute().actionGet();
        for (int i = 0; i < NUMBER_OF_DOCS; i++) {
            String id = Integer.toString(i);
            for (int j = 0; j < 5; j++) {
                SearchResponse response = client.client().prepareSearch().setQuery(QueryBuilders.termQuery("_id", id)).execute().actionGet();
                if (response.getHits().totalHits() > 1) {
                    System.err.println("[" + i + "] FAIL, HITS [" + response.getHits().totalHits() + "]");
                }
            }
            GetResponse getResponse = client.client().prepareGet("test", "type1", id).execute().actionGet();
            if (getResponse.isExists()) {
                long version = getResponse.getVersion();
                for (int j = 0; j < 5; j++) {
                    getResponse = client.client().prepareGet("test", "type1", id).execute().actionGet();
                    if (!getResponse.isExists()) {
                        System.err.println("[" + i + "] FAIL, EXISTED, and NOT_EXISTED");
                        break;
                    }
                    if (version != getResponse.getVersion()) {
                        System.err.println("[" + i + "] FAIL, DIFFERENT VERSIONS: [" + version + "], [" + getResponse.getVersion() + "]");
                        break;
                    }
                }
            } else {
                for (int j = 0; j < 5; j++) {
                    getResponse = client.client().prepareGet("test", "type1", id).execute().actionGet();
                    if (getResponse.isExists()) {
                        System.err.println("[" + i + "] FAIL, EXISTED, and NOT_EXISTED");
                        break;
                    }
                }
            }
        }
        System.out.println("done.");

        client.close();
        node1.close();
        node2.close();
    }
}