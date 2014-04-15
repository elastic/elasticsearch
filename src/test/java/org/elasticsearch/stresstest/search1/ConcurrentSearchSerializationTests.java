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

package org.elasticsearch.stresstest.search1;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHit;
import org.junit.Ignore;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Tests that data don't get corrupted while reading it over the streams.
 * <p/>
 * See: https://github.com/elasticsearch/elasticsearch/issues/1686.
 */
@Ignore("Stress Test")
public class ConcurrentSearchSerializationTests {

    public static void main(String[] args) throws Exception {

        Settings settings = ImmutableSettings.settingsBuilder().put("gateway.type", "none").build();

        Node node1 = NodeBuilder.nodeBuilder().settings(settings).node();
        Node node2 = NodeBuilder.nodeBuilder().settings(settings).node();
        Node node3 = NodeBuilder.nodeBuilder().settings(settings).node();

        final Client client = node1.client();

        System.out.println("Indexing...");
        final String data = RandomStrings.randomAsciiOfLength(ThreadLocalRandom.current(), 100);
        final CountDownLatch latch1 = new CountDownLatch(100);
        for (int i = 0; i < 100; i++) {
            client.prepareIndex("test", "type", Integer.toString(i))
                    .setSource("field", data)
                    .execute(new ActionListener<IndexResponse>() {
                        @Override
                        public void onResponse(IndexResponse indexResponse) {
                            latch1.countDown();
                        }

                        @Override
                        public void onFailure(Throwable e) {
                            latch1.countDown();
                        }
                    });
        }
        latch1.await();
        System.out.println("Indexed");

        System.out.println("searching...");
        Thread[] threads = new Thread[10];
        final CountDownLatch latch = new CountDownLatch(threads.length);
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < 1000; i++) {
                        SearchResponse searchResponse = client.prepareSearch("test")
                                .setQuery(QueryBuilders.matchAllQuery())
                                .setSize(i % 100)
                                .execute().actionGet();
                        for (SearchHit hit : searchResponse.getHits()) {
                            try {
                                if (!hit.sourceAsMap().get("field").equals(data)) {
                                    System.err.println("Field not equal!");
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
                    latch.countDown();
                }
            });
        }
        for (Thread thread : threads) {
            thread.start();
        }

        latch.await();

        System.out.println("done searching");
        client.close();
        node1.close();
        node2.close();
        node3.close();
    }
}
