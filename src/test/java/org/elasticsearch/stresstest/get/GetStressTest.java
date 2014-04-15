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

package org.elasticsearch.stresstest.get;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class GetStressTest {

    public static void main(String[] args) throws Exception {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("index.number_of_shards", 2)
                .put("index.number_of_replicas", 1)
                .build();

        final int NUMBER_OF_NODES = 2;
        final int NUMBER_OF_THREADS = 50;
        final TimeValue TEST_TIME = TimeValue.parseTimeValue("10m", null);

        Node[] nodes = new Node[NUMBER_OF_NODES];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = NodeBuilder.nodeBuilder().settings(settings).node();
        }

        final Node client = NodeBuilder.nodeBuilder()
                .settings(settings)
                .client(true)
                .node();

        client.client().admin().indices().prepareCreate("test").execute().actionGet();

        final AtomicBoolean done = new AtomicBoolean();
        final AtomicLong idGenerator = new AtomicLong();
        final AtomicLong counter = new AtomicLong();

        Thread[] threads = new Thread[NUMBER_OF_THREADS];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    ThreadLocalRandom random = ThreadLocalRandom.current();
                    while (!done.get()) {
                        String id = String.valueOf(idGenerator.incrementAndGet());
                        client.client().prepareIndex("test", "type1", id)
                                .setSource("field", random.nextInt(100))
                                .execute().actionGet();

                        GetResponse getResponse = client.client().prepareGet("test", "type1", id)
                                //.setFields(Strings.EMPTY_ARRAY)
                                .execute().actionGet();
                        if (!getResponse.isExists()) {
                            System.err.println("Failed to find " + id);
                        }

                        long count = counter.incrementAndGet();
                        if ((count % 10000) == 0) {
                            System.out.println("Executed " + count);
                        }
                    }
                }
            });
        }
        for (Thread thread : threads) {
            thread.start();
        }

        Thread.sleep(TEST_TIME.millis());

        System.out.println("test done.");
        done.set(true);
    }
}