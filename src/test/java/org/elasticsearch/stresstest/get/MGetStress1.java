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

import com.google.common.collect.Sets;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 */
public class MGetStress1 {

    public static void main(String[] args) throws Exception {
        final int NUMBER_OF_NODES = 2;
        final int NUMBER_OF_DOCS = 50000;
        final int MGET_BATCH = 1000;

        Node[] nodes = new Node[NUMBER_OF_NODES];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = NodeBuilder.nodeBuilder().node();
        }

        System.out.println("---> START Indexing initial data [" + NUMBER_OF_DOCS + "]");
        final Client client = nodes[0].client();
        for (int i = 0; i < NUMBER_OF_DOCS; i++) {
            client.prepareIndex("test", "type", Integer.toString(i)).setSource("field", "value").execute().actionGet();
        }
        System.out.println("---> DONE Indexing initial data [" + NUMBER_OF_DOCS + "]");

        final AtomicBoolean done = new AtomicBoolean();
        // start indexer
        Thread indexer = new Thread(new Runnable() {
            @Override
            public void run() {
                while (!done.get()) {
                    client.prepareIndex("test", "type", Integer.toString(ThreadLocalRandom.current().nextInt(NUMBER_OF_DOCS)))
                            .setSource("field", "value").execute().actionGet();
                }
            }
        });
        indexer.start();
        System.out.println("---> Starting indexer");

        // start the mget one
        Thread mget = new Thread(new Runnable() {
            @Override
            public void run() {
                while (!done.get()) {
                    Set<String> ids = Sets.newHashSet();
                    for (int i = 0; i < MGET_BATCH; i++) {
                        ids.add(Integer.toString(ThreadLocalRandom.current().nextInt(NUMBER_OF_DOCS)));
                    }
                    //System.out.println("---> mget for [" + ids.size() + "]");
                    MultiGetResponse response = client.prepareMultiGet().add("test", "type", ids).execute().actionGet();
                    int expected = ids.size();
                    int count = 0;
                    for (MultiGetItemResponse item : response) {
                        count++;
                        if (item.isFailed()) {
                            System.err.println("item failed... " + item.getFailure());
                        } else {
                            boolean removed = ids.remove(item.getId());
                            if (!removed) {
                                System.err.println("got id twice " + item.getId());
                            }
                        }
                    }
                    if (expected != count) {
                        System.err.println("Expected [" + expected + "], got back [" + count + "]");
                    }
                }
            }
        });
        mget.start();
        System.out.println("---> Starting mget");

        Thread.sleep(TimeValue.timeValueMinutes(10).millis());

        done.set(true);
    }
}
