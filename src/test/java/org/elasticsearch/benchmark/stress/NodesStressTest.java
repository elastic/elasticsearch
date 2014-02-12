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

package org.elasticsearch.benchmark.stress;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.node.Node;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.client.Requests.searchRequest;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.ImmutableSettings.Builder.EMPTY_SETTINGS;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.index.query.FilterBuilders.queryFilter;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;

/**
 *
 */
public class NodesStressTest {

    private Node[] nodes;

    private int numberOfNodes = 2;

    private Client[] clients;

    private AtomicLong idGenerator = new AtomicLong();

    private int fieldNumLimit = 50;

    private long searcherIterations = 10;
    private Searcher[] searcherThreads = new Searcher[1];

    private long indexIterations = 10;
    private Indexer[] indexThreads = new Indexer[1];

    private TimeValue sleepAfterDone = TimeValue.timeValueMillis(0);
    private TimeValue sleepBeforeClose = TimeValue.timeValueMillis(0);

    private CountDownLatch latch;
    private CyclicBarrier barrier1;
    private CyclicBarrier barrier2;

    public NodesStressTest() {
    }

    public NodesStressTest numberOfNodes(int numberOfNodes) {
        this.numberOfNodes = numberOfNodes;
        return this;
    }

    public NodesStressTest fieldNumLimit(int fieldNumLimit) {
        this.fieldNumLimit = fieldNumLimit;
        return this;
    }

    public NodesStressTest searchIterations(int searchIterations) {
        this.searcherIterations = searchIterations;
        return this;
    }

    public NodesStressTest searcherThreads(int numberOfSearcherThreads) {
        searcherThreads = new Searcher[numberOfSearcherThreads];
        return this;
    }

    public NodesStressTest indexIterations(long indexIterations) {
        this.indexIterations = indexIterations;
        return this;
    }

    public NodesStressTest indexThreads(int numberOfWriterThreads) {
        indexThreads = new Indexer[numberOfWriterThreads];
        return this;
    }

    public NodesStressTest sleepAfterDone(TimeValue time) {
        this.sleepAfterDone = time;
        return this;
    }

    public NodesStressTest sleepBeforeClose(TimeValue time) {
        this.sleepBeforeClose = time;
        return this;
    }

    public NodesStressTest build(Settings settings) throws Exception {
        settings = settingsBuilder()
//                .put("index.refresh_interval", 1, TimeUnit.SECONDS)
                .put(SETTING_NUMBER_OF_SHARDS, 5)
                .put(SETTING_NUMBER_OF_REPLICAS, 1)
                .put(settings)
                .build();

        nodes = new Node[numberOfNodes];
        clients = new Client[numberOfNodes];
        for (int i = 0; i < numberOfNodes; i++) {
            nodes[i] = nodeBuilder().settings(settingsBuilder().put(settings).put("name", "node" + i)).node();
            clients[i] = nodes[i].client();
        }

        for (int i = 0; i < searcherThreads.length; i++) {
            searcherThreads[i] = new Searcher(i);
        }
        for (int i = 0; i < indexThreads.length; i++) {
            indexThreads[i] = new Indexer(i);
        }

        latch = new CountDownLatch(1);
        barrier1 = new CyclicBarrier(2);
        barrier2 = new CyclicBarrier(2);
        // warmup
        StopWatch stopWatch = new StopWatch().start();
        Indexer warmup = new Indexer(-1).max(10000);
        warmup.start();
        barrier1.await();
        barrier2.await();
        latch.await();
        stopWatch.stop();
        System.out.println("Done Warmup, took [" + stopWatch.totalTime() + "]");

        latch = new CountDownLatch(searcherThreads.length + indexThreads.length);
        barrier1 = new CyclicBarrier(searcherThreads.length + indexThreads.length + 1);
        barrier2 = new CyclicBarrier(searcherThreads.length + indexThreads.length + 1);

        return this;
    }

    public void start() throws Exception {
        for (Thread t : searcherThreads) {
            t.start();
        }
        for (Thread t : indexThreads) {
            t.start();
        }
        barrier1.await();

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        barrier2.await();

        latch.await();
        stopWatch.stop();

        System.out.println("Done, took [" + stopWatch.totalTime() + "]");
        System.out.println("Sleeping before close: " + sleepBeforeClose);
        Thread.sleep(sleepBeforeClose.millis());

        for (Client client : clients) {
            client.close();
        }
        for (Node node : nodes) {
            node.close();
        }

        System.out.println("Sleeping before exit: " + sleepBeforeClose);
        Thread.sleep(sleepAfterDone.millis());
    }

    class Searcher extends Thread {
        final int id;
        long counter = 0;
        long max = searcherIterations;

        Searcher(int id) {
            super("Searcher" + id);
            this.id = id;
        }

        @Override
        public void run() {
            try {
                barrier1.await();
                barrier2.await();
                for (; counter < max; counter++) {
                    Client client = client(counter);
                    QueryBuilder query = termQuery("num", counter % fieldNumLimit);
                    query = constantScoreQuery(queryFilter(query));

                    SearchResponse search = client.search(searchRequest()
                            .source(searchSource().query(query)))
                            .actionGet();
//                    System.out.println("Got search response, hits [" + search.hits().totalHits() + "]");
                }
            } catch (Exception e) {
                System.err.println("Failed to search:");
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        }
    }

    class Indexer extends Thread {

        final int id;
        long counter = 0;
        long max = indexIterations;

        Indexer(int id) {
            super("Indexer" + id);
            this.id = id;
        }

        Indexer max(int max) {
            this.max = max;
            return this;
        }

        @Override
        public void run() {
            try {
                barrier1.await();
                barrier2.await();
                for (; counter < max; counter++) {
                    Client client = client(counter);
                    long id = idGenerator.incrementAndGet();
                    client.index(Requests.indexRequest().index("test").type("type1").id(Long.toString(id))
                            .source(XContentFactory.jsonBuilder().startObject()
                                    .field("num", id % fieldNumLimit)
                                    .endObject()))
                            .actionGet();
                }
                System.out.println("Indexer [" + id + "]: Done");
            } catch (Exception e) {
                System.err.println("Failed to index:");
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        }
    }

    private Client client(long i) {
        return clients[((int) (i % clients.length))];
    }

    public static void main(String[] args) throws Exception {
        NodesStressTest test = new NodesStressTest()
                .numberOfNodes(2)
                .indexThreads(5)
                .indexIterations(10 * 1000)
                .searcherThreads(5)
                .searchIterations(10 * 1000)
                .sleepBeforeClose(TimeValue.timeValueMinutes(10))
                .sleepAfterDone(TimeValue.timeValueMinutes(10))
                .build(EMPTY_SETTINGS);

        test.start();
    }
}
