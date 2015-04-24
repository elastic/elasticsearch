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

package org.elasticsearch.stresstest.rollingrestart;

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHit;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

/**
 *
 */
public class RollingRestartStressTest {

    private final ESLogger logger = Loggers.getLogger(getClass());

    private int numberOfShards = 5;
    private int numberOfReplicas = 1;
    private int numberOfNodes = 4;

    private int textTokens = 150;
    private int numberOfFields = 10;
    private long initialNumberOfDocs = 100000;

    private int indexers = 0;

    private TimeValue indexerThrottle = TimeValue.timeValueMillis(100);

    private Settings settings = ImmutableSettings.Builder.EMPTY_SETTINGS;

    private TimeValue period = TimeValue.timeValueMinutes(20);

    private boolean clearNodeData = true;

    private Node client;

    private AtomicLong indexCounter = new AtomicLong();
    private AtomicLong idCounter = new AtomicLong();


    public RollingRestartStressTest numberOfNodes(int numberOfNodes) {
        this.numberOfNodes = numberOfNodes;
        return this;
    }

    public RollingRestartStressTest numberOfShards(int numberOfShards) {
        this.numberOfShards = numberOfShards;
        return this;
    }

    public RollingRestartStressTest numberOfReplicas(int numberOfReplicas) {
        this.numberOfReplicas = numberOfReplicas;
        return this;
    }

    public RollingRestartStressTest initialNumberOfDocs(long initialNumberOfDocs) {
        this.initialNumberOfDocs = initialNumberOfDocs;
        return this;
    }

    public RollingRestartStressTest textTokens(int textTokens) {
        this.textTokens = textTokens;
        return this;
    }

    public RollingRestartStressTest numberOfFields(int numberOfFields) {
        this.numberOfFields = numberOfFields;
        return this;
    }

    public RollingRestartStressTest indexers(int indexers) {
        this.indexers = indexers;
        return this;
    }

    public RollingRestartStressTest indexerThrottle(TimeValue indexerThrottle) {
        this.indexerThrottle = indexerThrottle;
        return this;
    }

    public RollingRestartStressTest period(TimeValue period) {
        this.period = period;
        return this;
    }

    public RollingRestartStressTest cleanNodeData(boolean clearNodeData) {
        this.clearNodeData = clearNodeData;
        return this;
    }

    public RollingRestartStressTest settings(Settings settings) {
        this.settings = settings;
        return this;
    }

    public void run() throws Exception {
        Random random = new Random(0);

        Node[] nodes = new Node[numberOfNodes];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = NodeBuilder.nodeBuilder().settings(settings).node();
        }
        client = NodeBuilder.nodeBuilder().settings(settings).client(true).node();

        client.client().admin().indices().prepareCreate("test").setSettings(settingsBuilder()
                .put("index.number_of_shards", numberOfShards)
                .put("index.number_of_replicas", numberOfReplicas)
        ).execute().actionGet();

        logger.info("********** [START] INDEXING INITIAL DOCS");
        for (long i = 0; i < initialNumberOfDocs; i++) {
            indexDoc(random);
        }
        logger.info("********** [DONE ] INDEXING INITIAL DOCS");

        Indexer[] indexerThreads = new Indexer[indexers];
        for (int i = 0; i < indexerThreads.length; i++) {
            indexerThreads[i] = new Indexer();
        }
        for (int i = 0; i < indexerThreads.length; i++) {
            indexerThreads[i].start();
        }

        long testStart = System.currentTimeMillis();

        // start doing the rolling restart
        int nodeIndex = 0;
        while (true) {
            Path[] nodeData = nodes[nodeIndex].injector().getInstance(NodeEnvironment.class).nodeDataPaths();
            nodes[nodeIndex].close();
            if (clearNodeData) {
                try {
                    IOUtils.rm(nodeData);
                } catch (Exception ex) {
                     logger.debug("Failed to delete node data directories", ex);

                }
            }

            try {
                ClusterHealthResponse clusterHealth = client.client().admin().cluster().prepareHealth()
                        .setWaitForGreenStatus()
                        .setWaitForNodes(Integer.toString(numberOfNodes + 0 /* client node*/))
                        .setWaitForRelocatingShards(0)
                        .setTimeout("10m").execute().actionGet();
                if (clusterHealth.isTimedOut()) {
                    logger.warn("timed out waiting for green status....");
                }
            } catch (Exception e) {
                logger.warn("failed to execute cluster health....");
            }

            nodes[nodeIndex] = NodeBuilder.nodeBuilder().settings(settings).node();

            Thread.sleep(1000);

            try {
                ClusterHealthResponse clusterHealth = client.client().admin().cluster().prepareHealth()
                        .setWaitForGreenStatus()
                        .setWaitForNodes(Integer.toString(numberOfNodes + 1 /* client node*/))
                        .setWaitForRelocatingShards(0)
                        .setTimeout("10m").execute().actionGet();
                if (clusterHealth.isTimedOut()) {
                    logger.warn("timed out waiting for green status....");
                }
            } catch (Exception e) {
                logger.warn("failed to execute cluster health....");
            }

            if (++nodeIndex == nodes.length) {
                nodeIndex = 0;
            }

            if ((System.currentTimeMillis() - testStart) > period.millis()) {
                logger.info("test finished");
                break;
            }
        }

        for (int i = 0; i < indexerThreads.length; i++) {
            indexerThreads[i].close = true;
        }

        Thread.sleep(indexerThrottle.millis() + 10000);

        for (int i = 0; i < indexerThreads.length; i++) {
            if (!indexerThreads[i].closed) {
                logger.warn("thread not closed!");
            }
        }

        client.client().admin().indices().prepareRefresh().execute().actionGet();

        // check the count
        for (int i = 0; i < (nodes.length * 5); i++) {
            CountResponse count = client.client().prepareCount().setQuery(matchAllQuery()).execute().actionGet();
            logger.info("indexed [{}], count [{}], [{}]", count.getCount(), indexCounter.get(), count.getCount() == indexCounter.get() ? "OK" : "FAIL");
            if (count.getCount() != indexCounter.get()) {
                logger.warn("count does not match!");
            }
        }

        // scan all the docs, verify all have the same version based on the number of replicas
        SearchResponse searchResponse = client.client().prepareSearch()
                .setSearchType(SearchType.SCAN)
                .setQuery(matchAllQuery())
                .setSize(50)
                .setScroll(TimeValue.timeValueMinutes(2))
                .execute().actionGet();
        logger.info("Verifying versions for {} hits...", searchResponse.getHits().totalHits());

        while (true) {
            searchResponse = client.client().prepareSearchScroll(searchResponse.getScrollId()).setScroll(TimeValue.timeValueMinutes(2)).execute().actionGet();
            if (searchResponse.getFailedShards() > 0) {
                logger.warn("Search Failures " + Arrays.toString(searchResponse.getShardFailures()));
            }
            for (SearchHit hit : searchResponse.getHits()) {
                long version = -1;
                for (int i = 0; i < (numberOfReplicas + 1); i++) {
                    GetResponse getResponse = client.client().prepareGet(hit.index(), hit.type(), hit.id()).execute().actionGet();
                    if (version == -1) {
                        version = getResponse.getVersion();
                    } else {
                        if (version != getResponse.getVersion()) {
                            logger.warn("Doc {} has different version numbers {} and {}", hit.id(), version, getResponse.getVersion());
                        }
                    }
                }
            }
            if (searchResponse.getHits().hits().length == 0) {
                break;
            }
        }
        logger.info("Done verifying versions");

        client.close();
        for (Node node : nodes) {
            node.close();
        }
    }

    private class Indexer extends Thread {

        volatile boolean close = false;

        volatile boolean closed = false;

        @Override
        public void run() {
            Random random = new Random(0);
            while (true) {
                if (close) {
                    closed = true;
                    return;
                }
                try {
                    indexDoc(random);
                    Thread.sleep(indexerThrottle.millis());
                } catch (Exception e) {
                    logger.warn("failed to index / sleep", e);
                }
            }
        }
    }

    private void indexDoc(Random random) throws Exception {
        StringBuilder sb = new StringBuilder();
        XContentBuilder json = XContentFactory.jsonBuilder().startObject()
                .field("field", "value" + ThreadLocalRandom.current().nextInt());

        int fields = Math.abs(ThreadLocalRandom.current().nextInt()) % numberOfFields;
        for (int i = 0; i < fields; i++) {
            json.field("num_" + i, ThreadLocalRandom.current().nextDouble());
            int tokens = ThreadLocalRandom.current().nextInt() % textTokens;
            sb.setLength(0);
            for (int j = 0; j < tokens; j++) {
                sb.append(Strings.randomBase64UUID(random)).append(' ');
            }
            json.field("text_" + i, sb.toString());
        }

        json.endObject();

        String id = Long.toString(idCounter.incrementAndGet());
        client.client().prepareIndex("test", "type1", id)
                .setCreate(true)
                .setSource(json)
                .execute().actionGet();
        indexCounter.incrementAndGet();
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("es.logger.prefix", "");

        Settings settings = settingsBuilder()
                .put("index.shard.check_on_startup", true)
                .put("path.data", "data/data1,data/data2")
                .build();

        RollingRestartStressTest test = new RollingRestartStressTest()
                .settings(settings)
                .numberOfNodes(4)
                .numberOfShards(5)
                .numberOfReplicas(1)
                .initialNumberOfDocs(1000)
                .textTokens(150)
                .numberOfFields(10)
                .cleanNodeData(false)
                .indexers(5)
                .indexerThrottle(TimeValue.timeValueMillis(50))
                .period(TimeValue.timeValueMinutes(3));

        test.run();
    }
}
