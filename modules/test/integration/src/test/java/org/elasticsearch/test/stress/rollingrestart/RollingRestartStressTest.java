/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.test.stress.rollingrestart;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.common.UUID;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.jsr166y.ThreadLocalRandom;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.node.internal.InternalNode;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.index.query.xcontent.QueryBuilders.*;

/**
 * @author kimchy (shay.banon)
 */
public class RollingRestartStressTest {

    private final ESLogger logger = Loggers.getLogger(getClass());

    private int numberOfNodes = 4;

    private int textTokens = 150;
    private int numberOfFields = 10;
    private long initialNumberOfDocs = 100000;

    private int indexers = 0;

    private TimeValue indexerThrottle = TimeValue.timeValueMillis(100);

    private Settings settings = ImmutableSettings.Builder.EMPTY_SETTINGS;

    private TimeValue period = TimeValue.timeValueMinutes(20);

    private boolean clearNodeWork = true;

    private Node client;

    private AtomicLong indexCounter = new AtomicLong();


    public RollingRestartStressTest numberOfNodes(int numberOfNodes) {
        this.numberOfNodes = numberOfNodes;
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

    public RollingRestartStressTest cleanNodeWork(boolean clearNodeWork) {
        this.clearNodeWork = clearNodeWork;
        return this;
    }

    public RollingRestartStressTest settings(Settings settings) {
        this.settings = settings;
        return this;
    }

    public void run() throws Exception {
        Node[] nodes = new Node[numberOfNodes];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = NodeBuilder.nodeBuilder().settings(settings).node();
        }
        client = NodeBuilder.nodeBuilder().settings(settings).client(true).node();

        logger.info("********** [START] INDEXING INITIAL DOCS");
        for (long i = 0; i < initialNumberOfDocs; i++) {
            indexDoc();
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
            File nodeWork = ((InternalNode) nodes[nodeIndex]).injector().getInstance(NodeEnvironment.class).nodeLocation();
            nodes[nodeIndex].close();
            if (clearNodeWork) {
                FileSystemUtils.deleteRecursively(nodeWork);
            }
            nodes[nodeIndex] = NodeBuilder.nodeBuilder().settings(settings).node();

            try {
                ClusterHealthResponse clusterHealth = client.client().admin().cluster().prepareHealth().setWaitForGreenStatus().setTimeout("10m").execute().actionGet();
                if (clusterHealth.timedOut()) {
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
            logger.info("indexed [{}], count [{}]", count.count(), indexCounter.get());
            if (count.count() != indexCounter.get()) {
                logger.warn("count does not match!");
            }
        }

        client.close();
        for (Node node : nodes) {
            node.close();
        }
    }

    private class Indexer extends Thread {

        volatile boolean close = false;

        volatile boolean closed = false;

        @Override public void run() {
            while (true) {
                if (close) {
                    closed = true;
                    return;
                }
                try {
                    indexDoc();
                    Thread.sleep(indexerThrottle.millis());
                } catch (Exception e) {
                    logger.warn("failed to index / sleep", e);
                }
            }
        }
    }

    private void indexDoc() throws Exception {
        StringBuffer sb = new StringBuffer();
        XContentBuilder json = XContentFactory.jsonBuilder().startObject()
                .field("field", "value" + ThreadLocalRandom.current().nextInt());

        int fields = Math.abs(ThreadLocalRandom.current().nextInt()) % numberOfFields;
        for (int i = 0; i < fields; i++) {
            json.field("num_" + i, ThreadLocalRandom.current().nextDouble());
            int tokens = ThreadLocalRandom.current().nextInt() % textTokens;
            sb.setLength(0);
            for (int j = 0; j < tokens; j++) {
                sb.append(UUID.randomBase64UUID()).append(' ');
            }
            json.field("text_" + i, sb.toString());
        }

        json.endObject();

        client.client().prepareIndex("test", "type1")
                .setSource(json)
                .execute().actionGet();
        indexCounter.incrementAndGet();
    }

    public static void main(String[] args) throws Exception {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("index.shard.check_index", true)
                .put("gateway.type", "none")
                .build();

        RollingRestartStressTest test = new RollingRestartStressTest()
                .settings(settings)
                .numberOfNodes(4)
                .initialNumberOfDocs(100000)
                .textTokens(150)
                .numberOfFields(10)
                .cleanNodeWork(true)
                .indexers(5)
                .indexerThrottle(TimeValue.timeValueMillis(50))
                .period(TimeValue.timeValueMinutes(10));

        test.run();
    }
}
