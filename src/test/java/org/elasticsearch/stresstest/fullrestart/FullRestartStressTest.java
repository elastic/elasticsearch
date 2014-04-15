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

package org.elasticsearch.stresstest.fullrestart;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.FileSystemUtils;
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
import org.elasticsearch.node.internal.InternalNode;

import java.io.File;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

/**
 *
 */
public class FullRestartStressTest {

    private final ESLogger logger = Loggers.getLogger(getClass());

    private int numberOfNodes = 4;

    private boolean clearNodeWork = false;

    private int numberOfIndices = 5;
    private int textTokens = 150;
    private int numberOfFields = 10;
    private int bulkSize = 1000;
    private int numberOfDocsPerRound = 50000;

    private Settings settings = ImmutableSettings.Builder.EMPTY_SETTINGS;

    private TimeValue period = TimeValue.timeValueMinutes(20);

    private AtomicLong indexCounter = new AtomicLong();

    public FullRestartStressTest numberOfNodes(int numberOfNodes) {
        this.numberOfNodes = numberOfNodes;
        return this;
    }

    public FullRestartStressTest numberOfIndices(int numberOfIndices) {
        this.numberOfIndices = numberOfIndices;
        return this;
    }

    public FullRestartStressTest textTokens(int textTokens) {
        this.textTokens = textTokens;
        return this;
    }

    public FullRestartStressTest numberOfFields(int numberOfFields) {
        this.numberOfFields = numberOfFields;
        return this;
    }

    public FullRestartStressTest bulkSize(int bulkSize) {
        this.bulkSize = bulkSize;
        return this;
    }

    public FullRestartStressTest numberOfDocsPerRound(int numberOfDocsPerRound) {
        this.numberOfDocsPerRound = numberOfDocsPerRound;
        return this;
    }

    public FullRestartStressTest settings(Settings settings) {
        this.settings = settings;
        return this;
    }

    public FullRestartStressTest period(TimeValue period) {
        this.period = period;
        return this;
    }

    public FullRestartStressTest clearNodeWork(boolean clearNodeWork) {
        this.clearNodeWork = clearNodeWork;
        return this;
    }

    public void run() throws Exception {
        long numberOfRounds = 0;
        Random random = new Random(0);
        long testStart = System.currentTimeMillis();
        while (true) {
            Node[] nodes = new Node[numberOfNodes];
            for (int i = 0; i < nodes.length; i++) {
                nodes[i] = NodeBuilder.nodeBuilder().settings(settings).node();
            }
            Node client = NodeBuilder.nodeBuilder().settings(settings).client(true).node();

            // verify that the indices are there
            for (int i = 0; i < numberOfIndices; i++) {
                try {
                    client.client().admin().indices().prepareCreate("test" + i).execute().actionGet();
                } catch (Exception e) {
                    // might already exists, fine
                }
            }

            logger.info("*** Waiting for GREEN status");
            try {
                ClusterHealthResponse clusterHealth = client.client().admin().cluster().prepareHealth().setWaitForGreenStatus().setTimeout("10m").execute().actionGet();
                if (clusterHealth.isTimedOut()) {
                    logger.warn("timed out waiting for green status....");
                }
            } catch (Exception e) {
                logger.warn("failed to execute cluster health....");
            }

            CountResponse count = client.client().prepareCount().setQuery(matchAllQuery()).execute().actionGet();
            logger.info("*** index_count [{}], expected_count [{}]", count.getCount(), indexCounter.get());
            // verify count
            for (int i = 0; i < (nodes.length * 5); i++) {
                count = client.client().prepareCount().setQuery(matchAllQuery()).execute().actionGet();
                logger.debug("index_count [{}], expected_count [{}]", count.getCount(), indexCounter.get());
                if (count.getCount() != indexCounter.get()) {
                    logger.warn("!!! count does not match, index_count [{}], expected_count [{}]", count.getCount(), indexCounter.get());
                    throw new Exception("failed test, count does not match...");
                }
            }

            // verify search
            for (int i = 0; i < (nodes.length * 5); i++) {
                // do a search with norms field, so we don't rely on match all filtering cache
                SearchResponse search = client.client().prepareSearch().setQuery(matchAllQuery().normsField("field")).execute().actionGet();
                logger.debug("index_count [{}], expected_count [{}]", search.getHits().totalHits(), indexCounter.get());
                if (count.getCount() != indexCounter.get()) {
                    logger.warn("!!! search does not match, index_count [{}], expected_count [{}]", search.getHits().totalHits(), indexCounter.get());
                    throw new Exception("failed test, count does not match...");
                }
            }

            logger.info("*** ROUND {}", ++numberOfRounds);
            // bulk index data
            int numberOfBulks = numberOfDocsPerRound / bulkSize;
            for (int b = 0; b < numberOfBulks; b++) {
                BulkRequestBuilder bulk = client.client().prepareBulk();
                for (int k = 0; k < bulkSize; k++) {
                    StringBuilder sb = new StringBuilder();
                    XContentBuilder json = XContentFactory.jsonBuilder().startObject()
                            .field("field", "value" + ThreadLocalRandom.current().nextInt());

                    int fields = ThreadLocalRandom.current().nextInt() % numberOfFields;
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

                    bulk.add(Requests.indexRequest("test" + (Math.abs(ThreadLocalRandom.current().nextInt()) % numberOfIndices)).type("type1").source(json));
                    indexCounter.incrementAndGet();
                }
                bulk.execute().actionGet();
            }

            client.close();
            for (Node node : nodes) {
                File[] nodeDatas = ((InternalNode) node).injector().getInstance(NodeEnvironment.class).nodeDataLocations();
                node.close();
                if (clearNodeWork && !settings.get("gateway.type").equals("local")) {
                    FileSystemUtils.deleteRecursively(nodeDatas);
                }
            }

            if ((System.currentTimeMillis() - testStart) > period.millis()) {
                logger.info("test finished, full_restart_rounds [{}]", numberOfRounds);
                break;
            }

        }
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("es.logger.prefix", "");

        int numberOfNodes = 2;
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("index.shard.check_on_startup", true)
                .put("gateway.type", "local")
                .put("gateway.recover_after_nodes", numberOfNodes)
                .put("index.number_of_shards", 1)
                .put("path.data", "data/data1,data/data2")
                .build();

        FullRestartStressTest test = new FullRestartStressTest()
                .settings(settings)
                .period(TimeValue.timeValueMinutes(20))
                .clearNodeWork(false) // only applies to shared gateway
                .numberOfNodes(numberOfNodes)
                .numberOfIndices(1)
                .textTokens(150)
                .numberOfFields(10)
                .bulkSize(1000)
                .numberOfDocsPerRound(10000);

        test.run();
    }
}
