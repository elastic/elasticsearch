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
package org.elasticsearch.benchmark.recovery;

import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.recovery.ShardRecoveryResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.common.jna.Natives;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.BackgroundIndexer;
import org.elasticsearch.transport.TransportModule;

import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 *
 */
public class ReplicaRecoveryBenchmark {

    private static final String INDEX_NAME = "index";
    private static final String TYPE_NAME = "type";


    static int DOC_COUNT = (int) SizeValue.parseSizeValue("40k").singles();
    static int CONCURRENT_INDEXERS = 2;

    public static void main(String[] args) throws Exception {
        System.setProperty("es.logger.prefix", "");
        Natives.tryMlockall();

        Settings settings = settingsBuilder()
                .put("gateway.type", "local")
                .put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED, "false")
                .put(SETTING_NUMBER_OF_SHARDS, 1)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .put(TransportModule.TRANSPORT_TYPE_KEY, "local")
                .build();

        String clusterName = ReplicaRecoveryBenchmark.class.getSimpleName();
        Node node1 = nodeBuilder().clusterName(clusterName)
                .settings(settingsBuilder().put(settings))
                .node();

        final ESLogger logger = ESLoggerFactory.getLogger("benchmark");

        final Client client1 = node1.client();
        client1.admin().cluster().prepareUpdateSettings().setPersistentSettings("logger.indices.recovery: TRACE").get();
        final BackgroundIndexer indexer = new BackgroundIndexer(INDEX_NAME, TYPE_NAME, client1, 0, CONCURRENT_INDEXERS, false, new Random());
        indexer.setMinFieldSize(10);
        indexer.setMaxFieldSize(150);
        try {
            client1.admin().indices().prepareDelete(INDEX_NAME).get();
        } catch (IndexMissingException e) {
        }
        client1.admin().indices().prepareCreate(INDEX_NAME).get();
        indexer.start(DOC_COUNT / 2);
        while (indexer.totalIndexedDocs() < DOC_COUNT / 2) {
            Thread.sleep(5000);
            logger.info("--> indexed {} of {}", indexer.totalIndexedDocs(), DOC_COUNT);
        }
        client1.admin().indices().prepareFlush().get();
        indexer.continueIndexing(DOC_COUNT / 2);
        while (indexer.totalIndexedDocs() < DOC_COUNT) {
            Thread.sleep(5000);
            logger.info("--> indexed {} of {}", indexer.totalIndexedDocs(), DOC_COUNT);
        }


        logger.info("--> starting another node and allocating a shard on it");

        Node node2 = nodeBuilder().clusterName(clusterName)
                .settings(settingsBuilder().put(settings))
                .node();

        client1.admin().indices().prepareUpdateSettings(INDEX_NAME).setSettings(IndexMetaData.SETTING_NUMBER_OF_REPLICAS + ": 1").get();

        final AtomicBoolean end = new AtomicBoolean(false);

        final Thread backgroundLogger = new Thread(new Runnable() {

            long lastTime = System.currentTimeMillis();
            long lastDocs = indexer.totalIndexedDocs();
            long lastBytes = 0;
            long lastTranslogOps = 0;

            @Override
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {

                    }
                    if (end.get()) {
                        return;
                    }
                    long currentTime = System.currentTimeMillis();
                    long currentDocs = indexer.totalIndexedDocs();
                    RecoveryResponse recoveryResponse = client1.admin().indices().prepareRecoveries(INDEX_NAME).setActiveOnly(true).get();
                    List<ShardRecoveryResponse> indexRecoveries = recoveryResponse.shardResponses().get(INDEX_NAME);
                    long translogOps;
                    long bytes;
                    if (indexRecoveries.size() > 0) {
                        translogOps = indexRecoveries.get(0).recoveryState().getTranslog().recoveredOperations();
                        bytes = recoveryResponse.shardResponses().get(INDEX_NAME).get(0).recoveryState().getIndex().recoveredBytes();
                    } else {
                        bytes = lastBytes = 0;
                        translogOps = lastTranslogOps = 0;
                    }
                    float seconds = (currentTime - lastTime) / 1000.0F;
                    logger.info("--> indexed [{}];[{}] doc/s, recovered [{}] MB/s , translog ops [{}]/s ",
                            currentDocs, (currentDocs - lastDocs) / seconds,
                            (bytes - lastBytes) / 1024.0F / 1024F / seconds, (translogOps - lastTranslogOps) / seconds);
                    lastBytes = bytes;
                    lastTranslogOps = translogOps;
                    lastTime = currentTime;
                    lastDocs = currentDocs;
                }
            }
        });

        backgroundLogger.start();

        client1.admin().cluster().prepareHealth().setWaitForGreenStatus().get();

        logger.info("--> green. starting relocation cycles");

        long startDocIndexed = indexer.totalIndexedDocs();
        indexer.continueIndexing(DOC_COUNT * 50);

        long totalRecoveryTime = 0;
        long startTime = System.currentTimeMillis();
        long[] recoveryTimes = new long[3];
        for (int iteration = 0; iteration < 3; iteration++) {
            logger.info("--> removing replicas");
            client1.admin().indices().prepareUpdateSettings(INDEX_NAME).setSettings(IndexMetaData.SETTING_NUMBER_OF_REPLICAS + ": 0").get();
            logger.info("--> adding replica again");
            long recoveryStart = System.currentTimeMillis();
            client1.admin().indices().prepareUpdateSettings(INDEX_NAME).setSettings(IndexMetaData.SETTING_NUMBER_OF_REPLICAS + ": 1").get();
            client1.admin().cluster().prepareHealth(INDEX_NAME).setWaitForGreenStatus().setTimeout("15m").get();
            long recoveryTime = System.currentTimeMillis() - recoveryStart;
            totalRecoveryTime += recoveryTime;
            recoveryTimes[iteration] = recoveryTime;
            logger.info("--> recovery done in [{}]", new TimeValue(recoveryTime));

            // sleep some to let things clean up
            Thread.sleep(10000);
        }

        long endDocIndexed = indexer.totalIndexedDocs();
        long totalTime = System.currentTimeMillis() - startTime;
        indexer.stop();

        end.set(true);

        backgroundLogger.interrupt();

        backgroundLogger.join();

        logger.info("average doc/s [{}], average relocation time [{}], taking [{}], [{}], [{}]", (endDocIndexed - startDocIndexed) * 1000.0 / totalTime, new TimeValue(totalRecoveryTime / 3),
                TimeValue.timeValueMillis(recoveryTimes[0]), TimeValue.timeValueMillis(recoveryTimes[1]), TimeValue.timeValueMillis(recoveryTimes[2])
        );

        client1.close();
        node1.close();
        node2.close();
    }
}
