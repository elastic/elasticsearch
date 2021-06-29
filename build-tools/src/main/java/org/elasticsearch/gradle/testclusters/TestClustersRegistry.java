/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gradle.testclusters;

import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.services.BuildService;
import org.gradle.api.services.BuildServiceParameters;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public abstract class TestClustersRegistry implements BuildService<BuildServiceParameters.None> {
    private static final Logger logger = Logging.getLogger(TestClustersRegistry.class);
    private static final String TESTCLUSTERS_INSPECT_FAILURE = "testclusters.inspect.failure";
    private final Boolean allowClusterToSurvive = Boolean.valueOf(System.getProperty(TESTCLUSTERS_INSPECT_FAILURE, "false"));
    private final Map<ElasticsearchCluster, Integer> claimsInventory = new HashMap<>();
    private final Set<ElasticsearchCluster> runningClusters = new HashSet<>();

    public void claimCluster(ElasticsearchCluster cluster) {
        cluster.freeze();
        claimsInventory.put(cluster, claimsInventory.getOrDefault(cluster, 0) + 1);
    }

    public void maybeStartCluster(ElasticsearchCluster cluster) {
        if (runningClusters.contains(cluster)) {
            return;
        }
        runningClusters.add(cluster);
        cluster.start();
    }

    public void stopCluster(ElasticsearchCluster cluster, boolean taskFailed) {
        if (taskFailed) {
            // If the task fails, and other tasks use this cluster, the other task will likely never be
            // executed at all, so we will never be called again to un-claim and terminate it.
            if (allowClusterToSurvive) {
                logger.info("Not stopping clusters, disabled by property");
                // task failed or this is the last one to stop
                for (int i = 1;; i += i) {
                    logger.lifecycle(
                        "No more test clusters left to run, going to sleep because {} was set," + " interrupt (^C) to stop clusters.",
                        TESTCLUSTERS_INSPECT_FAILURE
                    );
                    try {
                        Thread.sleep(1000 * i);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            } else {
                cluster.stop(true);
                runningClusters.remove(cluster);
            }
        } else {
            int currentClaims = claimsInventory.getOrDefault(cluster, 0) - 1;
            claimsInventory.put(cluster, currentClaims);

            if (currentClaims <= 0 && runningClusters.contains(cluster)) {
                cluster.stop(false);
                runningClusters.remove(cluster);
            }
        }
    }

}
