/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.apache.lucene.tests.mockfile.HandleLimitFS;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.function.Function;
import java.util.stream.Collectors;

@HandleLimitFS.MaxOpenHandles(limit = 100 * 1024)
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ClusterAllocationTests extends ESIntegTestCase {

    public void testBalanced() {
        test("balanced");
    }

    public void testDesiredBalance() {
        test("desired_balance");
    }

    /**
     * Might require setting higher soft/hard limits for max files with `ulimit -n unlimited`
     */
    private void test(String type) {

        var clusterSettings = Settings.builder()
            .put("cluster.routing.allocation.type", type)
            .build();

        for (int i = 0; i < 9; i++) {
            internalCluster().startNode(clusterSettings);
        }


        for (int i = 0; i < 1000; i++) {
            var indexName = "index-" + i;
            var indexSettings = Settings.builder()
                .put("index.number_of_shards", 5)
                .put("index.number_of_replicas", 0)
                .build();
            client().admin().indices().prepareCreate(indexName).setSettings(indexSettings).get();
        }

        var results = new ArrayList<Long>();
        for (int i = 1000; i < 1100; i++) {
            var indexName = "index-" + i;
            var indexSettings = Settings.builder()
                .put("index.number_of_shards", 5)
                .put("index.number_of_replicas", 0)
                .build();
            var start = System.nanoTime();
            client().admin().indices().prepareCreate(indexName).setSettings(indexSettings).get();
            var duration = (System.nanoTime() - start) / 1000000;
            logger.info("Created [{}] in {} ms", i, duration);
            results.add(duration);
        }

        Collections.sort(results);
        var stats = results.stream().collect(Collectors.summarizingLong(it -> it));

        logger.info("Allocator [{}]. avg: {}, max: {}, p99: {}, p95: {}",
            type, stats.getAverage(), stats.getMax(), results.get(99), results.get(95));
    }
}
