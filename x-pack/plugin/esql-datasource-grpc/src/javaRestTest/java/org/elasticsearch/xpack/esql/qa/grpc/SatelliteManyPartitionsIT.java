/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.grpc;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.client.Request;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.datasource.grpc.BlockingSatelliteServer;
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.junit.ClassRule;

import java.util.List;
import java.util.Map;

/**
 * A table with many partitions is read in full. Above 32 splits the planner groups them (a coalesced split) to save drivers; every
 * partition must still be read, whether the query is pushed or not. A connector that is handed the group instead of its
 * partitions reads the wrong data and nothing says so: 100 partitions came back as 28 reads.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class SatelliteManyPartitionsIT extends ESRestTestCase {

    private static final int PARTITIONS = 100;
    private static final int ROWS_PER_PARTITION = 3; // what BlockingSatelliteServer sends for each stream

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .nodes(2)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting(Federation.FEDERATION_ENABLED.getKey(), "true")
        .setting("xpack.ml.enabled", "false")
        .jvmArg("--add-opens=java.base/java.nio=ALL-UNNAMED")
        .jvmArg("-Darrow.allocation.manager.type=Unsafe")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    private long count(String dataset, boolean pushdown) throws Exception {
        Request query = new Request("POST", "/_query");
        query.setJsonEntity(
            "{\"query\": \"FROM " + dataset + " | STATS c = COUNT(*) | LIMIT 5\", \"pragma\": {\"satellite_pushdown\": " + pushdown + "}}"
        );
        Map<String, Object> response = entityAsMap(client().performRequest(query));
        return ((Number) ((List<?>) ((List<?>) response.get("values")).get(0)).get(0)).longValue();
    }

    public void testEveryPartitionIsReadWhateverTheDistribution() throws Exception {
        try (BlockingSatelliteServer satellite = new BlockingSatelliteServer()) {
            satellite.endpointCount = PARTITIONS;
            Request source = new Request("PUT", "/_query/data_source/many_src");
            source.setJsonEntity("{\"type\": \"flight\"}");
            client().performRequest(source);
            Request dataset = new Request("PUT", "/_query/dataset/many_ds");
            dataset.setJsonEntity("{\"data_source\": \"many_src\", \"resource\": \"flight://127.0.0.1:" + satellite.port() + "/t\"}");
            client().performRequest(dataset);

            for (boolean pushdown : new boolean[] { false, true }) {
                int before = satellite.doGetCalls.get();
                assertEquals(
                    "every partition is read (pushdown " + pushdown + ")",
                    (long) PARTITIONS * ROWS_PER_PARTITION,
                    count("many_ds", pushdown)
                );
                assertEquals("one stream per partition (pushdown " + pushdown + ")", PARTITIONS, satellite.doGetCalls.get() - before);
            }
        }
    }
}
