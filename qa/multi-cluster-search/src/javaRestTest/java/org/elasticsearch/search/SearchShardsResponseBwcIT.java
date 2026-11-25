/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TestRule;

import static org.hamcrest.Matchers.equalTo;

@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class SearchShardsResponseBwcIT extends ESRestTestCase {

    @ClassRule
    public static TestRule clusterRule = MultiClusterSearchClusters.CLUSTER_RULE;

    @BeforeClass
    public static void seedCcsRemoteClusterData() throws Exception {
        MultiClusterSearchClusters.beforeSuite();
    }

    private static final String REMOTE_CLUSTER_ALIAS = "my_remote_cluster";

    @Override
    protected String getTestRestCluster() {
        return MultiClusterSearchClusters.localClusterHosts();
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    static RestClient newRemoteClient() {
        return RestClient.builder(randomFrom(MultiClusterSearchClusters.remoteClusterHosts())).build();
    }

    public void testSkippedShardsPreservedAcrossVersions() throws Exception {
        try (RestClient remoteClient = newRemoteClient()) {
            // Create 5 single-shard indices on the new remote, each with docs in a distinct day
            for (int day = 1; day <= 5; day++) {
                String index = "test-day-" + day;
                createIndex(
                    remoteClient,
                    index,
                    Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0).build()
                );

                // Index 3 documents per day
                StringBuilder bulk = new StringBuilder();
                for (int doc = 0; doc < 3; doc++) {
                    String ts = Strings.format("2026-01-%02dT%02d:00:00Z", day, 10 + doc * 4);
                    bulk.append("{\"index\":{}}\n");
                    bulk.append("{\"@timestamp\":\"").append(ts).append("\",\"value\":").append(doc).append("}\n");
                }
                Request bulkReq = new Request("POST", "/" + index + "/_bulk");
                bulkReq.addParameter("refresh", "true");
                bulkReq.setJsonEntity(bulk.toString());
                Response bulkResp = remoteClient.performRequest(bulkReq);
                ObjectPath bulkPath = ObjectPath.createFromResponse(bulkResp);
                assertFalse("bulk indexing should not have errors", (boolean) bulkPath.evaluate("errors"));
            }
        }

        // CCS search from the local targeting all 5 indices on the remote.
        // The date range matches only day 1, so can-match should skip 4 shards.
        Request searchReq = new Request("POST", "/" + REMOTE_CLUSTER_ALIAS + ":test-day-*/_search");
        searchReq.addParameter("ccs_minimize_roundtrips", "false");
        searchReq.setJsonEntity("""
            {
                "query": {
                    "range": {
                        "@timestamp": {
                            "gte": "2026-01-01",
                            "lt": "2026-01-02"
                        }
                    }
                }
            }
            """);

        Response searchResp = client().performRequest(searchReq);
        ObjectPath result = ObjectPath.createFromResponse(searchResp);

        assertThat("should find the 3 documents from day 1", (int) result.evaluate("hits.total.value"), equalTo(3));
        assertThat("all 5 shards should be accounted for in the total", (int) result.evaluate("_shards.total"), equalTo(5));
        assertThat("4 non-matching shards should be reported as skipped", (int) result.evaluate("_shards.skipped"), equalTo(4));
        assertThat("all 5 shards should be successful", (int) result.evaluate("_shards.successful"), equalTo(5));
        assertThat("no shards should fail", (int) result.evaluate("_shards.failed"), equalTo(0));
    }
}
