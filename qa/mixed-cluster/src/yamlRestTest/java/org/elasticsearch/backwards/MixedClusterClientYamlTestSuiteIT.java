/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.backwards;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

@TimeoutSuite(millis = 60 * TimeUnits.MINUTE) // some of the windows test VMs are slow as hell
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class MixedClusterClientYamlTestSuiteIT extends ESClientYamlSuiteTestCase {

    /** Bounds the per-copy searches of {@link #dumpClusterStateOnFailure()}. */
    private static final int MAX_COPIES_TO_SEARCH = 20;

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.CLUSTER;

    public MixedClusterClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return createParameters();
    }

    @Override
    protected boolean randomizeContentType() {
        return false;
    }

    /**
     * Logs which node holds which shard copy, recent recoveries, per-copy refresh, indexing and seq_no stats, and the
     * number of docs a search on each started copy sees. A rare flake in this suite has a search right after
     * {@code indices.refresh} miss all docs of a new index while every shard reports success (#161650, #151793),
     * pointing at one copy not exposing the refreshed docs; this records which copy and whether it was refreshed.
     */
    @Override
    protected void dumpClusterStateOnFailure() throws IOException {
        logger.info("Nodes on test failure:\n{}", cat("/_cat/nodes?v&h=id,name,version,master&full_id=true"));
        logger.info("Shards on test failure:\n{}", cat("/_cat/shards?v&h=index,shard,prirep,state,docs,node,id&s=index,shard,prirep"));
        logger.info(
            "Recoveries on test failure:\n{}",
            cat(
                "/_cat/recovery?v&h=index,shard,type,stage,source_node,target_node,start_time_millis,stop_time_millis,"
                    + "translog_ops_recovered&s=index,shard"
            )
        );

        final StringBuilder copies = new StringBuilder();
        final Response stats = adminClient().performRequest(new Request("GET", "/_stats/docs,indexing,refresh?level=shards"));
        final Map<String, Object> indices = ObjectPath.createFromResponse(stats).evaluate("indices");
        int searched = 0;
        for (var index : indices.entrySet()) {
            final Map<String, Object> shards = ObjectPath.evaluate(index.getValue(), "shards");
            for (var shard : shards.entrySet()) {
                for (Object copy : (List<?>) shard.getValue()) {
                    final String nodeId = ObjectPath.evaluate(copy, "routing.node");
                    copies.append(index.getKey())
                        .append('[')
                        .append(shard.getKey())
                        .append("][")
                        .append(Boolean.TRUE.equals(ObjectPath.evaluate(copy, "routing.primary")) ? 'p' : 'r')
                        .append("] node ")
                        .append(nodeId)
                        .append(" docs ")
                        .append(ObjectPath.<Object>evaluate(copy, "docs.count"))
                        .append(" index_total ")
                        .append(ObjectPath.<Object>evaluate(copy, "indexing.index_total"))
                        .append(" refresh_total ")
                        .append(ObjectPath.<Object>evaluate(copy, "refresh.total"))
                        .append(" external_refresh_total ")
                        .append(ObjectPath.<Object>evaluate(copy, "refresh.external_total"))
                        .append(" max_seq_no ")
                        .append(ObjectPath.<Object>evaluate(copy, "seq_no.max_seq_no"))
                        .append(" local_checkpoint ")
                        .append(ObjectPath.<Object>evaluate(copy, "seq_no.local_checkpoint"))
                        .append(" global_checkpoint ")
                        .append(ObjectPath.<Object>evaluate(copy, "seq_no.global_checkpoint"));
                    if (searched++ < MAX_COPIES_TO_SEARCH) {
                        final Request search = new Request("POST", "/" + index.getKey() + "/_search");
                        search.addParameter("size", "0");
                        search.addParameter("rest_total_hits_as_int", "true");
                        search.addParameter("preference", "_shards:" + shard.getKey() + "|_only_nodes:" + nodeId);
                        copies.append(" search_hits ");
                        try {
                            copies.append(
                                ObjectPath.createFromResponse(adminClient().performRequest(search)).<Object>evaluate("hits.total")
                            );
                        } catch (IOException e) {
                            copies.append("failed: ").append(e.getMessage());
                        }
                    }
                    copies.append('\n');
                }
            }
        }
        logger.info("Shard copies on test failure:\n{}", copies);
    }

    private static String cat(String path) throws IOException {
        final Response response = adminClient().performRequest(new Request("GET", path));
        return new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
    }
}
