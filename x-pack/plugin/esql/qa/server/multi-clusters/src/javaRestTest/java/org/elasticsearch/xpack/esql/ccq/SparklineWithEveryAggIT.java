/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.ccq;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.qa.rest.SparklineWithEveryAggTestCase;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.List;

@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class SparklineWithEveryAggIT extends SparklineWithEveryAggTestCase {
    static ElasticsearchCluster remoteCluster = Clusters.remoteCluster();
    static ElasticsearchCluster localCluster = Clusters.localCluster(remoteCluster);

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(remoteCluster).around(localCluster);

    private static RestClient remoteClient;

    public SparklineWithEveryAggIT(AggTestCase testCase) {
        super(testCase);
    }

    @Override
    protected String getTestRestCluster() {
        return localCluster.getHttpAddresses();
    }

    @Before
    public void assumeSparklineSupported() throws IOException {
        assumeTrue(
            requiredCapabilities() + " not supported by all clusters",
            clusterHasCapability("POST", "/_query", List.of(), requiredCapabilities()).orElse(false)
                && clusterHasCapability(remoteClient(), "POST", "/_query", List.of(), requiredCapabilities()).orElse(false)
        );
    }

    @Override
    protected String indexPattern() {
        return Clusters.REMOTE_CLUSTER_NAME + ":" + INDEX_NAME + "," + INDEX_NAME;
    }

    @Override
    protected void doEnsureIndex() throws IOException {
        createIndexAndData(client());
        createIndexAndData(remoteClient());
    }

    private RestClient remoteClient() throws IOException {
        if (remoteClient == null) {
            List<HttpHost> clusterHosts = parseClusterHosts(remoteCluster.getHttpAddresses());
            remoteClient = buildClient(restClientSettings(), clusterHosts.toArray(new HttpHost[0]));
        }
        return remoteClient;
    }

    @AfterClass
    public static void closeRemoteClient() throws IOException {
        try {
            IOUtils.close(remoteClient);
        } finally {
            remoteClient = null;
        }
    }
}
