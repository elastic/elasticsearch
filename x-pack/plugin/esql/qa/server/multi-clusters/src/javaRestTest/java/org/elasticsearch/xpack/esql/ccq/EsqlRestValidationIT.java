/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.ccq;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.HttpHost;
import org.elasticsearch.Version;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.qa.rest.EsqlRestValidationTestCase;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.StringJoiner;

import static org.elasticsearch.xpack.esql.ccq.Clusters.REMOTE_CLUSTER_NAME;

@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class EsqlRestValidationIT extends EsqlRestValidationTestCase {
    static ElasticsearchCluster remoteCluster = Clusters.remoteCluster();
    static ElasticsearchCluster localCluster = Clusters.localCluster(remoteCluster);

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(remoteCluster).around(localCluster);
    private static RestClient remoteClient;

    @Override
    protected String getTestRestCluster() {
        return localCluster.getHttpAddresses();
    }

    @AfterClass
    public static void closeRemoteClients() throws IOException {
        try {
            IOUtils.close(remoteClient);
        } finally {
            remoteClient = null;
        }
    }

    @Override
    protected String clusterSpecificIndexName(String pattern) {
        StringJoiner sj = new StringJoiner(",");
        for (String index : pattern.split(",")) {
            sj.add(remoteClusterIndex(index));
        }
        return sj.toString();
    }

    private static String remoteClusterIndex(String indexName) {
        return REMOTE_CLUSTER_NAME + ":" + indexName;
    }

    @Override
    protected RestClient provisioningClient() throws IOException {
        return remoteClusterClient();
    }

    @Override
    protected RestClient provisioningAdminClient() throws IOException {
        return remoteClusterClient();
    }

    private RestClient remoteClusterClient() throws IOException {
        if (remoteClient == null) {
            var clusterHosts = parseClusterHosts(remoteCluster.getHttpAddresses());
            remoteClient = buildClient(restClientSettings(), clusterHosts.toArray(new HttpHost[0]));
        }
        return remoteClient;
    }

    protected boolean isSkipUnavailable() {
        return true;
    }

    @Override
    public void testAlias() throws IOException {
        assumeFalse("expecting skip_unavailable to be false", isSkipUnavailable());
        super.testAlias();
    }

    @Override
    public void testExistentIndexWithoutWildcard() throws IOException {
        assumeFalse("expecting skip_unavailable to be false", isSkipUnavailable());
        super.testExistentIndexWithoutWildcard();
    }

    @Before
    public void skipTestOnOldVersions() {
        assumeTrue("skip on old versions", Clusters.localClusterVersion().equals(Version.V_8_19_0));
    }
}
