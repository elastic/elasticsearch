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
import org.elasticsearch.xpack.esql.qa.rest.UnmappedFieldsLoadAllMaxFieldsTestCase;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

/**
 * Queries an index that only lives on the remote cluster: the remote's data nodes build the {@code _unmapped_fields} column and
 * the local coordinator expands - and caps - it.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class UnmappedFieldsLoadAllMaxFieldsIT extends UnmappedFieldsLoadAllMaxFieldsTestCase {
    static ElasticsearchCluster remoteCluster = Clusters.remoteCluster();
    static ElasticsearchCluster localCluster = Clusters.localCluster(remoteCluster);

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(remoteCluster).around(localCluster);
    private static RestClient remoteClient;

    @Override
    protected String getTestRestCluster() {
        return localCluster.getHttpAddresses();
    }

    /**
     * Both clusters: the local one coordinates and caps, the remote one holds the data. Built lazily because the base class's
     * {@code @Before} needs it, and JUnit runs that before any {@code @Before} declared here.
     */
    @Override
    protected List<RestClient> clientsHoldingData() {
        if (remoteClient == null) {
            try {
                var clusterHosts = parseClusterHosts(remoteCluster.getHttpAddresses());
                remoteClient = buildClient(restClientSettings(), clusterHosts.toArray(new HttpHost[0]));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return List.of(remoteClient);
    }

    @Override
    protected List<RestClient> clientsRequiringCapability() {
        return List.of(client(), clientsHoldingData().get(0));
    }

    @Override
    protected String indexPattern(String index) {
        return Clusters.REMOTE_CLUSTER_NAME + ":" + index;
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
