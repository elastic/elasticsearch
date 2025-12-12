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
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.qa.rest.AllSupportedFieldsTestCase;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Fetch all field types via cross cluster search, possible on a different version.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class AllSupportedFieldsIT extends AllSupportedFieldsTestCase {
    static ElasticsearchCluster remoteCluster = Clusters.remoteCluster();
    static ElasticsearchCluster localCluster = Clusters.localCluster(remoteCluster);

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(remoteCluster).around(localCluster);

    private static RestClient remoteClient;
    private static Map<String, NodeInfo> remoteNodeToInfo;

    public AllSupportedFieldsIT(MappedFieldType.FieldExtractPreference extractPreference, IndexMode indexMode) {
        super(extractPreference, indexMode);
    }

    @Before
    public void createRemoteIndices() throws IOException {
        if (supportsNodeAssignment()) {
            for (Map.Entry<String, NodeInfo> e : remoteNodeToInfo().entrySet()) {
                createIndexForNode(remoteClient(), minVersion(), e.getKey(), e.getValue().id(), indexMode());
            }
        } else {
            createIndexForNode(remoteClient(), minVersion(), null, null, indexMode());
        }

        // We need a single lookup index that has the same name across all clusters, as well as a single enrich policy per cluster.
        // We create both only when we're testing LOOKUP mode.
        if (indexExists(remoteClient(), LOOKUP_INDEX_NAME) == false && indexMode() == IndexMode.LOOKUP) {
            createAllTypesIndex(remoteClient(), minVersion(), LOOKUP_INDEX_NAME, null, indexMode());
            createAllTypesDoc(remoteClient(), minVersion(), LOOKUP_INDEX_NAME);
            createEnrichPolicy(remoteClient(), minVersion(), LOOKUP_INDEX_NAME, ENRICH_POLICY_NAME);
        }
    }

    private Map<String, NodeInfo> remoteNodeToInfo() throws IOException {
        if (remoteNodeToInfo == null) {
            remoteNodeToInfo = fetchNodeToInfo(remoteClient(), "remote_cluster");
        }
        return remoteNodeToInfo;
    }

    @Override
    protected Map<String, NodeInfo> allNodeToInfo() throws IOException {
        Map<String, NodeInfo> all = new TreeMap<>();
        all.putAll(super.allNodeToInfo());
        all.putAll(remoteNodeToInfo());
        return all;
    }

    private RestClient remoteClient() throws IOException {
        if (remoteClient == null) {
            var clusterHosts = parseClusterHosts(remoteCluster.getHttpAddresses());
            remoteClient = buildClient(restClientSettings(), clusterHosts.toArray(new HttpHost[0]));
        }
        return remoteClient;
    }

    @Override
    protected String getTestRestCluster() {
        return localCluster.getHttpAddresses();
    }

    @AfterClass
    public static void closeRemoteClient() throws IOException {
        try {
            IOUtils.close(remoteClient);
        } finally {
            remoteClient = null;
        }
    }

    @Override
    protected String allIndexPattern() {
        return "*:%mode%*,%mode%*";
    }

    @Override
    protected boolean fetchDenseVectorAggMetricDoubleIfFns() throws IOException {
        return super.fetchDenseVectorAggMetricDoubleIfFns()
            && clusterHasCapability(remoteClient(), "GET", "/_query", List.of(), List.of("DENSE_VECTOR_AGG_METRIC_DOUBLE_IF_FNS")).orElse(
                false
            );
    }

    @Override
    protected boolean fetchAllIsCrossCluster() {
        return true;
    }

    @Override
    protected boolean lookupJoinOnAllIndicesSupported() throws IOException {
        return clusterHasCapability(client(), "GET", "/_query", List.of(), List.of("ENABLE_LOOKUP_JOIN_ON_REMOTE")).orElse(false)
            && clusterHasCapability(remoteClient(), "GET", "/_query", List.of(), List.of("ENABLE_LOOKUP_JOIN_ON_REMOTE")).orElse(false);
    }

    public final void testFetchAllOnlyFromRemotes() throws IOException {
        doTestFetchAll(fromAllQuery("*:%mode%*", """
            , _id, _ignored, _index_mode, _score, _source, _version
            | LIMIT 1000
            """), remoteNodeToInfo(), allNodeToInfo());
    }
}
