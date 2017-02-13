/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.resolver.cluster;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.xpack.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.monitoring.MonitoringSettings;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringTemplateUtils;
import org.elasticsearch.xpack.monitoring.resolver.MonitoringIndexNameResolver;
import org.elasticsearch.xpack.monitoring.test.MonitoringIntegTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.core.Is.is;

//test is just too slow, please fix it to not be sleep-based
@LuceneTestCase.BadApple(bugUrl = "https://github.com/elastic/x-plugins/issues/1007")
@ClusterScope(scope = Scope.TEST)
public class ClusterStateTests extends MonitoringIntegTestCase {

    private int randomInt = randomInt();

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(MonitoringSettings.INTERVAL.getKey(), "-1")
                .put("xpack.monitoring.exporters.default_local.type", "local")
                .put("node.attr.custom", randomInt)
                .build();
    }

    @Before
    public void init() throws Exception {
        updateMonitoringInterval(3L, TimeUnit.SECONDS);
        waitForMonitoringIndices();
    }

    @After
    public void cleanup() throws Exception {
        disableMonitoringInterval();
        wipeMonitoringIndices();
    }

    public void testClusterState() throws Exception {
        logger.debug("--> waiting for documents to be collected");
        awaitMonitoringDocsCount(greaterThan(0L), ClusterStateResolver.TYPE);

        logger.debug("--> searching for monitoring documents of type [{}]", ClusterStateResolver.TYPE);
        SearchResponse response = client().prepareSearch().setTypes(ClusterStateResolver.TYPE).get();
        assertThat(response.getHits().getTotalHits(), greaterThan(0L));

        logger.debug("--> checking that every document contains the expected fields");
        Set<String> filters = ClusterStateResolver.FILTERS;
        for (SearchHit searchHit : response.getHits().getHits()) {
            Map<String, Object> fields = searchHit.getSourceAsMap();

            for (String filter : filters) {
                assertContains(filter, fields);
            }
        }

        logger.debug("--> cluster state successfully collected");
    }

    /**
     * This test should fail if the mapping for the 'nodes' attribute
     * in the 'cluster_state' document is NOT set to 'enable: false'
     */
    public void testNoNodesIndexing() throws Exception {
        logger.debug("--> waiting for documents to be collected");
        awaitMonitoringDocsCount(greaterThan(0L), ClusterStateResolver.TYPE);

        logger.debug("--> searching for monitoring documents of type [{}]", ClusterStateResolver.TYPE);
        SearchResponse response = client().prepareSearch().setTypes(ClusterStateResolver.TYPE).get();
        assertThat(response.getHits().getTotalHits(), greaterThan(0L));

        DiscoveryNodes nodes = client().admin().cluster().prepareState().clear().setNodes(true).get().getState().nodes();

        logger.debug("--> ensure that the 'nodes' attributes of the cluster state document is not indexed");
        assertHitCount(client().prepareSearch().setSize(0)
                .setTypes(ClusterStateResolver.TYPE)
                .setQuery(matchQuery("cluster_state.nodes." + nodes.getMasterNodeId() + ".name",
                        nodes.getMasterNode().getName())).get(), 0L);
    }

    public void testClusterStateNodes() throws Exception {
        final long nbNodes = internalCluster().size();

        MonitoringIndexNameResolver.Timestamped timestampedResolver =
                new MockTimestampedIndexNameResolver(MonitoredSystem.ES, Settings.EMPTY, MonitoringTemplateUtils.TEMPLATE_VERSION);
        assertNotNull(timestampedResolver);

        String timestampedIndex = timestampedResolver.indexPattern();
        awaitIndexExists(timestampedIndex);

        logger.debug("--> waiting for documents to be collected");
        awaitMonitoringDocsCount(greaterThanOrEqualTo(nbNodes), ClusterStateNodeResolver.TYPE);

        logger.debug("--> searching for monitoring documents of type [{}]", ClusterStateNodeResolver.TYPE);
        SearchResponse response = client().prepareSearch(timestampedIndex).setTypes(ClusterStateNodeResolver.TYPE).get();
        assertThat(response.getHits().getTotalHits(), greaterThanOrEqualTo(nbNodes));

        logger.debug("--> checking that every document contains the expected fields");
        String[] filters = {
                MonitoringIndexNameResolver.Fields.CLUSTER_UUID,
                MonitoringIndexNameResolver.Fields.TIMESTAMP,
                MonitoringIndexNameResolver.Fields.SOURCE_NODE,
                ClusterStateNodeResolver.Fields.STATE_UUID,
                ClusterStateNodeResolver.Fields.NODE,
                ClusterStateNodeResolver.Fields.NODE + "."
                        + ClusterStateNodeResolver.Fields.ID,
        };

        for (SearchHit searchHit : response.getHits().getHits()) {
            Map<String, Object> fields = searchHit.getSourceAsMap();

            for (String filter : filters) {
                assertContains(filter, fields);
            }
        }

        logger.debug("--> check that node attributes are indexed");
        assertThat(client().prepareSearch().setSize(0)
                .setIndices(timestampedIndex)
                .setTypes(ClusterStateNodeResolver.TYPE)
                .setQuery(QueryBuilders.matchQuery(MonitoringIndexNameResolver.Fields.SOURCE_NODE + ".attributes.custom", randomInt))
                .get().getHits().getTotalHits(), greaterThan(0L));

        logger.debug("--> cluster state nodes successfully collected");
    }

    public void testDiscoveryNodes() throws Exception {
        final long nbNodes = internalCluster().size();

        MonitoringIndexNameResolver.Data dataResolver = new MockDataIndexNameResolver(MonitoringTemplateUtils.TEMPLATE_VERSION);
        assertNotNull(dataResolver);

        String dataIndex = dataResolver.indexPattern();
        awaitIndexExists(dataIndex);

        logger.debug("--> waiting for documents to be collected");
        awaitMonitoringDocsCount(greaterThanOrEqualTo(nbNodes), DiscoveryNodeResolver.TYPE);

        logger.debug("--> searching for monitoring documents of type [{}]", DiscoveryNodeResolver.TYPE);
        SearchResponse response = client().prepareSearch(dataIndex).setTypes(DiscoveryNodeResolver.TYPE).get();
        assertThat(response.getHits().getTotalHits(), greaterThanOrEqualTo(nbNodes));

        logger.debug("--> checking that every document contains the expected fields");
        String[] filters = {
                MonitoringIndexNameResolver.Fields.CLUSTER_UUID,
                MonitoringIndexNameResolver.Fields.TIMESTAMP,
                MonitoringIndexNameResolver.Fields.SOURCE_NODE,
                DiscoveryNodeResolver.Fields.NODE,
                DiscoveryNodeResolver.Fields.NODE + "."
                        + DiscoveryNodeResolver.Fields.ID,
                DiscoveryNodeResolver.Fields.NODE + "."
                        + DiscoveryNodeResolver.Fields.NAME,
                DiscoveryNodeResolver.Fields.NODE + "."
                        + DiscoveryNodeResolver.Fields.ATTRIBUTES,
                DiscoveryNodeResolver.Fields.NODE + "."
                        + DiscoveryNodeResolver.Fields.TRANSPORT_ADDRESS,
        };

        for (SearchHit searchHit : response.getHits().getHits()) {
            Map<String, Object> fields = searchHit.getSourceAsMap();

            for (String filter : filters) {
                assertContains(filter, fields);
            }
        }

        for (final String nodeName : internalCluster().getNodeNames()) {
            final String nodeId = internalCluster().clusterService(nodeName).localNode().getId();

            logger.debug("--> getting monitoring document for node id [{}]", nodeId);
            assertThat(client().prepareGet(dataIndex, DiscoveryNodeResolver.TYPE, nodeId).get().isExists(), is(true));

            // checks that document is not indexed
            assertHitCount(client().prepareSearch(dataIndex).setSize(0)
                    .setTypes(DiscoveryNodeResolver.TYPE)
                    .setQuery(QueryBuilders.boolQuery()
                            .should(matchQuery("node.id", nodeId))
                            .should(matchQuery("node.name", nodeName))).get(), 0);
        }

        logger.debug("--> cluster state nodes successfully collected");
    }
}
