/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector.indices;

import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequestBuilder;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.monitoring.collector.BaseCollectorTestCase;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.elasticsearch.xpack.monitoring.MonitoringTestUtils.randomMonitoringNode;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IndexStatsCollectorTests extends BaseCollectorTestCase {

    public void testShouldCollectReturnsFalseIfMonitoringNotAllowed() {
        // this controls the blockage
        when(licenseState.isMonitoringAllowed()).thenReturn(false);
        final boolean isElectedMaster = randomBoolean();
        whenLocalNodeElectedMaster(isElectedMaster);

        final IndexStatsCollector collector = new IndexStatsCollector(Settings.EMPTY, clusterService, licenseState, client);

        assertThat(collector.shouldCollect(isElectedMaster), is(false));
        if (isElectedMaster) {
            verify(licenseState).isMonitoringAllowed();
        }
    }

    public void testShouldCollectReturnsFalseIfNotMaster() {
        when(licenseState.isMonitoringAllowed()).thenReturn(true);
        final IndexStatsCollector collector = new IndexStatsCollector(Settings.EMPTY, clusterService, licenseState, client);

        assertThat(collector.shouldCollect(false), is(false));
    }

    public void testShouldCollectReturnsTrue() {
        when(licenseState.isMonitoringAllowed()).thenReturn(true);
        final IndexStatsCollector collector = new IndexStatsCollector(Settings.EMPTY, clusterService, licenseState, client);

        assertThat(collector.shouldCollect(true), is(true));
        verify(licenseState).isMonitoringAllowed();
    }

    public void testDoCollect() throws Exception {
        final TimeValue timeout = TimeValue.timeValueSeconds(randomIntBetween(1, 120));
        withCollectionTimeout(IndexStatsCollector.INDEX_STATS_TIMEOUT, timeout);

        whenLocalNodeElectedMaster(true);

        final String clusterName = randomAlphaOfLength(10);
        whenClusterStateWithName(clusterName);

        final String clusterUUID = UUID.randomUUID().toString();
        whenClusterStateWithUUID(clusterUUID);

        final RoutingTable routingTable = mock(RoutingTable.class);
        when(clusterState.routingTable()).thenReturn(routingTable);

        final MonitoringDoc.Node node = randomMonitoringNode(random());

        final int indices = randomIntBetween(0, 10);
        final Map<String, IndexStats> indicesStats = new HashMap<>(indices);
        final Map<String, IndexMetaData> indicesMetaData = new HashMap<>(indices);
        final Map<String, IndexRoutingTable> indicesRoutingTable = new HashMap<>(indices);

        for (int i = 0; i < indices; i++) {
            final String index = "_index_" + i;
            final IndexStats indexStats = mock(IndexStats.class);
            final IndexMetaData indexMetaData = mock(IndexMetaData.class);
            final IndexRoutingTable indexRoutingTable = mock(IndexRoutingTable.class);

            indicesStats.put(index, indexStats);
            indicesMetaData.put(index, indexMetaData);
            indicesRoutingTable.put(index, indexRoutingTable);

            when(indexStats.getIndex()).thenReturn(index);
            when(metaData.index(index)).thenReturn(indexMetaData);
            when(routingTable.index(index)).thenReturn(indexRoutingTable);
        }

        final IndicesStatsResponse indicesStatsResponse = mock(IndicesStatsResponse.class);
        when(indicesStatsResponse.getIndices()).thenReturn(indicesStats);

        final IndicesStatsRequestBuilder indicesStatsRequestBuilder =
                spy(new IndicesStatsRequestBuilder(mock(ElasticsearchClient.class), IndicesStatsAction.INSTANCE));
        doReturn(indicesStatsResponse).when(indicesStatsRequestBuilder).get(eq(timeout));

        final IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);
        when(indicesAdminClient.prepareStats()).thenReturn(indicesStatsRequestBuilder);

        final AdminClient adminClient = mock(AdminClient.class);
        when(adminClient.indices()).thenReturn(indicesAdminClient);

        final Client client = mock(Client.class);
        when(client.admin()).thenReturn(adminClient);

        final IndexStatsCollector collector = new IndexStatsCollector(Settings.EMPTY, clusterService, licenseState, client);
        assertEquals(timeout, collector.getCollectionTimeout());

        final long interval = randomNonNegativeLong();

        final Collection<MonitoringDoc> results = collector.doCollect(node, interval, clusterState);
        verify(indicesAdminClient).prepareStats();
        verify(clusterState, times(1 + indices)).metaData();
        verify(clusterState, times(indices)).routingTable();
        verify(metaData).clusterUUID();

        assertEquals(1 + indices, results.size());

        for (final MonitoringDoc document : results) {
            assertThat(document.getCluster(), equalTo(clusterUUID));
            assertThat(document.getTimestamp(), greaterThan(0L));
            assertThat(document.getIntervalMillis(), equalTo(interval));
            assertThat(document.getNode(), equalTo(node));
            assertThat(document.getSystem(), is(MonitoredSystem.ES));
            assertThat(document.getId(), nullValue());

            if (document instanceof IndicesStatsMonitoringDoc) {
                assertThat(document.getType(), equalTo(IndicesStatsMonitoringDoc.TYPE));
                assertThat(((IndicesStatsMonitoringDoc) document).getIndicesStats(), is(indicesStatsResponse));
            } else {
                assertThat(document.getType(), equalTo(IndexStatsMonitoringDoc.TYPE));

                final IndexStatsMonitoringDoc indexStatsDocument = (IndexStatsMonitoringDoc)document;
                final String index = indexStatsDocument.getIndexStats().getIndex();

                assertThat(indexStatsDocument.getIndexStats(), is(indicesStats.get(index)));
                assertThat(indexStatsDocument.getIndexMetaData(), is(indicesMetaData.get(index)));
                assertThat(indexStatsDocument.getIndexRoutingTable(), is(indicesRoutingTable.get(index)));
            }
        }
    }
}
