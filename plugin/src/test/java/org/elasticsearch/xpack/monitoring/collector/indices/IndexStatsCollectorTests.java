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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IndexStatsCollectorTests extends BaseCollectorTestCase {

    public void testShouldCollectReturnsFalseIfMonitoringNotAllowed() {
        // this controls the blockage
        when(licenseState.isMonitoringAllowed()).thenReturn(false);
        whenLocalNodeElectedMaster(randomBoolean());

        final IndexStatsCollector collector =
                new IndexStatsCollector(Settings.EMPTY, clusterService, monitoringSettings, licenseState, client);

        assertThat(collector.shouldCollect(), is(false));
        verify(licenseState).isMonitoringAllowed();
    }

    public void testShouldCollectReturnsFalseIfNotMaster() {
        when(licenseState.isMonitoringAllowed()).thenReturn(true);
        // this controls the blockage
        whenLocalNodeElectedMaster(false);

        final IndexStatsCollector collector =
                new IndexStatsCollector(Settings.EMPTY, clusterService, monitoringSettings, licenseState, client);

        assertThat(collector.shouldCollect(), is(false));
        verify(licenseState).isMonitoringAllowed();
        verify(nodes).isLocalNodeElectedMaster();
    }

    public void testShouldCollectReturnsTrue() {
        when(licenseState.isMonitoringAllowed()).thenReturn(true);
        whenLocalNodeElectedMaster(true);

        final IndexStatsCollector collector =
                new IndexStatsCollector(Settings.EMPTY, clusterService, monitoringSettings, licenseState, client);

        assertThat(collector.shouldCollect(), is(true));
        verify(licenseState).isMonitoringAllowed();
        verify(nodes).isLocalNodeElectedMaster();
    }

    public void testDoCollect() throws Exception {
        whenLocalNodeElectedMaster(true);

        final String clusterName = randomAlphaOfLength(10);
        whenClusterStateWithName(clusterName);

        final String clusterUUID = UUID.randomUUID().toString();
        whenClusterStateWithUUID(clusterUUID);

        final MonitoringDoc.Node node = randomMonitoringNode(random());

        final TimeValue timeout = mock(TimeValue.class);
        when(monitoringSettings.indexStatsTimeout()).thenReturn(timeout);

        final Map<String, IndexStats> indicesStats = new HashMap<>();
        final int indices = randomIntBetween(0, 10);
        for (int i = 0; i < indices; i++) {
            String index = "_index_" + i;
            IndexStats indexStats = mock(IndexStats.class);
            when(indexStats.getIndex()).thenReturn(index);
            indicesStats.put(index, indexStats);
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

        final IndexStatsCollector collector =
                new IndexStatsCollector(Settings.EMPTY, clusterService, monitoringSettings, licenseState, client);

        final Collection<MonitoringDoc> results = collector.doCollect(node);
        verify(indicesAdminClient).prepareStats();

        assertEquals(1 + indices, results.size());

        for (MonitoringDoc document : results) {
            assertThat(document.getCluster(), equalTo(clusterUUID));
            assertThat(document.getTimestamp(), greaterThan(0L));
            assertThat(document.getNode(), equalTo(node));
            assertThat(document.getSystem(), is(MonitoredSystem.ES));
            assertThat(document.getId(), nullValue());

            if (document instanceof IndicesStatsMonitoringDoc) {
                assertThat(document.getType(), equalTo(IndicesStatsMonitoringDoc.TYPE));
                assertThat(((IndicesStatsMonitoringDoc) document).getIndicesStats(), is(indicesStatsResponse));
            } else {
                assertThat(document.getType(), equalTo(IndexStatsMonitoringDoc.TYPE));

                IndexStats indexStats = ((IndexStatsMonitoringDoc) document).getIndexStats();
                assertThat(indexStats, is(indicesStats.get(indexStats.getIndex())));
            }
        }
    }
}
