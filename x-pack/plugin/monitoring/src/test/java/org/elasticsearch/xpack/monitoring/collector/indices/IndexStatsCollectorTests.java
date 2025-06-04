/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.collector.indices;

import org.elasticsearch.exception.ElasticsearchTimeoutException;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequestBuilder;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.client.internal.AdminClient;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.client.internal.IndicesAdminClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.monitoring.BaseCollectorTestCase;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.elasticsearch.xpack.monitoring.MonitoringTestUtils.randomMonitoringNode;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IndexStatsCollectorTests extends BaseCollectorTestCase {

    public void testShouldCollectReturnsFalseIfNotMaster() {
        final IndexStatsCollector collector = new IndexStatsCollector(clusterService, licenseState, client);

        assertThat(collector.shouldCollect(false), is(false));
    }

    public void testShouldCollectReturnsTrue() {
        final IndexStatsCollector collector = new IndexStatsCollector(clusterService, licenseState, client);

        assertThat(collector.shouldCollect(true), is(true));
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

        final IndicesStatsResponse indicesStatsResponse = mock(IndicesStatsResponse.class);
        final MonitoringDoc.Node node = randomMonitoringNode(random());

        // Number of indices that exist in the cluster state and returned in the IndicesStatsResponse
        final int existingIndices = randomIntBetween(0, 10);
        // Number of indices returned in the IndicesStatsResponse only
        final int createdIndices = randomIntBetween(0, 10);
        // Number of indices returned in the local cluster state only
        final int deletedIndices = randomIntBetween(0, 10);
        // Total number of indices
        final int indices = existingIndices + createdIndices + deletedIndices;

        final Map<String, IndexStats> indicesStats = Maps.newMapWithExpectedSize(indices);
        final Map<String, IndexMetadata> indicesMetadata = Maps.newMapWithExpectedSize(indices);
        final Map<String, IndexRoutingTable> indicesRoutingTable = Maps.newMapWithExpectedSize(indices);

        for (int i = 0; i < indices; i++) {
            final String index = "_index_" + i;
            final IndexStats indexStats = mock(IndexStats.class);
            when(indexStats.getIndex()).thenReturn(index);

            final IndexMetadata indexMetadata = mock(IndexMetadata.class);
            final IndexRoutingTable indexRoutingTable = mock(IndexRoutingTable.class);

            if (i < (createdIndices + existingIndices)) {
                when(indicesStatsResponse.getIndex(index)).thenReturn(indexStats);
            }
            if (i >= createdIndices) {
                indicesMetadata.put(index, indexMetadata);
                when(projectMetadata.index(index)).thenReturn(indexMetadata);

                indicesRoutingTable.put(index, indexRoutingTable);
                when(routingTable.index(index)).thenReturn(indexRoutingTable);

                if (i < (createdIndices + existingIndices)) {
                    indicesStats.put(index, indexStats);
                }
            }
        }
        when(indicesStatsResponse.getShardFailures()).thenReturn(new DefaultShardOperationFailedException[0]);

        final String[] indexNames = indicesMetadata.keySet().toArray(new String[0]);
        when(projectMetadata.getConcreteAllIndices()).thenReturn(indexNames);

        final IndicesStatsRequestBuilder indicesStatsRequestBuilder = spy(new IndicesStatsRequestBuilder(mock(ElasticsearchClient.class)));
        doReturn(indicesStatsResponse).when(indicesStatsRequestBuilder).get();

        final IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);
        when(indicesAdminClient.prepareStats()).thenReturn(indicesStatsRequestBuilder);

        final AdminClient adminClient = mock(AdminClient.class);
        when(adminClient.indices()).thenReturn(indicesAdminClient);

        final Client client = mock(Client.class);
        when(client.admin()).thenReturn(adminClient);

        final IndexStatsCollector collector = new IndexStatsCollector(clusterService, licenseState, client);
        assertEquals(timeout, collector.getCollectionTimeout());

        final long interval = randomNonNegativeLong();

        final Collection<MonitoringDoc> results = collector.doCollect(node, interval, clusterState);
        verify(indicesAdminClient).prepareStats();
        verify(indicesStatsRequestBuilder).setTimeout(timeout);

        verify(indicesStatsResponse, times(existingIndices + deletedIndices)).getIndex(anyString());
        verify(projectMetadata, times(existingIndices)).index(anyString());
        verify(routingTable, times(existingIndices)).index(anyString());
        verify(metadata).clusterUUID();

        assertEquals(1 + existingIndices, results.size());

        for (final MonitoringDoc document : results) {
            assertThat(document.getCluster(), equalTo(clusterUUID));
            assertThat(document.getTimestamp(), greaterThan(0L));
            assertThat(document.getIntervalMillis(), equalTo(interval));
            assertThat(document.getNode(), equalTo(node));
            assertThat(document.getSystem(), is(MonitoredSystem.ES));
            assertThat(document.getId(), nullValue());

            if (document instanceof IndicesStatsMonitoringDoc) {
                assertThat(document.getType(), equalTo(IndicesStatsMonitoringDoc.TYPE));
                final List<IndexStats> actualIndicesStats = ((IndicesStatsMonitoringDoc) document).getIndicesStats();
                actualIndicesStats.forEach((value) -> assertThat(value, is(indicesStats.get(value.getIndex()))));
                assertThat(actualIndicesStats.size(), equalTo(indicesStats.size()));
            } else {
                assertThat(document.getType(), equalTo(IndexStatsMonitoringDoc.TYPE));

                final IndexStatsMonitoringDoc indexStatsDocument = (IndexStatsMonitoringDoc) document;
                final String index = indexStatsDocument.getIndexStats().getIndex();

                assertThat(indexStatsDocument.getIndexStats(), is(indicesStats.get(index)));
                assertThat(indexStatsDocument.getIndexMetadata(), is(indicesMetadata.get(index)));
                assertThat(indexStatsDocument.getIndexRoutingTable(), is(indicesRoutingTable.get(index)));
            }
        }

        assertWarnings(
            "[xpack.monitoring.collection.index.stats.timeout] setting was deprecated in Elasticsearch and will be removed "
                + "in a future release. See the deprecation documentation for the next major version."
        );
    }

    public void testDoCollectThrowsTimeoutException() throws Exception {
        final TimeValue timeout = TimeValue.timeValueSeconds(randomIntBetween(1, 120));
        withCollectionTimeout(IndexStatsCollector.INDEX_STATS_TIMEOUT, timeout);

        whenLocalNodeElectedMaster(true);

        final IndicesStatsResponse indicesStatsResponse = mock(IndicesStatsResponse.class);
        final MonitoringDoc.Node node = randomMonitoringNode(random());

        when(indicesStatsResponse.getShardFailures()).thenReturn(
            new DefaultShardOperationFailedException[] {
                new DefaultShardOperationFailedException(
                    "test",
                    0,
                    new FailedNodeException(node.getUUID(), "msg", new ElasticsearchTimeoutException("test timeout"))
                ) }
        );

        final IndicesStatsRequestBuilder indicesStatsRequestBuilder = spy(new IndicesStatsRequestBuilder(mock(ElasticsearchClient.class)));
        doReturn(indicesStatsResponse).when(indicesStatsRequestBuilder).get();

        final IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);
        when(indicesAdminClient.prepareStats()).thenReturn(indicesStatsRequestBuilder);

        final AdminClient adminClient = mock(AdminClient.class);
        when(adminClient.indices()).thenReturn(indicesAdminClient);

        final Client client = mock(Client.class);
        when(client.admin()).thenReturn(adminClient);

        final IndexStatsCollector collector = new IndexStatsCollector(clusterService, licenseState, client);
        final long interval = randomNonNegativeLong();

        expectThrows(ElasticsearchTimeoutException.class, () -> collector.doCollect(node, interval, clusterState));

        assertWarnings(
            "[xpack.monitoring.collection.index.stats.timeout] setting was deprecated in Elasticsearch and will be removed "
                + "in a future release. See the deprecation documentation for the next major version."
        );
    }

}
