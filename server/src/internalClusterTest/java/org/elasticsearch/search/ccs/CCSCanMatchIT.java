/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.ccs;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.PointValues;
import org.elasticsearch.action.search.CanMatchNodeRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.engine.InternalEngineFactory;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.index.shard.ShardLongFieldRange;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.in;

public class CCSCanMatchIT extends AbstractMultiClustersTestCase {
    static final String REMOTE_CLUSTER = "cluster_a";

    @Override
    protected List<String> remoteClusterAlias() {
        return List.of("cluster_a");
    }

    private static class EngineWithExposingTimestamp extends InternalEngine {
        EngineWithExposingTimestamp(EngineConfig engineConfig) {
            super(engineConfig);
            assert IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.get(config().getIndexSettings().getSettings()) : "require read-only index";
        }

        @Override
        public ShardLongFieldRange getRawFieldRange(String field) {
            try (Searcher searcher = acquireSearcher("test")) {
                final DirectoryReader directoryReader = searcher.getDirectoryReader();

                final byte[] minPackedValue = PointValues.getMinPackedValue(directoryReader, field);
                final byte[] maxPackedValue = PointValues.getMaxPackedValue(directoryReader, field);
                if (minPackedValue == null || maxPackedValue == null) {
                    assert minPackedValue == null && maxPackedValue == null
                        : Arrays.toString(minPackedValue) + "-" + Arrays.toString(maxPackedValue);
                    return ShardLongFieldRange.EMPTY;
                }

                return ShardLongFieldRange.of(LongPoint.decodeDimension(minPackedValue, 0), LongPoint.decodeDimension(maxPackedValue, 0));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    public static class ExposingTimestampEnginePlugin extends Plugin implements EnginePlugin {

        @Override
        public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
            if (IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.get(indexSettings.getSettings())) {
                return Optional.of(EngineWithExposingTimestamp::new);
            } else {
                return Optional.of(new InternalEngineFactory());
            }
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        return CollectionUtils.appendToCopy(super.nodePlugins(clusterAlias), ExposingTimestampEnginePlugin.class);
    }

    int createIndexAndIndexDocs(String cluster, String index, int numberOfShards, long timestamp, boolean exposeTimestamp)
        throws Exception {
        Client client = client(cluster);
        assertAcked(
            client.admin()
                .indices()
                .prepareCreate(index)
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                )
                .setMapping("@timestamp", "type=date", "position", "type=long")
        );
        int numDocs = between(100, 500);
        for (int i = 0; i < numDocs; i++) {
            client.prepareIndex(index).setSource("position", i, "@timestamp", timestamp + i).get();
        }
        if (exposeTimestamp) {
            client.admin().indices().prepareClose(index).get();
            client.admin()
                .indices()
                .prepareUpdateSettings(index)
                .setSettings(Settings.builder().put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), true).build())
                .get();
            client.admin().indices().prepareOpen(index).get();
            assertBusy(() -> {
                IndexLongFieldRange timestampRange = cluster(cluster).clusterService().state().metadata().index(index).getTimestampRange();
                assertTrue(Strings.toString(timestampRange), timestampRange.containsAllShardRanges());
            });
        } else {
            client.admin().indices().prepareRefresh(index).get();
        }
        return numDocs;
    }

    public void testCanMatchOnTimeRange() throws Exception {
        long timestamp = randomLongBetween(10_000_000, 50_000_000);
        int oldLocalNumShards = randomIntBetween(1, 5);
        createIndexAndIndexDocs(LOCAL_CLUSTER, "local_old_index", oldLocalNumShards, timestamp - 10_000, true);
        int oldRemoteNumShards = randomIntBetween(1, 5);
        createIndexAndIndexDocs(REMOTE_CLUSTER, "remote_old_index", oldRemoteNumShards, timestamp - 10_000, true);

        int newLocalNumShards = randomIntBetween(1, 5);
        int localDocs = createIndexAndIndexDocs(LOCAL_CLUSTER, "local_new_index", newLocalNumShards, timestamp, randomBoolean());
        int newRemoteNumShards = randomIntBetween(1, 5);
        int remoteDocs = createIndexAndIndexDocs(REMOTE_CLUSTER, "remote_new_index", newRemoteNumShards, timestamp, randomBoolean());

        for (String cluster : List.of(LOCAL_CLUSTER, REMOTE_CLUSTER)) {
            for (TransportService ts : cluster(cluster).getInstances(TransportService.class)) {
                MockTransportService mockTransportService = (MockTransportService) ts;
                mockTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                    if (action.equals(SearchTransportService.QUERY_CAN_MATCH_NODE_NAME)) {
                        CanMatchNodeRequest canMatchNodeRequest = (CanMatchNodeRequest) request;
                        List<String> indices = canMatchNodeRequest.getShardLevelRequests()
                            .stream()
                            .map(r -> r.shardId().getIndexName())
                            .toList();
                        assertThat("old indices should be prefiltered on coordinator node", "local_old_index", Matchers.not(in(indices)));
                        assertThat("old indices should be prefiltered on coordinator node", "remote_old_index", Matchers.not(in(indices)));
                        if (cluster.equals(LOCAL_CLUSTER)) {
                            DiscoveryNode targetNode = connection.getNode();
                            DiscoveryNodes remoteNodes = cluster(REMOTE_CLUSTER).clusterService().state().nodes();
                            assertNull("No can_match requests sent across clusters", remoteNodes.get(targetNode.getId()));
                        }
                    }
                    connection.sendRequest(requestId, action, request, options);
                });
            }
        }
        try {
            for (boolean minimizeRoundTrips : List.of(true, false)) {
                SearchSourceBuilder source = new SearchSourceBuilder().query(new RangeQueryBuilder("@timestamp").from(timestamp));
                SearchRequest request = new SearchRequest("local_*", "*:remote_*");
                request.source(source).setCcsMinimizeRoundtrips(minimizeRoundTrips);
                assertResponse(client().search(request), response -> {
                    assertHitCount(response, localDocs + remoteDocs);
                    int totalShards = oldLocalNumShards + newLocalNumShards + oldRemoteNumShards + newRemoteNumShards;
                    assertThat(response.getTotalShards(), equalTo(totalShards));
                    assertThat(response.getSkippedShards(), equalTo(oldLocalNumShards + oldRemoteNumShards));
                });
            }
        } finally {
            for (String cluster : List.of(LOCAL_CLUSTER, REMOTE_CLUSTER)) {
                for (TransportService ts : cluster(cluster).getInstances(TransportService.class)) {
                    MockTransportService mockTransportService = (MockTransportService) ts;
                    mockTransportService.clearAllRules();
                }
            }
        }
    }
}
