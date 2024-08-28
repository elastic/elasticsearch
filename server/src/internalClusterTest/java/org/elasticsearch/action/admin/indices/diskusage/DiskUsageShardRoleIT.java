/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.diskusage;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingRoleStrategy;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineCreationFailureException;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.engine.ReadOnlyEngine;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class DiskUsageShardRoleIT extends ESIntegTestCase {

    @Override
    protected boolean addMockInternalEngine() {
        return false; // use custom engine already
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(ShardRoleTestPlugin.class);
        plugins.add(MockTransportService.TestPlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings)).put(STATELESS_ENABLED.getKey(), true).build();
    }

    public static final Setting<Boolean> STATELESS_ENABLED = Setting.boolSetting(
        DiscoveryNode.STATELESS_ENABLED_SETTING_NAME,
        false,
        Setting.Property.NodeScope
    );

    public static class ShardRoleTestPlugin extends Plugin implements EnginePlugin, ClusterPlugin {
        @Override
        public List<Setting<?>> getSettings() {
            return List.of(STATELESS_ENABLED);
        }

        static Engine createSearchOnlyEngine(EngineConfig config) {
            try {
                config.getStore().createEmpty();
            } catch (IOException e) {
                throw new EngineCreationFailureException(config.getShardId(), "failed to create empty store", e);
            }
            var seqNoStats = new SeqNoStats(
                SequenceNumbers.NO_OPS_PERFORMED,
                SequenceNumbers.NO_OPS_PERFORMED,
                SequenceNumbers.NO_OPS_PERFORMED
            );
            return new ReadOnlyEngine(config, seqNoStats, new TranslogStats(), false, Function.identity(), false, false) {
                @Override
                public IndexCommitRef acquireLastIndexCommit(boolean flushFirst) {
                    return null;
                }
            };
        }

        @Override
        public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
            return Optional.of(config -> {
                if (config.isPromotableToPrimary()) {
                    return new InternalEngine(config);
                } else {
                    return createSearchOnlyEngine(config);
                }
            });
        }

        @Override
        public ShardRoutingRoleStrategy getShardRoutingRoleStrategy() {
            return new ShardRoutingRoleStrategy() {
                @Override
                public ShardRouting.Role newReplicaRole() {
                    return ShardRouting.Role.SEARCH_ONLY;
                }

                @Override
                public ShardRouting.Role newEmptyRole(int copyIndex) {
                    return ShardRouting.Role.INDEX_ONLY;
                }
            };
        }
    }

    public void testShardRoutingRole() throws Exception {
        int totalShards = 0;
        int numIndices = between(1, 5);
        internalCluster().ensureAtLeastNumDataNodes(3);
        AtomicInteger analyzedShards = new AtomicInteger();
        for (TransportService ts : internalCluster().getInstances(TransportService.class)) {
            var transportService = (MockTransportService) ts;
            transportService.addRequestHandlingBehavior("indices:admin/analyze_disk_usage[s]", (handler, request, channel, task) -> {
                analyzedShards.incrementAndGet();
                handler.messageReceived(request, new TransportChannel() {
                    @Override
                    public String getProfileName() {
                        return channel.getProfileName();
                    }

                    @Override
                    public void sendResponse(TransportResponse response) {
                        channel.sendResponse(response);
                    }

                    @Override
                    public void sendResponse(Exception exception) {
                        throw new AssertionError("Analyzing shard should not fail", exception);
                    }
                }, task);
            });
        }
        try {
            for (int i = 0; i < numIndices; i++) {
                String indexName = "test-index-" + i;
                int numberOfShards = between(1, 3);
                totalShards += numberOfShards;
                createIndex(indexName, numberOfShards, 0);
                ensureGreen(indexName);
                int numDocs = randomIntBetween(1, 5);
                for (int d = 0; d < numDocs; d++) {
                    int value = randomIntBetween(1, 5);
                    final XContentBuilder doc = XContentFactory.jsonBuilder().startObject().field("value", value).endObject();
                    prepareIndex(indexName).setId("id-" + d).setSource(doc).get();
                }
                assertAcked(
                    client().admin()
                        .indices()
                        .prepareUpdateSettings(indexName)
                        .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, between(0, 2)))
                );
                ensureGreen(indexName);
            }
            for (boolean flush : new boolean[] { false, true }) {
                analyzedShards.set(0);
                String[] indices = { "test-index-*" };
                var resp = client().execute(
                    TransportAnalyzeIndexDiskUsageAction.TYPE,
                    new AnalyzeIndexDiskUsageRequest(indices, AnalyzeIndexDiskUsageRequest.DEFAULT_INDICES_OPTIONS, flush)
                ).actionGet();
                assertThat(resp.getTotalShards(), equalTo(totalShards));
                assertThat(resp.getSuccessfulShards(), equalTo(totalShards));
                assertThat(resp.getFailedShards(), equalTo(0));
                assertThat(analyzedShards.get(), equalTo(totalShards));
            }
        } finally {
            for (TransportService ts : internalCluster().getInstances(TransportService.class)) {
                ((MockTransportService) ts).clearAllRules();
            }
        }
    }
}
