/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.monitor.metrics;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.engine.PendingTranslogRecoveryException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class NodeFlushMetricsIT extends ESIntegTestCase {

    private static final AtomicReference<Exception> flushFailure = new AtomicReference<>();

    private static void failFlushes(IndexShard shard, int times, Exception failure) {
        flushFailure.set(failure);
        for (int i = 0; i < times; i++) {
            shard.flush(
                new FlushRequest().force(true).waitIfOngoing(true),
                ActionListener.wrap(result -> fail("flush must fail"), e -> {})
            );
        }
    }

    private static long recordedValue(TestTelemetryPlugin plugin, String metricName) {
        final List<Measurement> measurements = plugin.getLongAsyncCounterMeasurement(metricName);
        assertThat(metricName, measurements.size(), equalTo(1));
        return measurements.getFirst().getLong();
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestTelemetryPlugin.class, NodeIndexingMetricsIT.TestAPMInternalSettings.class, TestEnginePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put("telemetry.export.interval", TimeValue.timeValueSeconds(0)) // disable metrics cache refresh delay
            .build();
    }

    public void testFailedFlushesAreReportedPerCategory() {
        final String dataNode = internalCluster().startNode();
        ensureStableCluster(1);

        final TestTelemetryPlugin plugin = internalCluster().getInstance(PluginsService.class, dataNode)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();
        plugin.resetMeter();

        assertAcked(prepareCreate("test"));
        client(dataNode).index(new IndexRequest("test").source(Map.of())).actionGet();
        final IndexShard shard = internalCluster().getInstance(IndicesService.class, dataNode)
            .indexServiceSafe(resolveIndex("test"))
            .getShard(0);

        // a different count per category, so that swapped or missing metric wiring fails the assertions
        try {
            failFlushes(shard, 1, new IndexShardClosedException(shard.shardId()));
            failFlushes(shard, 2, new AlreadyClosedException("engine closed"));
            failFlushes(shard, 3, new PendingTranslogRecoveryException("pending translog recovery"));
            failFlushes(shard, 4, new IllegalStateException("unexpected"));
        } finally {
            flushFailure.set(null);
        }

        // simulate async apm `polling` call for metrics
        plugin.collect();

        assertThat(recordedValue(plugin, "es.flush.failed.total"), equalTo(10L));
        assertThat(recordedValue(plugin, "es.flush.failed.shard_closed.total"), equalTo(1L));
        assertThat(recordedValue(plugin, "es.flush.failed.engine_closed.total"), equalTo(2L));
        assertThat(recordedValue(plugin, "es.flush.failed.pending_translog_recovery.total"), equalTo(3L));
    }

    /**
     * Installs an engine whose flushes fail on demand, so that failure categories can be produced without breaking real storage.
     */
    public static class TestEnginePlugin extends Plugin implements EnginePlugin {
        @Override
        public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
            return Optional.of(TestFailingFlushEngine::new);
        }
    }

    private static class TestFailingFlushEngine extends InternalEngine {
        TestFailingFlushEngine(EngineConfig config) {
            super(config);
        }

        @Override
        protected void flushHoldingLock(boolean force, boolean waitIfOngoing, FlushResultListener listener) {
            final Exception failure = flushFailure.get();
            if (failure != null) {
                listener.onFailure(failure);
            } else {
                super.flushHoldingLock(force, waitIfOngoing, listener);
            }
        }
    }
}
