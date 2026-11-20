/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.FlushMetrics;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.telemetry.metric.MetricAttributes;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Checks that a node passes its real meter registry down to every shard, so flush failures reach APM. Which failures are counted is
 * covered by {@code IndexShardTests}.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class FlushMetricsIT extends ESIntegTestCase {

    private static final AtomicReference<RuntimeException> flushFailure = new AtomicReference<>();

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestTelemetryPlugin.class, TestEnginePlugin.class);
    }

    public void testFlushFailureIsReportedToNodeMeterRegistry() {
        final String dataNode = internalCluster().startNode();
        final TestTelemetryPlugin plugin = internalCluster().getInstance(PluginsService.class, dataNode)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();

        assertAcked(prepareCreate("test"));
        client(dataNode).index(new IndexRequest("test").source(Map.of())).actionGet();
        final IndexShard shard = internalCluster().getInstance(IndicesService.class, dataNode)
            .indexServiceSafe(resolveIndex("test"))
            .getShard(0);

        flushFailure.set(new IllegalStateException("unexpected"));
        try {
            shard.flush(
                new FlushRequest().force(true).waitIfOngoing(true),
                ActionListener.wrap(result -> fail("flush must fail"), e -> {})
            );
        } finally {
            flushFailure.set(null);
        }

        final List<Measurement> measurements = plugin.getLongCounterMeasurement(FlushMetrics.FLUSH_UNEXPECTED_FAILURE_TOTAL);
        assertThat(measurements, hasSize(1));
        assertThat(measurements.getFirst().getLong(), equalTo(1L));
        assertThat(
            measurements.getFirst().attributes(),
            equalTo(
                Map.of(
                    MetricAttributes.ES_INDEX_MODE,
                    IndexMode.STANDARD.getName(),
                    MetricAttributes.ERROR_TYPE,
                    IllegalStateException.class.getSimpleName()
                )
            )
        );
    }

    /**
     * Installs an engine whose flushes fail on demand from inside the engine's flush, where failures are reported, without breaking
     * real storage.
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
        protected void ensureCanFlush() {
            super.ensureCanFlush();
            final RuntimeException failure = flushFailure.get();
            if (failure != null) {
                throw failure;
            }
        }
    }
}
