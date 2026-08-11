/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class CustomPluggableMetricsIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), CounterMetricsPlugin.class);
    }

    @Before
    public void checkFeatureFlag() {
        assumeTrue("directory metrics feature flag must be enabled for test", Store.DIRECTORY_METRICS_FEATURE_FLAG.isEnabled());
    }

    public void testCustomMetricsRegisteredAndCaptured() throws IOException {
        String node = internalCluster().getRandomNodeName();
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, node);

        CounterMetricsPlugin plugin = internalCluster().getInstance(PluginsService.class, node)
            .filterPlugins(CounterMetricsPlugin.class)
            .findFirst()
            .orElseThrow();

        Tuple<Void, DirectoryMetrics> tuple = indicesService.withDirectoryMetrics(() -> {
            plugin.holder.instance().increment();
            plugin.holder.instance().increment();
            plugin.holder.instance().increment();
            return null;
        });

        DirectoryMetrics metrics = tuple.v2();
        assertThat(metrics.metrics("counter"), notNullValue());
        DirectoryMetricsTests.Counter counter = metrics.metrics("counter").cast(DirectoryMetricsTests.Counter.class);
        assertThat(counter.getCount(), equalTo(3L));

        assertThat(metrics.metrics("store"), notNullValue());
    }

    public static class CounterMetricsPlugin extends Plugin implements EnginePlugin {
        PluggableDirectoryMetricsHolder<DirectoryMetricsTests.Counter> holder;

        public CounterMetricsPlugin() {
            this.holder = new ThreadLocalDirectoryMetricHolder<>(DirectoryMetricsTests.Counter::new);
            if (randomBoolean()) {
                this.holder = holder.singleThreaded();
            }
        }

        @Override
        public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
            return Optional.empty();
        }

        @Override
        public void registerDirectoryMetrics(BiConsumer<String, PluggableDirectoryMetricsHolder<?>> registrator) {
            registrator.accept("counter", holder);
        }

        @Override
        public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
            return List.of(
                new NamedWriteableRegistry.Entry(
                    DirectoryMetrics.PluggableMetrics.class,
                    DirectoryMetricsTests.Counter.NAME,
                    DirectoryMetricsTests.Counter::new
                )
            );
        }
    }
}
