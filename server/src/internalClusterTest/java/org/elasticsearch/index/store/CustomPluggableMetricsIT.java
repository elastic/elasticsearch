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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

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
        Counter counter = metrics.metrics("counter").cast(Counter.class);
        assertThat(counter.getCount(), equalTo(3L));

        assertThat(metrics.metrics("store"), notNullValue());
    }

    public static class Counter implements DirectoryMetrics.PluggableMetrics<Counter> {
        public static final String NAME = "counter";
        private int count;

        public Counter() {}

        public Counter(int count) {
            this.count = count;
        }

        public Counter(StreamInput in) throws IOException {
            this.count = in.readVInt();
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(count);
        }

        public void increment() {
            count++;
        }

        public long getCount() {
            return count;
        }

        @Override
        public Supplier<Counter> delta() {
            Counter snapshot = copy();
            return () -> new Counter(count - snapshot.count);
        }

        @Override
        public Counter copy() {
            return new Counter(count);
        }

        @Override
        public Counter merge(Counter other) {
            return new Counter(count + other.count);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("count", count);
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Counter that = (Counter) o;
            return count == that.count;
        }

        @Override
        public int hashCode() {
            return Objects.hash(count);
        }
    }

    public static class CounterMetricsPlugin extends Plugin implements EnginePlugin {
        PluggableDirectoryMetricsHolder<Counter> holder;

        public CounterMetricsPlugin() {
            this.holder = new ThreadLocalDirectoryMetricHolder<>(Counter::new);
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
            return List.of(new NamedWriteableRegistry.Entry(DirectoryMetrics.PluggableMetrics.class, Counter.NAME, Counter::new));
        }
    }
}
