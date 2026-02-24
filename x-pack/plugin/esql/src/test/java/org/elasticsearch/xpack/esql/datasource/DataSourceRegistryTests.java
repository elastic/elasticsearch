/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasource.spi.DataSource;
import org.elasticsearch.xpack.esql.datasource.spi.DataSourceCapabilities;
import org.elasticsearch.xpack.esql.datasource.spi.DataSourceDescriptor;
import org.elasticsearch.xpack.esql.datasource.spi.DataSourceFactory;
import org.elasticsearch.xpack.esql.datasource.spi.DataSourcePartition;
import org.elasticsearch.xpack.esql.datasource.spi.DataSourcePlan;
import org.elasticsearch.xpack.esql.datasource.spi.DataSourcePlugin;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class DataSourceRegistryTests extends ESTestCase {

    public void testSinglePluginRegistersFactory() {
        DataSourceFactory mockFactory = (config, settings) -> null;
        DataSourcePlugin plugin = new DataSourcePlugin() {
            @Override
            public Map<String, DataSourceFactory> dataSources(Settings settings) {
                return Map.of("s3", mockFactory);
            }
        };

        DataSourceRegistry registry = new DataSourceRegistry(List.of(plugin), Settings.EMPTY);

        assertThat(registry.hasType("s3"), is(true));
        assertThat(registry.factory("s3"), is(mockFactory));
    }

    public void testMultiplePluginsCoexist() {
        DataSourceFactory s3Factory = (config, settings) -> null;
        DataSourceFactory pgFactory = (config, settings) -> null;

        DataSourcePlugin plugin1 = new DataSourcePlugin() {
            @Override
            public Map<String, DataSourceFactory> dataSources(Settings settings) {
                return Map.of("s3", s3Factory);
            }
        };
        DataSourcePlugin plugin2 = new DataSourcePlugin() {
            @Override
            public Map<String, DataSourceFactory> dataSources(Settings settings) {
                return Map.of("postgres", pgFactory);
            }
        };

        DataSourceRegistry registry = new DataSourceRegistry(List.of(plugin1, plugin2), Settings.EMPTY);

        assertThat(registry.hasType("s3"), is(true));
        assertThat(registry.hasType("postgres"), is(true));
        assertThat(registry.factory("s3"), is(s3Factory));
        assertThat(registry.factory("postgres"), is(pgFactory));
        assertThat(registry.registeredTypes(), containsInAnyOrder("s3", "postgres"));
    }

    public void testSinglePluginMultipleTypes() {
        DataSourceFactory icebergFactory = (config, settings) -> null;
        DataSourceFactory deltaFactory = (config, settings) -> null;

        DataSourcePlugin plugin = new DataSourcePlugin() {
            @Override
            public Map<String, DataSourceFactory> dataSources(Settings settings) {
                return Map.of("iceberg", icebergFactory, "delta", deltaFactory);
            }
        };

        DataSourceRegistry registry = new DataSourceRegistry(List.of(plugin), Settings.EMPTY);

        assertThat(registry.hasType("iceberg"), is(true));
        assertThat(registry.hasType("delta"), is(true));
        assertThat(registry.factory("iceberg"), is(icebergFactory));
        assertThat(registry.factory("delta"), is(deltaFactory));
    }

    public void testDuplicateTypeThrows() {
        DataSourcePlugin plugin1 = new DataSourcePlugin() {
            @Override
            public Map<String, DataSourceFactory> dataSources(Settings settings) {
                return Map.of("s3", (config, s) -> null);
            }
        };
        DataSourcePlugin plugin2 = new DataSourcePlugin() {
            @Override
            public Map<String, DataSourceFactory> dataSources(Settings settings) {
                return Map.of("s3", (config, s) -> null);
            }
        };

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new DataSourceRegistry(List.of(plugin1, plugin2), Settings.EMPTY)
        );
        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("Duplicate data source type [s3]"));
    }

    public void testEmptyPluginList() {
        DataSourceRegistry registry = new DataSourceRegistry(List.of(), Settings.EMPTY);

        assertThat(registry.hasType("anything"), is(false));
        assertThat(registry.factory("anything"), is(nullValue()));
        assertThat(registry.registeredTypes().isEmpty(), is(true));
    }

    public void testUnknownTypeReturnsNull() {
        DataSourcePlugin plugin = new DataSourcePlugin() {
            @Override
            public Map<String, DataSourceFactory> dataSources(Settings settings) {
                return Map.of("s3", (config, s) -> null);
            }
        };

        DataSourceRegistry registry = new DataSourceRegistry(List.of(plugin), Settings.EMPTY);

        assertThat(registry.hasType("unknown"), is(false));
        assertThat(registry.factory("unknown"), is(nullValue()));
    }

    public void testFactoryReceivesSettings() {
        Settings nodeSettings = Settings.builder().put("custom.key", "custom-value").build();

        DataSourcePlugin plugin = new DataSourcePlugin() {
            @Override
            public Map<String, DataSourceFactory> dataSources(Settings settings) {
                // Verify settings are passed through
                assertEquals("custom-value", settings.get("custom.key"));
                return Map.of("test", (config, s) -> null);
            }
        };

        DataSourceRegistry registry = new DataSourceRegistry(List.of(plugin), nodeSettings);
        assertThat(registry.factory("test"), is(notNullValue()));
    }

    public void testFactoryCreateProducesDataSource() {
        DataSource mockDataSource = new StubDataSource();
        DataSourceFactory factory = (config, settings) -> mockDataSource;

        DataSourcePlugin plugin = new DataSourcePlugin() {
            @Override
            public Map<String, DataSourceFactory> dataSources(Settings settings) {
                return Map.of("stub", factory);
            }
        };

        DataSourceRegistry registry = new DataSourceRegistry(List.of(plugin), Settings.EMPTY);
        DataSource result = registry.factory("stub").create(Map.of(), Map.of());
        assertSame(mockDataSource, result);
    }

    /**
     * Minimal stub DataSource for testing factory create.
     */
    private static class StubDataSource implements DataSource {
        @Override
        public String type() {
            return "stub";
        }

        @Override
        public DataSourceCapabilities capabilities() {
            return DataSourceCapabilities.forCoordinatorOnly();
        }

        @Override
        public void resolve(DataSourceDescriptor source, ResolutionContext context, ActionListener<DataSourcePlan> listener) {
            listener.onFailure(new UnsupportedOperationException("stub"));
        }

        @Override
        public SourceOperator.SourceOperatorFactory createSourceOperator(DataSourcePartition partition, ExecutionContext context) {
            throw new UnsupportedOperationException("stub");
        }
    }
}
