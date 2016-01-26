/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.marvel.MarvelSettings;
import org.elasticsearch.marvel.agent.exporter.local.LocalExporter;
import org.elasticsearch.marvel.cleaner.CleanerService;
import org.elasticsearch.shield.InternalClient;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 *
 */
public class ExportersTests extends ESTestCase {
    private Exporters exporters;
    private Map<String, Exporter.Factory> factories;
    private ClusterService clusterService;
    private ClusterSettings clusterSettings;

    @Before
    public void init() throws Exception {
        factories = new HashMap<>();

        Client client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);
        clusterService = mock(ClusterService.class);

        // we always need to have the local exporter as it serves as the default one
        factories.put(LocalExporter.TYPE, new LocalExporter.Factory(new InternalClient.Insecure(client), clusterService,
                mock(CleanerService.class)));
        clusterSettings = new ClusterSettings(Settings.EMPTY, new HashSet<>(Arrays.asList(MarvelSettings.COLLECTORS,
                MarvelSettings.INTERVAL, MarvelSettings.EXPORTERS_SETTINGS)));
        exporters = new Exporters(Settings.EMPTY, factories, clusterService, clusterSettings);
    }

    public void testInitExportersDefault() throws Exception {
        Exporter.Factory factory = new TestFactory("_type", true);
        factories.put("_type", factory);
        Exporters.CurrentExporters internalExporters = exporters.initExporters(Settings.builder()
                .build());

        assertThat(internalExporters, notNullValue());
        assertThat(internalExporters.settings.getAsMap().size(), is(0));
        assertThat(internalExporters.exporters.size(), is(1));
        assertThat(internalExporters.exporters, hasKey("default_" + LocalExporter.TYPE));
        assertThat(internalExporters.exporters.get("default_" + LocalExporter.TYPE), instanceOf(LocalExporter.class));
    }

    public void testInitExportersSingle() throws Exception {
        Exporter.Factory factory = new TestFactory("_type", true);
        factories.put("_type", factory);
        Exporters.CurrentExporters internalExporters = exporters.initExporters(Settings.builder()
                .put("_name.type", "_type")
                .build());

        assertThat(internalExporters, notNullValue());
        assertThat(internalExporters.settings.getAsMap().size(), is(1));
        assertThat(internalExporters.settings.getAsMap(), hasEntry("_name.type", "_type"));
        assertThat(internalExporters.exporters.size(), is(1));
        assertThat(internalExporters.exporters, hasKey("_name"));
        assertThat(internalExporters.exporters.get("_name"), instanceOf(TestFactory.TestExporter.class));
        assertThat(internalExporters.exporters.get("_name").type, is("_type"));
    }

    public void testInitExportersSingleDisabled() throws Exception {
        Exporter.Factory factory = new TestFactory("_type", true);
        factories.put("_type", factory);
        Exporters.CurrentExporters internalExporters = exporters.initExporters(Settings.builder()
                .put("_name.type", "_type")
                .put("_name.enabled", false)
                .build());

        assertThat(internalExporters, notNullValue());
        assertThat(internalExporters.settings.getAsMap().size(), is(2));
        assertThat(internalExporters.settings.getAsMap(), hasEntry("_name.type", "_type"));
        assertThat(internalExporters.settings.getAsMap(), hasEntry("_name.enabled", "false"));

        // the only configured exporter is disabled... yet we intentionally don't fallback on the default

        assertThat(internalExporters.exporters.size(), is(0));
    }

    public void testInitExportersSingleUnknownType() throws Exception {
        try {
            exporters.initExporters(Settings.builder()
                    .put("_name.type", "unknown_type")
                    .build());
            fail("Expected SettingsException");
        } catch (SettingsException e) {
            assertThat(e.getMessage(), containsString("unknown exporter type [unknown_type]"));
        }
    }

    public void testInitExportersSingleMissingExporterType() throws Exception {
        try {
            exporters.initExporters(Settings.builder()
                    .put("_name.foo", "bar")
                    .build());
            fail("Expected SettingsException");
        } catch (SettingsException e) {
            assertThat(e.getMessage(), containsString("missing exporter type for [_name]"));
        }
    }

    public void testInitExportersMultipleSameType() throws Exception {
        Exporter.Factory factory = new TestFactory("_type", false);
        factories.put("_type", factory);
        Exporters.CurrentExporters internalExporters = exporters.initExporters(Settings.builder()
                .put("_name0.type", "_type")
                .put("_name1.type", "_type")
                .build());

        assertThat(internalExporters, notNullValue());
        assertThat(internalExporters.settings.getAsMap().size(), is(2));
        assertThat(internalExporters.settings.getAsMap(), hasEntry("_name0.type", "_type"));
        assertThat(internalExporters.settings.getAsMap(), hasEntry("_name1.type", "_type"));
        assertThat(internalExporters.exporters.size(), is(2));
        assertThat(internalExporters.exporters, hasKey("_name0"));
        assertThat(internalExporters.exporters.get("_name0"), instanceOf(TestFactory.TestExporter.class));
        assertThat(internalExporters.exporters.get("_name0").type, is("_type"));
        assertThat(internalExporters.exporters, hasKey("_name1"));
        assertThat(internalExporters.exporters.get("_name1"), instanceOf(TestFactory.TestExporter.class));
        assertThat(internalExporters.exporters.get("_name1").type, is("_type"));
    }

    public void testInitExportersMultipleSameTypeSingletons() throws Exception {
        Exporter.Factory factory = new TestFactory("_type", true);
        factories.put("_type", factory);
        try {
            exporters.initExporters(Settings.builder()
                    .put("_name0.type", "_type")
                    .put("_name1.type", "_type")
                    .build());
            fail("Expected SettingsException");
        } catch (SettingsException e) {
            assertThat(e.getMessage(), containsString("multiple [_type] exporters are configured. there can only be one"));
        }
    }

    public void testSettingsUpdate() throws Exception {
        Exporter.Factory factory = spy(new TestFactory("_type", false));
        factories.put("_type", factory);

        final AtomicReference<Settings> settingsHolder = new AtomicReference<>();

        exporters = new Exporters(Settings.builder()
                .put("xpack.monitoring.agent.exporters._name0.type", "_type")
                .put("xpack.monitoring.agent.exporters._name1.type", "_type")
                .build(), factories, clusterService, clusterSettings) {
            @Override
            CurrentExporters initExporters(Settings settings) {
                settingsHolder.set(settings);
                return super.initExporters(settings);
            }
        };
        exporters.start();

        assertThat(settingsHolder.get(), notNullValue());
        Map<String, String> settings = settingsHolder.get().getAsMap();
        assertThat(settings.size(), is(2));
        assertThat(settings, hasEntry("_name0.type", "_type"));
        assertThat(settings, hasEntry("_name1.type", "_type"));

        Settings update = Settings.builder()
                .put("xpack.monitoring.agent.exporters._name0.foo", "bar")
                .put("xpack.monitoring.agent.exporters._name1.foo", "bar")
                .build();
        clusterSettings.applySettings(update);
        assertThat(settingsHolder.get(), notNullValue());
        settings = settingsHolder.get().getAsMap();
        assertThat(settings.size(), is(4));
        assertThat(settings, hasEntry("_name0.type", "_type"));
        assertThat(settings, hasEntry("_name0.foo", "bar"));
        assertThat(settings, hasEntry("_name1.type", "_type"));
        assertThat(settings, hasEntry("_name1.foo", "bar"));
    }

    public void testOpenBulkOnMaster() throws Exception {
        Exporter.Factory factory = new MockFactory("mock", false);
        Exporter.Factory masterOnlyFactory = new MockFactory("mock_master_only", true);
        factories.put("mock", factory);
        factories.put("mock_master_only", masterOnlyFactory);
        Exporters exporters = new Exporters(Settings.builder()
                .put("xpack.monitoring.agent.exporters._name0.type", "mock")
                .put("xpack.monitoring.agent.exporters._name1.type", "mock_master_only")
                .build(), factories, clusterService, clusterSettings);
        exporters.start();

        DiscoveryNode localNode = mock(DiscoveryNode.class);
        when(localNode.masterNode()).thenReturn(true);
        when(clusterService.localNode()).thenReturn(localNode);

        ExportBulk bulk = exporters.openBulk();
        assertThat(bulk, notNullValue());

        verify(exporters.getExporter("_name0"), times(1)).masterOnly();
        verify(exporters.getExporter("_name0"), times(1)).openBulk();
        verify(exporters.getExporter("_name1"), times(1)).masterOnly();
        verify(exporters.getExporter("_name1"), times(1)).openBulk();
    }

    public void testExportNotOnMaster() throws Exception {
        Exporter.Factory factory = new MockFactory("mock", false);
        Exporter.Factory masterOnlyFactory = new MockFactory("mock_master_only", true);
        factories.put("mock", factory);
        factories.put("mock_master_only", masterOnlyFactory);
        Exporters exporters = new Exporters(Settings.builder()
                .put("xpack.monitoring.agent.exporters._name0.type", "mock")
                .put("xpack.monitoring.agent.exporters._name1.type", "mock_master_only")
                .build(), factories, clusterService, clusterSettings);
        exporters.start();

        DiscoveryNode localNode = mock(DiscoveryNode.class);
        when(localNode.masterNode()).thenReturn(false);
        when(clusterService.localNode()).thenReturn(localNode);

        ExportBulk bulk = exporters.openBulk();
        assertThat(bulk, notNullValue());

        verify(exporters.getExporter("_name0"), times(1)).masterOnly();
        verify(exporters.getExporter("_name0"), times(1)).openBulk();
        verify(exporters.getExporter("_name1"), times(1)).masterOnly();
        verifyNoMoreInteractions(exporters.getExporter("_name1"));
    }

    static class TestFactory extends Exporter.Factory<TestFactory.TestExporter> {
        public TestFactory(String type, boolean singleton) {
            super(type, singleton);
        }

        @Override
        public TestExporter create(Exporter.Config config) {
            return new TestExporter(type(), config);
        }

        static class TestExporter extends Exporter {
            public TestExporter(String type, Config config) {
                super(type, config);
            }

            @Override
            public void export(Collection<MonitoringDoc> monitoringDocs) throws Exception {
            }

            @Override
            public ExportBulk openBulk() {
                return mock(ExportBulk.class);
            }

            @Override
            public void close() {
            }
        }
    }

    static class MockFactory extends Exporter.Factory<Exporter> {
        private final boolean masterOnly;

        public MockFactory(String type, boolean masterOnly) {
            super(type, false);
            this.masterOnly = masterOnly;
        }

        @Override
        public Exporter create(Exporter.Config config) {
            Exporter exporter = mock(Exporter.class);
            when(exporter.type()).thenReturn(type());
            when(exporter.name()).thenReturn(config.name());
            when(exporter.masterOnly()).thenReturn(masterOnly);
            when(exporter.openBulk()).thenReturn(mock(ExportBulk.class));
            return exporter;
        }
    }

}
