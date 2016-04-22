/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter;

import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.marvel.MonitoringSettings;
import org.elasticsearch.marvel.MonitoredSystem;
import org.elasticsearch.marvel.agent.exporter.local.LocalExporter;
import org.elasticsearch.marvel.cleaner.CleanerService;
import org.elasticsearch.marvel.support.init.proxy.MonitoringClientProxy;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
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
        factories.put(LocalExporter.TYPE, new LocalExporter.Factory(MonitoringClientProxy.of(client), clusterService,
                mock(CleanerService.class)));
        clusterSettings = new ClusterSettings(Settings.EMPTY, new HashSet<>(Arrays.asList(MonitoringSettings.COLLECTORS,
                MonitoringSettings.INTERVAL, MonitoringSettings.EXPORTERS_SETTINGS)));
        exporters = new Exporters(Settings.EMPTY, factories, clusterService, clusterSettings);
    }

    public void testInitExportersDefault() throws Exception {
        Exporter.Factory factory = new TestFactory("_type", true);
        factories.put("_type", factory);
        Map<String, Exporter> internalExporters = exporters.initExporters(Settings.builder().build());

        assertThat(internalExporters, notNullValue());
        assertThat(internalExporters.size(), is(1));
        assertThat(internalExporters, hasKey("default_" + LocalExporter.TYPE));
        assertThat(internalExporters.get("default_" + LocalExporter.TYPE), instanceOf(LocalExporter.class));
    }

    public void testInitExportersSingle() throws Exception {
        Exporter.Factory factory = new TestFactory("_type", true);
        factories.put("_type", factory);
        Map<String, Exporter> internalExporters = exporters.initExporters(Settings.builder()
                .put("_name.type", "_type")
                .build());

        assertThat(internalExporters, notNullValue());
        assertThat(internalExporters.size(), is(1));
        assertThat(internalExporters, hasKey("_name"));
        assertThat(internalExporters.get("_name"), instanceOf(TestFactory.TestExporter.class));
        assertThat(internalExporters.get("_name").type, is("_type"));
    }

    public void testInitExportersSingleDisabled() throws Exception {
        Exporter.Factory factory = new TestFactory("_type", true);
        factories.put("_type", factory);
        Map<String, Exporter> internalExporters = exporters.initExporters(Settings.builder()
                .put("_name.type", "_type")
                .put("_name.enabled", false)
                .build());

        assertThat(internalExporters, notNullValue());

        // the only configured exporter is disabled... yet we intentionally don't fallback on the default
        assertThat(internalExporters.size(), is(0));
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
        Map<String, Exporter> internalExporters = exporters.initExporters(Settings.builder()
                .put("_name0.type", "_type")
                .put("_name1.type", "_type")
                .build());

        assertThat(internalExporters, notNullValue());
        assertThat(internalExporters.size(), is(2));
        assertThat(internalExporters, hasKey("_name0"));
        assertThat(internalExporters.get("_name0"), instanceOf(TestFactory.TestExporter.class));
        assertThat(internalExporters.get("_name0").type, is("_type"));
        assertThat(internalExporters, hasKey("_name1"));
        assertThat(internalExporters.get("_name1"), instanceOf(TestFactory.TestExporter.class));
        assertThat(internalExporters.get("_name1").type, is("_type"));
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

        Settings nodeSettings = Settings.builder()
                .put("xpack.monitoring.agent.exporters._name0.type", "_type")
                .put("xpack.monitoring.agent.exporters._name1.type", "_type")
                .build();
        clusterSettings = new ClusterSettings(nodeSettings, new HashSet<>(Arrays.asList(MonitoringSettings.EXPORTERS_SETTINGS)));

        exporters = new Exporters(nodeSettings, factories, clusterService, clusterSettings) {
            @Override
            Map<String, Exporter> initExporters(Settings settings) {
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

        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(nodes.isLocalNodeElectedMaster()).thenReturn(true);
        when(clusterService.state()).thenReturn(ClusterState.builder(ClusterName.DEFAULT).nodes(nodes).build());

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

        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(nodes.isLocalNodeElectedMaster()).thenReturn(false);
        when(clusterService.state()).thenReturn(ClusterState.builder(ClusterName.DEFAULT).nodes(nodes).build());

        ExportBulk bulk = exporters.openBulk();
        assertThat(bulk, notNullValue());

        verify(exporters.getExporter("_name0"), times(1)).masterOnly();
        verify(exporters.getExporter("_name0"), times(1)).openBulk();
        verify(exporters.getExporter("_name1"), times(1)).masterOnly();
        verifyNoMoreInteractions(exporters.getExporter("_name1"));
    }

    /**
     * This test creates N threads that export a random number of document
     * using a {@link Exporters} instance.
     */
    public void testConcurrentExports() throws Exception {
        final int nbExporters = randomIntBetween(1, 5);

        logger.info("--> creating {} exporters", nbExporters);
        Settings.Builder settings = Settings.builder();
        for (int i = 0; i < nbExporters; i++) {
            settings.put("xpack.monitoring.agent.exporters._name" + String.valueOf(i) + ".type", "record");
        }

        Exporter.Factory factory = new CountingExportFactory("record", false);
        factories.put("record", factory);

        Exporters exporters = new Exporters(settings.build(), factories, clusterService, clusterSettings);
        exporters.start();

        final Thread[] threads = new Thread[3 + randomInt(7)];
        final CyclicBarrier barrier = new CyclicBarrier(threads.length);
        final List<Throwable> exceptions = new CopyOnWriteArrayList<>();

        int total = 0;

        logger.info("--> exporting documents using {} threads", threads.length);
        for (int i = 0; i < threads.length; i++) {
            int nbDocs = randomIntBetween(10, 50);
            total += nbDocs;

            final int threadNum = i;
            final int threadDocs = nbDocs;

            logger.debug("--> exporting thread [{}] exports {} documents", threadNum, threadDocs);
            threads[i] = new Thread(new AbstractRunnable() {
                @Override
                public void onFailure(Throwable t) {
                    logger.error("unexpected error in exporting thread", t);
                    exceptions.add(t);
                }

                @Override
                protected void doRun() throws Exception {
                    List<MonitoringDoc> docs = new ArrayList<>();
                    for (int n = 0; n < threadDocs; n++) {
                        docs.add(new MonitoringDoc(MonitoredSystem.ES.getSystem(), Version.CURRENT.toString()));
                    }
                    barrier.await(10, TimeUnit.SECONDS);
                    try {
                        exporters.export(docs);
                        logger.debug("--> thread [{}] successfully exported {} documents", threadNum, threadDocs);
                    } catch (Exception e) {
                        logger.debug("--> thread [{}] failed to export {} documents", threadNum, threadDocs);
                    }

                }
            }, "export_thread_" + i);
            threads[i].start();
        }

        logger.info("--> waiting for threads to exports {} documents", total);
        for (Thread thread : threads) {
            thread.join();
        }

        assertThat(exceptions, empty());
        for (Exporter exporter : exporters) {
            assertThat(exporter, instanceOf(CountingExportFactory.CountingExporter.class));
            assertThat(((CountingExportFactory.CountingExporter) exporter).getExportedCount(), equalTo(total));
        }

        exporters.close();
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
            public ExportBulk openBulk() {
                return mock(ExportBulk.class);
            }

            @Override
            public void doClose() {
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

    /**
     * A factory of exporters that count the number of exported documents.
     */
    static class CountingExportFactory extends Exporter.Factory<CountingExportFactory.CountingExporter> {

        public CountingExportFactory(String type, boolean singleton) {
            super(type, singleton);
        }

        @Override
        public CountingExporter create(Exporter.Config config) {
            return new CountingExporter(type(), config);
        }

        static class CountingExporter extends Exporter {

            private static final AtomicInteger count = new AtomicInteger(0);
            private List<CountingBulk> bulks = new CopyOnWriteArrayList<>();

            public CountingExporter(String type, Config config) {
                super(type, config);
            }

            @Override
            public ExportBulk openBulk() {
                CountingBulk bulk = new CountingBulk(type + "#" + count.getAndIncrement());
                bulks.add(bulk);
                return bulk;
            }

            @Override
            public void doClose() {
            }

            public int getExportedCount() {
                int exported = 0;
                for (CountingBulk bulk : bulks) {
                    exported += bulk.getCount();
                }
                return exported;
            }
        }

        static class CountingBulk extends ExportBulk {

            private final AtomicInteger count = new AtomicInteger();

            public CountingBulk(String name) {
                super(name);
            }

            @Override
            protected void doAdd(Collection<MonitoringDoc> docs) throws ExportException {
                count.addAndGet(docs.size());
            }

            @Override
            protected void doFlush() {
            }

            @Override
            protected void doClose() throws ExportException {
            }

            int getCount() {
                return count.get();
            }
        }
    }
}
