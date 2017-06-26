/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.monitoring.MonitoringSettings;
import org.elasticsearch.xpack.monitoring.exporter.local.LocalExporter;
import org.elasticsearch.xpack.monitoring.cleaner.CleanerService;
import org.elasticsearch.xpack.security.InternalClient;
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
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExportersTests extends ESTestCase {
    private Exporters exporters;
    private Map<String, Exporter.Factory> factories;
    private ClusterService clusterService;
    private ClusterState state;
    private final XPackLicenseState licenseState = mock(XPackLicenseState.class);
    private ClusterSettings clusterSettings;
    private ThreadContext threadContext;

    @Before
    public void init() throws Exception {
        factories = new HashMap<>();

        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        threadContext = new ThreadContext(Settings.EMPTY);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        InternalClient internalClient = new InternalClient(Settings.EMPTY, threadPool, client);
        clusterService = mock(ClusterService.class);
        // default state.version() will be 0, which is "valid"
        state = mock(ClusterState.class);
        clusterSettings = new ClusterSettings(Settings.EMPTY,
                new HashSet<>(Arrays.asList(MonitoringSettings.INTERVAL, MonitoringSettings.EXPORTERS_SETTINGS)));
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(clusterService.state()).thenReturn(state);

        // we always need to have the local exporter as it serves as the default one
        factories.put(LocalExporter.TYPE, config -> new LocalExporter(config, internalClient, mock(CleanerService.class)));

        exporters = new Exporters(Settings.EMPTY, factories, clusterService, licenseState, threadContext);
    }

    public void testInitExportersDefault() throws Exception {
        factories.put("_type", TestExporter::new);
        Map<String, Exporter> internalExporters = exporters.initExporters(Settings.builder().build());

        assertThat(internalExporters, notNullValue());
        assertThat(internalExporters.size(), is(1));
        assertThat(internalExporters, hasKey("default_" + LocalExporter.TYPE));
        assertThat(internalExporters.get("default_" + LocalExporter.TYPE), instanceOf(LocalExporter.class));
    }

    public void testInitExportersSingle() throws Exception {
        factories.put("_type", TestExporter::new);
        Map<String, Exporter> internalExporters = exporters.initExporters(Settings.builder()
                .put("_name.type", "_type")
                .build());

        assertThat(internalExporters, notNullValue());
        assertThat(internalExporters.size(), is(1));
        assertThat(internalExporters, hasKey("_name"));
        assertThat(internalExporters.get("_name"), instanceOf(TestExporter.class));
        assertThat(internalExporters.get("_name").config().type(), is("_type"));
    }

    public void testInitExportersSingleDisabled() throws Exception {
        factories.put("_type", TestExporter::new);
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
        factories.put("_type", TestExporter::new);
        Map<String, Exporter> internalExporters = exporters.initExporters(Settings.builder()
                .put("_name0.type", "_type")
                .put("_name1.type", "_type")
                .build());

        assertThat(internalExporters, notNullValue());
        assertThat(internalExporters.size(), is(2));
        assertThat(internalExporters, hasKey("_name0"));
        assertThat(internalExporters.get("_name0"), instanceOf(TestExporter.class));
        assertThat(internalExporters.get("_name0").config().type(), is("_type"));
        assertThat(internalExporters, hasKey("_name1"));
        assertThat(internalExporters.get("_name1"), instanceOf(TestExporter.class));
        assertThat(internalExporters.get("_name1").config().type(), is("_type"));
    }

    public void testInitExportersMultipleSameTypeSingletons() throws Exception {
        factories.put("_type", TestSingletonExporter::new);
        SettingsException e = expectThrows(SettingsException.class, () ->
            exporters.initExporters(Settings.builder()
                    .put("_name0.type", "_type")
                    .put("_name1.type", "_type")
                    .build())
        );
        assertThat(e.getMessage(), containsString("multiple [_type] exporters are configured. there can only be one"));
    }

    public void testSettingsUpdate() throws Exception {
        factories.put("_type", TestExporter::new);

        final AtomicReference<Settings> settingsHolder = new AtomicReference<>();

        Settings nodeSettings = Settings.builder()
                .put("xpack.monitoring.exporters._name0.type", "_type")
                .put("xpack.monitoring.exporters._name1.type", "_type")
                .build();
        clusterSettings = new ClusterSettings(nodeSettings, new HashSet<>(Arrays.asList(MonitoringSettings.EXPORTERS_SETTINGS)));
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        exporters = new Exporters(nodeSettings, factories, clusterService, licenseState, threadContext) {
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
                .put("xpack.monitoring.exporters._name0.foo", "bar")
                .put("xpack.monitoring.exporters._name1.foo", "bar")
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

    public void testExporterBlocksOnClusterState() {
        when(state.version()).thenReturn(ClusterState.UNKNOWN_VERSION);

        final int nbExporters = randomIntBetween(1, 5);
        final Settings.Builder settings = Settings.builder();

        for (int i = 0; i < nbExporters; i++) {
            settings.put("xpack.monitoring.exporters._name" + String.valueOf(i) + ".type", "record");
        }

        final Exporters exporters = new Exporters(settings.build(), factories, clusterService, licenseState, threadContext);

        assertThat(exporters.openBulk(), nullValue());
    }

    /**
     * This test creates N threads that export a random number of document
     * using a {@link Exporters} instance.
     */
    public void testConcurrentExports() throws Exception {
        final int nbExporters = randomIntBetween(1, 5);

        Settings.Builder settings = Settings.builder();
        for (int i = 0; i < nbExporters; i++) {
            settings.put("xpack.monitoring.exporters._name" + String.valueOf(i) + ".type", "record");
        }

        factories.put("record", (s) -> new CountingExporter(s, threadContext));

        Exporters exporters = new Exporters(settings.build(), factories, clusterService, licenseState, threadContext);
        exporters.start();

        final Thread[] threads = new Thread[3 + randomInt(7)];
        final CyclicBarrier barrier = new CyclicBarrier(threads.length);
        final List<Throwable> exceptions = new CopyOnWriteArrayList<>();

        int total = 0;

        for (int i = 0; i < threads.length; i++) {
            int nbDocs = randomIntBetween(10, 50);
            total += nbDocs;

            final int threadNum = i;
            final int threadDocs = nbDocs;

            threads[i] = new Thread(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    exceptions.add(e);
                }

                @Override
                protected void doRun() throws Exception {
                    List<MonitoringDoc> docs = new ArrayList<>();
                    for (int n = 0; n < threadDocs; n++) {
                        docs.add(new MonitoringDoc(MonitoredSystem.ES.getSystem(),
                                Version.CURRENT.toString(), null, null, null, 0L,
                                (MonitoringDoc.Node) null));
                    }
                    barrier.await(10, TimeUnit.SECONDS);
                    exporters.export(docs, ActionListener.wrap(
                            r -> logger.debug("--> thread [{}] successfully exported {} documents", threadNum, threadDocs),
                            e -> logger.debug("--> thread [{}] failed to export {} documents", threadNum, threadDocs)));

                }
            }, "export_thread_" + i);
            threads[i].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        assertThat(exceptions, empty());
        for (Exporter exporter : exporters) {
            assertThat(exporter, instanceOf(CountingExporter.class));
            assertThat(((CountingExporter) exporter).getExportedCount(), equalTo(total));
        }

        exporters.close();
    }

    static class TestExporter extends Exporter {
        TestExporter(Config config) {
            super(config);
        }

        @Override
        public ExportBulk openBulk() {
            return mock(ExportBulk.class);
        }

        @Override
        public void doClose() {
        }
    }

    static class TestSingletonExporter extends TestExporter {
        TestSingletonExporter(Config config) {
            super(config);
        }

        @Override
        public boolean isSingleton() {
            return true;
        }
    }


    static class MockFactory implements Exporter.Factory {

        @Override
        public Exporter create(Exporter.Config config) {
            Exporter exporter = mock(Exporter.class);
            when(exporter.name()).thenReturn(config.name());
            when(exporter.openBulk()).thenReturn(mock(ExportBulk.class));
            return exporter;
        }

    }

    static class CountingExporter extends Exporter {

        private static final AtomicInteger count = new AtomicInteger(0);
        private List<CountingBulk> bulks = new CopyOnWriteArrayList<>();
        private final ThreadContext threadContext;

        CountingExporter(Config config, ThreadContext threadContext) {
            super(config);
            this.threadContext = threadContext;
        }

        @Override
        public ExportBulk openBulk() {
            CountingBulk bulk = new CountingBulk(config.type() + "#" + count.getAndIncrement(), threadContext);
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

        CountingBulk(String name, ThreadContext threadContext) {
            super(name, threadContext);
        }

        @Override
        protected void doAdd(Collection<MonitoringDoc> docs) throws ExportException {
            count.addAndGet(docs.size());
        }

        @Override
        protected void doFlush(ActionListener<Void> listener) {
            listener.onResponse(null);
        }

        @Override
        protected void doClose(ActionListener<Void> listener) {
            listener.onResponse(null);
        }

        int getCount() {
            return count.get();
        }
    }
}
