/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.monitoring.MonitoringService;
import org.elasticsearch.xpack.monitoring.cleaner.CleanerService;
import org.elasticsearch.xpack.monitoring.exporter.http.HttpExporter;
import org.elasticsearch.xpack.monitoring.exporter.local.LocalExporter;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ExportersTests extends ESTestCase {
    private Exporters exporters;
    private Map<String, Exporter.Factory> factories;
    private ClusterService clusterService;
    private ClusterState state;
    private SSLService sslService;
    private final ClusterBlocks blocks = mock(ClusterBlocks.class);
    private final Metadata metadata = mock(Metadata.class);
    private final XPackLicenseState licenseState = mock(XPackLicenseState.class);
    private ClusterSettings clusterSettings;
    private ThreadContext threadContext;

    @Before
    public void init() {
        factories = new HashMap<>();

        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        threadContext = new ThreadContext(Settings.EMPTY);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        clusterService = mock(ClusterService.class);
        // default state.version() will be 0, which is "valid"
        state = mock(ClusterState.class);
        Set<Setting<?>> settingsSet = new HashSet<>(Exporters.getSettings());
        settingsSet.add(MonitoringService.INTERVAL);
        clusterSettings = new ClusterSettings(Settings.EMPTY, settingsSet);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(clusterService.state()).thenReturn(state);
        when(state.blocks()).thenReturn(blocks);
        when(state.metadata()).thenReturn(metadata);
        sslService = mock(SSLService.class);

        // we always need to have the local exporter as it serves as the default one
        factories.put(LocalExporter.TYPE, config -> new LocalExporter(config, client, mock(CleanerService.class)));

        exporters = new Exporters(Settings.EMPTY, factories, clusterService, licenseState, threadContext, sslService);
    }

    public void testHostsMustBeSetIfTypeIsHttp() {
        final String prefix = "xpack.monitoring.exporters.example";
        final Settings settings  = Settings.builder().put(prefix + ".type", "http").build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> HttpExporter.TYPE_SETTING.getConcreteSetting(prefix + ".type").get(settings));
        assertThat(e, hasToString(containsString("Failed to parse value [http] for setting [" + prefix + ".type]")));
        assertThat(e.getCause(), instanceOf(SettingsException.class));
        assertThat(e.getCause(), hasToString(containsString("host list for [" + prefix + ".host] is empty")));
    }

    public void testIndexNameTimeFormatMustBeValid() {
        final String prefix = "xpack.monitoring.exporters.example";
        final String setting = ".index.name.time_format";
        final String value = "yyyy.MM.dd.j";
        final Settings settings = Settings.builder().put(prefix + setting, value).build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> Exporter.INDEX_NAME_TIME_FORMAT_SETTING.getConcreteSetting(prefix + setting).get(settings));
        assertThat(e, hasToString(containsString("Invalid format: [" + value + "]: Unknown pattern letter: j")));
        assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(e.getCause(), hasToString(containsString("Unknown pattern letter: j")));
    }

    public void testExporterIndexPattern() {
        Exporter.Config config = mock(Exporter.Config.class);
        when(config.name()).thenReturn("anything");
        when(config.settings()).thenReturn(Settings.EMPTY);
        DateFormatter formatter = Exporter.dateTimeFormatter(config);
        Instant instant = Instant.ofEpochSecond(randomLongBetween(0, 86400 * 365 * 130L));
        ZonedDateTime zonedDateTime = instant.atZone(ZoneOffset.UTC);
        int year = zonedDateTime.getYear();
        int month = zonedDateTime.getMonthValue();
        int day = zonedDateTime.getDayOfMonth();
        String expecdateDate = String.format(Locale.ROOT, "%02d.%02d.%02d", year, month, day);
        String formattedDate = formatter.format(instant);
        assertThat("input date was " + instant, expecdateDate, is(formattedDate));
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
        factories.put("local", TestExporter::new);
        Map<String, Exporter> internalExporters = exporters.initExporters(Settings.builder()
                .put("xpack.monitoring.exporters._name.type", "local")
                .build());

        assertThat(internalExporters, notNullValue());
        assertThat(internalExporters.size(), is(1));
        assertThat(internalExporters, hasKey("_name"));
        assertThat(internalExporters.get("_name"), instanceOf(TestExporter.class));
        assertThat(internalExporters.get("_name").config().type(), is("local"));
    }

    public void testInitExportersSingleDisabled() throws Exception {
        factories.put("local", TestExporter::new);
        Map<String, Exporter> internalExporters = exporters.initExporters(Settings.builder()
                .put("xpack.monitoring.exporters._name.type", "local")
                .put("xpack.monitoring.exporters._name.enabled", false)
                .build());

        assertThat(internalExporters, notNullValue());

        // the only configured exporter is disabled... yet we intentionally don't fallback on the default
        assertThat(internalExporters.size(), is(0));
    }

    public void testInitExportersSingleUnknownType() throws Exception {
        SettingsException e = expectThrows(SettingsException.class, () -> exporters.initExporters(Settings.builder()
                .put("xpack.monitoring.exporters._name.type", "unknown_type")
                .build()));
        assertThat(e.getMessage(), containsString("unknown exporter type [unknown_type]"));
    }

    public void testInitExportersSingleMissingExporterType() throws Exception {
        SettingsException e = expectThrows(SettingsException.class, () -> exporters.initExporters(
                Settings.builder().put("xpack.monitoring.exporters._name.foo", "bar").build()));
        assertThat(e.getMessage(), containsString("missing exporter type for [_name]"));
    }

    public void testInitExportersMultipleSameType() throws Exception {
        factories.put("_type", TestExporter::new);
        Map<String, Exporter> internalExporters = exporters.initExporters(Settings.builder()
                .put("xpack.monitoring.exporters._name0.type", "_type")
                .put("xpack.monitoring.exporters._name1.type", "_type")
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
        factories.put("local", TestSingletonExporter::new);
        SettingsException e = expectThrows(SettingsException.class, () ->
            exporters.initExporters(Settings.builder()
                    .put("xpack.monitoring.exporters._name0.type", "local")
                    .put("xpack.monitoring.exporters._name1.type", "local")
                    .build())
        );
        assertThat(e.getMessage(), containsString("multiple [local] exporters are configured. there can only be one"));
    }

    public void testSettingsUpdate() throws Exception {
        factories.put("http", TestExporter::new);
        factories.put("local", TestExporter::new);

        final AtomicReference<Settings> settingsHolder = new AtomicReference<>();

        Settings nodeSettings = Settings.builder()
                .put("xpack.monitoring.exporters._name0.type", "local")
                .put("xpack.monitoring.exporters._name1.type", "http")
                .build();
        clusterSettings = new ClusterSettings(nodeSettings, new HashSet<>(Exporters.getSettings()));
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        exporters = new Exporters(nodeSettings, factories, clusterService, licenseState, threadContext, sslService) {
            @Override
            Map<String, Exporter> initExporters(Settings settings) {
                settingsHolder.set(settings);
                return super.initExporters(settings);
            }
        };
        exporters.start();

        assertThat(settingsHolder.get(), notNullValue());
        Settings settings = settingsHolder.get();
        assertThat(settings.size(), is(2));
        assertEquals(settings.get("xpack.monitoring.exporters._name0.type"), "local");
        assertEquals(settings.get("xpack.monitoring.exporters._name1.type"), "http");

        Settings update = Settings.builder()
                .put("xpack.monitoring.exporters._name0.use_ingest", true)
                .put("xpack.monitoring.exporters._name1.use_ingest", false)
                .build();
        clusterSettings.applySettings(update);
        assertThat(settingsHolder.get(), notNullValue());
        settings = settingsHolder.get();
        logger.info(settings);
        assertThat(settings.size(), is(4));
        assertEquals(settings.get("xpack.monitoring.exporters._name0.type"), "local");
        assertEquals(settings.get("xpack.monitoring.exporters._name0.use_ingest"), "true");
        assertEquals(settings.get("xpack.monitoring.exporters._name1.type"), "http");
        assertEquals(settings.get("xpack.monitoring.exporters._name1.use_ingest"), "false");
    }

    public void testExporterBlocksOnClusterState() {
        if (rarely()) {
            when(metadata.clusterUUID()).thenReturn(ClusterState.UNKNOWN_UUID);
        } else if (rarely()) {
            when(blocks.hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)).thenReturn(true);
        } else {
            when(state.version()).thenReturn(ClusterState.UNKNOWN_VERSION);
        }

        final int nbExporters = randomIntBetween(1, 5);
        final Settings.Builder settings = Settings.builder();

        for (int i = 0; i < nbExporters; i++) {
            settings.put("xpack.monitoring.exporters._name" + String.valueOf(i) + ".type", "record");
        }

        final Exporters exporters = new Exporters(settings.build(), factories, clusterService, licenseState, threadContext, sslService);

        // synchronously checks the cluster state
        exporters.wrapExportBulk(ActionListener.wrap(
            bulk -> assertThat(bulk, is(nullValue())),
            e -> fail(e.getMessage())
        ));

        verify(state).blocks();
    }

    /**
     * Verifies that, when no exporters are enabled, the {@code Exporters} will still return as expected.
     */
    public void testNoExporters() throws Exception {
        Settings.Builder settings =
            Settings.builder()
                    .put("xpack.monitoring.exporters.explicitly_disabled.type", "local")
                    .put("xpack.monitoring.exporters.explicitly_disabled.enabled", false);

        Exporters exporters = new Exporters(settings.build(), factories, clusterService, licenseState, threadContext, sslService);
        exporters.start();

        assertThat(exporters.getEnabledExporters(), empty());

        assertExporters(exporters);

        exporters.close();
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

        Exporters exporters = new Exporters(settings.build(), factories, clusterService, licenseState, threadContext, sslService);
        exporters.start();

        assertThat(exporters.getEnabledExporters(), hasSize(nbExporters));

        final int total = assertExporters(exporters);

        for (Exporter exporter : exporters.getEnabledExporters()) {
            assertThat(exporter, instanceOf(CountingExporter.class));
            assertThat(((CountingExporter) exporter).getExportedCount(), equalTo(total));
        }

        exporters.close();
    }

    /**
     * All dynamic monitoring settings should have dependency on type.
     */
    public void testSettingsDependency() {
        List<Setting.AffixSetting<?>> settings = Exporters.getSettings().stream().filter(Setting::isDynamic).collect(Collectors.toList());
        settings.stream().filter(s -> s.getKey().equals("xpack.monitoring.exporters.*.type") == false)
            .forEach(setting -> assertThat(setting.getKey() + " does not have a dependency on type",
                setting.getDependencies().stream().map(Setting.AffixSettingDependency::getSetting).distinct().collect(Collectors.toList()),
                contains(Exporter.TYPE_SETTING)));
    }

    /**
     * This is a variation of testing that all settings validated at cluster state update ensure that the type is set. This workflow
     * emulates adding valid settings, then attempting to remove the type. This should never be allowed since type if type is null
     * then any associated settings are extraneous and thus invalid (and can cause validation issues on cluster state application).
     */
    public void testRemoveType() {
        //run the update for all dynamic settings and ensure that they correctly throw an exception
        List<Setting.AffixSetting<?>> settings = Exporters.getSettings().stream().filter(Setting::isDynamic).collect(Collectors.toList());
        settings.stream().filter(s -> s.getKey().equals("xpack.monitoring.exporters.*.type") == false)
            .forEach(setting -> {
                String fullSettingName = setting.getKey().replace("*", "foobar");
                Settings nodeSettings = Settings.builder()
                    .put("xpack.monitoring.exporters.foobar.type", randomFrom("local, http")) //actual type should not matter
                    .put(fullSettingName, "")
                    .build();

                clusterSettings = new ClusterSettings(nodeSettings, new HashSet<>(Exporters.getSettings()));
                when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

                Settings update = Settings.builder()
                    .put("xpack.monitoring.exporters.foobar.type", (String) null)
                    .build();

                Settings.Builder target = Settings.builder().put(nodeSettings);
                clusterSettings.updateDynamicSettings(update, target, Settings.builder(), "persistent");
                IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                    () -> clusterSettings.validate(target.build(), true));
                assertThat(e.getMessage(),
                    containsString("missing required setting [xpack.monitoring.exporters.foobar.type] for setting [" + fullSettingName));
            });
    }

    /**
     * Attempt to export a random number of documents via {@code exporters} from multiple threads.
     *
     * @param exporters The setup / started exporters instance to use.
     * @return The total number of documents sent to the {@code exporters}.
     */
    private int assertExporters(final Exporters exporters) throws InterruptedException {
        final Thread[] threads = new Thread[3 + randomInt(7)];
        final CyclicBarrier barrier = new CyclicBarrier(threads.length);
        final List<Throwable> exceptions = new CopyOnWriteArrayList<>();
        final AtomicInteger counter = new AtomicInteger(threads.length);

        int total = 0;

        for (int i = 0; i < threads.length; i++) {
            final int threadDocs = randomIntBetween(10, 50);
            final int threadNum = i;

            total += threadDocs;

            threads[i] = new Thread(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    exceptions.add(e);
                }

                @Override
                protected void doRun() throws Exception {
                    final List<MonitoringDoc> docs = new ArrayList<>();
                    for (int n = 0; n < threadDocs; n++) {
                        docs.add(new TestMonitoringDoc(randomAlphaOfLength(5), randomNonNegativeLong(), randomNonNegativeLong(),
                                                       null, MonitoredSystem.ES, randomAlphaOfLength(5), null, String.valueOf(n)));
                    }
                    exporters.export(docs, ActionListener.wrap(
                        r -> {
                            counter.decrementAndGet();
                            logger.debug("--> thread [{}] successfully exported {} documents", threadNum, threadDocs);
                        },
                        e -> {
                            exceptions.add(e);
                            logger.debug("--> thread [{}] failed to export {} documents", threadNum, threadDocs);
                        })
                    );
                    barrier.await(10, TimeUnit.SECONDS);
                }
            }, "export_thread_" + i);

            threads[i].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        assertThat(exceptions, empty());
        assertThat(counter.get(), is(0));

        return total;
    }

    static class TestExporter extends Exporter {
        TestExporter(Config config) {
            super(config);
        }

        @Override
        public void openBulk(final ActionListener<ExportBulk> listener) {
            listener.onResponse(mock(ExportBulk.class));
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

    static class CountingExporter extends Exporter {

        private static final AtomicInteger count = new AtomicInteger(0);
        private List<CountingBulk> bulks = new CopyOnWriteArrayList<>();
        private final ThreadContext threadContext;

        CountingExporter(Config config, ThreadContext threadContext) {
            super(config);
            this.threadContext = threadContext;
        }

        @Override
        public void openBulk(final ActionListener<ExportBulk> listener) {
            CountingBulk bulk = new CountingBulk(config.type() + "#" + count.getAndIncrement(), threadContext);
            bulks.add(bulk);

            listener.onResponse(bulk);
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

        int getCount() {
            return count.get();
        }
    }

    static class TestMonitoringDoc extends MonitoringDoc {

        private final String value;

        TestMonitoringDoc(String cluster, long timestamp, long interval,
                          Node node, MonitoredSystem system, String type, String id, String value) {
            super(cluster, timestamp, interval, node, system, type, id);
            this.value = value;
        }

        @Override
        protected void innerToXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("test", value);
        }
    }
}
