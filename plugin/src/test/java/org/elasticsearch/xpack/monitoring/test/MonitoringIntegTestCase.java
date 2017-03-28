/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.test;

import io.netty.util.internal.SystemPropertyUtil;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.TestCluster;
import org.elasticsearch.test.store.MockFSIndexStore;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xpack.XPackClient;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.monitoring.MonitoringService;
import org.elasticsearch.xpack.monitoring.MonitoringSettings;
import org.elasticsearch.xpack.monitoring.client.MonitoringClient;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.monitoring.resolver.MonitoringIndexNameResolver;
import org.elasticsearch.xpack.monitoring.resolver.ResolversRegistry;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.authc.file.FileRealm;
import org.elasticsearch.xpack.security.authc.support.Hasher;
import org.elasticsearch.xpack.security.authc.support.SecuredString;
import org.elasticsearch.xpack.security.crypto.CryptoService;
import org.elasticsearch.xpack.watcher.WatcherLifeCycleService;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

public abstract class MonitoringIntegTestCase extends ESIntegTestCase {

    public static final String MONITORING_INDICES_PREFIX = MonitoringIndexNameResolver.PREFIX + MonitoringIndexNameResolver.DELIMITER;

    /**
     * Enables individual tests to control the behavior.
     * <p>
     * Control this by overriding {@link #enableSecurity()}, which defaults to enabling it randomly.
     */
    // TODO: what is going on here?
    // SCARY: This needs to be static or lots of tests randomly fail, but it's not used statically!
    protected static Boolean securityEnabled;
    /**
     * Enables individual tests to control the behavior.
     * <p>
     * Control this by overriding {@link #enableWatcher()}, which defaults to disabling it (this will change!).
     */
    protected Boolean watcherEnabled;

    @Override
    protected TestCluster buildTestCluster(Scope scope, long seed) throws IOException {
        if (securityEnabled == null) {
            securityEnabled = enableSecurity();
        }
        if (watcherEnabled == null) {
            watcherEnabled = enableWatcher();
        }

        logger.debug("--> security {}", securityEnabled ? "enabled" : "disabled");
        logger.debug("--> watcher {}", watcherEnabled ? "enabled" : "disabled");

        return super.buildTestCluster(scope, seed);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(XPackSettings.WATCHER_ENABLED.getKey(), watcherEnabled)
                // Disable native ML autodetect_process as the c++ controller won't be available
                .put(MachineLearning.AUTODETECT_PROCESS.getKey(), false)
                .put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), false)
                // we do this by default in core, but for monitoring this isn't needed and only adds noise.
                .put("index.store.mock.check_index_on_close", false);

        SecuritySettings.apply(securityEnabled, builder);

        return builder.build();
    }

    @Override
    protected Settings transportClientSettings() {
        if (securityEnabled) {
            return Settings.builder()
                    .put(super.transportClientSettings())
                    .put("client.transport.sniff", false)
                    .put(Security.USER_SETTING.getKey(), "test:changeme")
                    .put(NetworkModule.TRANSPORT_TYPE_KEY, Security.NAME4)
                    .put(NetworkModule.HTTP_TYPE_KEY, Security.NAME4)
                    .build();
        }
        return Settings.builder().put(super.transportClientSettings())
                .put("xpack.security.enabled", false)
                .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        Set<Class<? extends Plugin>> plugins = new HashSet<>(super.getMockPlugins());
        plugins.remove(MockTransportService.TestPlugin.class); // security has its own transport service
        plugins.add(MockFSIndexStore.TestPlugin.class);
        return plugins;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(XPackPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
    }

    @Override
    protected Function<Client,Client> getClientWrapper() {
        if (securityEnabled == false) {
            return Function.identity();
        }
        Map<String, String> headers = Collections.singletonMap("Authorization",
                basicAuthHeaderValue(SecuritySettings.TEST_USERNAME, new SecuredString(SecuritySettings.TEST_PASSWORD.toCharArray())));
        return client -> (client instanceof NodeClient) ? client.filterWithHeader(headers) : client;
    }

    protected MonitoringClient monitoringClient() {
        Client client = securityEnabled ? internalCluster().transportClient() : client();
        return randomBoolean() ? new XPackClient(client).monitoring() : new MonitoringClient(client);
    }

    @Override
    protected Set<String> excludeTemplates() {
        return monitoringTemplateNames();
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        startMonitoringService();
    }

    @After
    public void tearDown() throws Exception {
        if (watcherEnabled != null && watcherEnabled) {
            internalCluster().getInstance(WatcherLifeCycleService.class, internalCluster().getMasterName()).stop();
        }
        stopMonitoringService();
        super.tearDown();
    }

    /**
     * Override and return {@code false} to force running without Security.
     */
    protected boolean enableSecurity() {
        return randomBoolean();
    }

    /**
     * Override and return {@code false} to force running without Watcher.
     */
    protected boolean enableWatcher() {
        // Once randomDefault() becomes the default again, then this should only be actively disabled when
        // trying to figure out exactly how many indices are at play
        return false;
    }

    protected void startMonitoringService() {
        internalCluster().getInstances(MonitoringService.class).forEach(MonitoringService::start);
    }

    protected void stopMonitoringService() {
        internalCluster().getInstances(MonitoringService.class).forEach(MonitoringService::stop);
    }

    protected void wipeMonitoringIndices() throws Exception {
        CountDown retries = new CountDown(3);
        assertBusy(() -> {
            try {
                boolean exist = client().admin().indices().prepareExists(MONITORING_INDICES_PREFIX + "*")
                        .get().isExists();
                if (exist) {
                    deleteMonitoringIndices();
                } else {
                    retries.countDown();
                }
            } catch (IndexNotFoundException e) {
                retries.countDown();
            }
            assertThat(retries.isCountedDown(), is(true));
        });
    }

    protected void deleteMonitoringIndices() {
        assertAcked(client().admin().indices().prepareDelete(MONITORING_INDICES_PREFIX + "*"));
    }

    protected void awaitMonitoringDocsCount(Matcher<Long> matcher, String... types) throws Exception {
        assertBusy(() -> assertMonitoringDocsCount(matcher, types), 30, TimeUnit.SECONDS);
    }

    protected void ensureMonitoringIndicesYellow() {
        ensureYellow(".monitoring-es-*");
    }

    protected void assertMonitoringDocsCount(Matcher<Long> matcher, String... types) {
        flushAndRefresh(MONITORING_INDICES_PREFIX + "*");
        long count = client().prepareSearch(MONITORING_INDICES_PREFIX + "*").setSize(0).setTypes(types).get().getHits().getTotalHits();
        logger.trace("--> searched for [{}] documents, found [{}]", Strings.arrayToCommaDelimitedString(types), count);
        assertThat(count, matcher);
    }

    protected List<Tuple<String, String>> monitoringTemplates() {
        return StreamSupport.stream(new ResolversRegistry(Settings.EMPTY).spliterator(), false)
                .map((resolver) -> new Tuple<>(resolver.templateName(), resolver.template()))
                .distinct()
                .collect(Collectors.toList());
    }

    protected Set<String> monitoringTemplateNames() {
        return StreamSupport.stream(new ResolversRegistry(Settings.EMPTY).spliterator(), false)
                .map(MonitoringIndexNameResolver::templateName)
                .collect(Collectors.toSet());
    }

    protected void assertTemplateInstalled(String name) {
        boolean found = false;
        for (IndexTemplateMetaData template : client().admin().indices().prepareGetTemplates().get().getIndexTemplates()) {
            if (Regex.simpleMatch(name, template.getName())) {
                found =  true;
            }
        }
        assertTrue("failed to find a template matching [" + name + "]", found);
    }

    protected void waitForMonitoringTemplate(String name) throws Exception {
        assertBusy(new Runnable() {
            @Override
            public void run() {
                assertTemplateInstalled(name);
            }
        }, 30, TimeUnit.SECONDS);
    }

    protected void waitForMonitoringTemplates() throws Exception {
        assertBusy(() -> monitoringTemplateNames().forEach(this::assertTemplateInstalled), 30, TimeUnit.SECONDS);
    }

    protected void waitForMonitoringIndices() throws Exception {
        awaitIndexExists(MONITORING_INDICES_PREFIX + "*");
        assertBusy(this::ensureMonitoringIndicesYellow);
    }

    protected void awaitIndexExists(final String index) throws Exception {
        assertBusy(() -> {
            assertIndicesExists(index);
        }, 30, TimeUnit.SECONDS);
    }

    protected void assertIndicesExists(String... indices) {
        logger.trace("checking if index exists [{}]", Strings.arrayToCommaDelimitedString(indices));
        assertThat(client().admin().indices().prepareExists(indices).get().isExists(), is(true));
    }

    protected void updateClusterSettings(Settings.Builder settings) {
        updateClusterSettings(settings.build());
    }

    protected void updateClusterSettings(Settings settings) {
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings));
    }

    /**
     * Checks if a field exist in a map of values. If the field contains a dot like 'foo.bar'
     * it checks that 'foo' exists in the map of values and that it points to a sub-map. Then
     * it recurses to check if 'bar' exists in the sub-map.
     */
    protected void assertContains(String field, Map<String, Object> values) {
        assertContains(field, values, null);
    }

    /**
     * Checks if a field exist in a map of values. If the field contains a dot like 'foo.bar'
     * it checks that 'foo' exists in the map of values and that it points to a sub-map. Then
     * it recurses to check if 'bar' exists in the sub-map.
     */
    protected void assertContains(String field, Map<String, Object> values, String parent) {
        assertNotNull("field name should not be null", field);
        assertNotNull("values map should not be null", values);

        int point = field.indexOf('.');
        if (point > -1) {
            assertThat(point, allOf(greaterThan(0), lessThan(field.length())));

            String segment = field.substring(0, point);
            assertTrue(Strings.hasText(segment));

            boolean fieldExists = values.containsKey(segment);
            assertTrue("expecting field [" + rebuildName(parent, segment) + "] to be present in monitoring document", fieldExists);

            Object value = values.get(segment);
            String next = field.substring(point + 1);
            if (next.length() > 0) {
                assertTrue(value instanceof Map);
                assertContains(next, (Map<String, Object>) value, rebuildName(parent, segment));
            } else {
                assertFalse(value instanceof Map);
            }
        } else {
            assertTrue("expecting field [" + rebuildName(parent, field) + "] to be present in monitoring document",
                       values.containsKey(field));
        }
    }

    private String rebuildName(String parent, String field) {
        if (Strings.isEmpty(parent)) {
            return field;
        }

        return parent + "." + field;
    }

    protected void disableMonitoringInterval() {
        updateMonitoringInterval(TimeValue.MINUS_ONE.millis(), TimeUnit.MILLISECONDS);
    }

    protected void updateMonitoringInterval(long value, TimeUnit timeUnit) {
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(
                Settings.builder().put(MonitoringSettings.INTERVAL.getKey(), value, timeUnit)));
    }

    public class MockDataIndexNameResolver extends MonitoringIndexNameResolver.Data<MonitoringDoc> {

        public MockDataIndexNameResolver(String version) {
            super(version);
        }

        @Override
        protected void buildXContent(MonitoringDoc document, XContentBuilder builder, ToXContent.Params params) throws IOException {
            throw new UnsupportedOperationException("MockDataIndexNameResolver does not support resolving building XContent");
        }
    }

    protected class MockTimestampedIndexNameResolver extends MonitoringIndexNameResolver.Timestamped<MonitoringDoc> {

        public MockTimestampedIndexNameResolver(MonitoredSystem system, Settings settings, String version) {
            super(system, settings, version);
        }

        @Override
        protected void buildXContent(MonitoringDoc document, XContentBuilder builder, ToXContent.Params params) throws IOException {
            throw new UnsupportedOperationException("MockTimestampedIndexNameResolver does not support resolving building XContent");
        }
    }

    /** security related settings */

    public static class SecuritySettings {

        public static final String TEST_USERNAME = "test";
        public static final String TEST_PASSWORD = "changeme";
        private static final String TEST_PASSWORD_HASHED =  new String(Hasher.BCRYPT.hash(new SecuredString(TEST_PASSWORD.toCharArray())));

        static boolean auditLogsEnabled = SystemPropertyUtil.getBoolean("tests.audit_logs", true);
        static byte[] systemKey = generateKey(); // must be the same for all nodes

        public static final String IP_FILTER = "allow: all\n";

        public static final String USERS =
                "transport_client:" + TEST_PASSWORD_HASHED + "\n" +
                        TEST_USERNAME + ":" + TEST_PASSWORD_HASHED + "\n" +
                        "admin:" + TEST_PASSWORD_HASHED + "\n" +
                        "monitor:" + TEST_PASSWORD_HASHED;

        public static final String USER_ROLES =
                "transport_client:transport_client\n" +
                        "test:test\n" +
                        "admin:admin\n" +
                        "monitor:monitor";

        public static final String ROLES =
                "test:\n" + // a user for the test infra.
                "  cluster: [ 'cluster:monitor/nodes/info', 'cluster:monitor/state', 'cluster:monitor/health', 'cluster:monitor/stats'," +
                " 'cluster:admin/settings/update', 'cluster:admin/repository/delete', 'cluster:monitor/nodes/liveness'," +
                " 'indices:admin/template/get', 'indices:admin/template/put', 'indices:admin/template/delete'," +
                " 'cluster:admin/ingest/pipeline/get', 'cluster:admin/ingest/pipeline/put', 'cluster:admin/ingest/pipeline/delete'," +
                " 'cluster:monitor/task', 'cluster:admin/xpack/monitoring/bulk' ]\n" +
                "  indices:\n" +
                "    - names: '*'\n" +
                "      privileges: [ all ]\n" +
                "\n" +
                "admin:\n" +
                "  cluster: [ 'cluster:monitor/nodes/info', 'cluster:monitor/nodes/liveness' ]\n" +
                "monitor:\n" +
                "  cluster: [ 'cluster:monitor/nodes/info', 'cluster:monitor/nodes/liveness' ]\n"
                ;


        public static void apply(boolean enabled, Settings.Builder builder)  {
            if (!enabled) {
                builder.put("xpack.security.enabled", false);
                return;
            }
            try {
                Path conf = createTempDir().resolve("monitoring_security");
                Path xpackConf = conf.resolve(XPackPlugin.NAME);
                Files.createDirectories(xpackConf);
                writeFile(xpackConf, "users", USERS);
                writeFile(xpackConf, "users_roles", USER_ROLES);
                writeFile(xpackConf, "roles.yml", ROLES);
                writeFile(xpackConf, "system_key", systemKey);

                builder.put("xpack.security.enabled", true)
                        .put("xpack.ml.autodetect_process", false)
                        .put("xpack.security.authc.realms.esusers.type", FileRealm.TYPE)
                        .put("xpack.security.authc.realms.esusers.order", 0)
                        .put("xpack.security.audit.enabled", auditLogsEnabled)
                        .put(NetworkModule.TRANSPORT_TYPE_KEY, Security.NAME4)
                        .put(NetworkModule.HTTP_TYPE_KEY, Security.NAME4)
                        .put(Environment.PATH_CONF_SETTING.getKey(), conf);
            } catch (IOException ex) {
                throw new RuntimeException("failed to build settings for security", ex);
            }
        }

        static byte[] generateKey() {
            try {
                return CryptoService.generateKey();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public static String writeFile(Path folder, String name, String content) throws IOException {
            Path file = folder.resolve(name);
            try (BufferedWriter stream = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
                Streams.copy(content, stream);
            } catch (IOException e) {
                throw new ElasticsearchException("error writing file in test", e);
            }
            return file.toAbsolutePath().toString();
        }

        public static String writeFile(Path folder, String name, byte[] content) throws IOException {
            Path file = folder.resolve(name);
            try (OutputStream stream = Files.newOutputStream(file)) {
                Streams.copy(content, stream);
            } catch (IOException e) {
                throw new ElasticsearchException("error writing file in test", e);
            }
            return file.toAbsolutePath().toString();
        }
    }
}
