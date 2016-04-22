/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.test;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.marvel.MonitoringSettings;
import org.elasticsearch.marvel.MonitoredSystem;
import org.elasticsearch.marvel.agent.AgentService;
import org.elasticsearch.marvel.agent.exporter.MonitoringDoc;
import org.elasticsearch.marvel.agent.resolver.MonitoringIndexNameResolver;
import org.elasticsearch.marvel.agent.resolver.ResolversRegistry;
import org.elasticsearch.marvel.client.MonitoringClient;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.shield.Security;
import org.elasticsearch.shield.authc.file.FileRealm;
import org.elasticsearch.shield.authc.support.Hasher;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authz.store.FileRolesStore;
import org.elasticsearch.shield.crypto.InternalCryptoService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.TestCluster;
import org.elasticsearch.test.store.MockFSIndexStore;
import org.elasticsearch.test.transport.AssertingLocalTransport;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.watcher.Watcher;
import org.elasticsearch.xpack.XPackClient;
import org.elasticsearch.xpack.XPackPlugin;
import org.hamcrest.Matcher;
import org.jboss.netty.util.internal.SystemPropertyUtil;
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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

/**
 *
 */
public abstract class MarvelIntegTestCase extends ESIntegTestCase {

    public static final String MONITORING_INDICES_PREFIX = MonitoringIndexNameResolver.PREFIX + MonitoringIndexNameResolver.DELIMITER;

    /**
     * Enables individual tests to control the behavior.
     * <p>
     * Control this by overriding {@link #enableShield()}, which defaults to enabling it randomly.
     */
    // SCARY: This needs to be static or lots of tests randomly fail, but it's not used statically!
    protected static Boolean shieldEnabled;
    /**
     * Enables individual tests to control the behavior.
     * <p>
     * Control this by overriding {@link #enableWatcher()}, which defaults to disabling it (this will change!).
     */
    protected Boolean watcherEnabled;

    @Override
    protected TestCluster buildTestCluster(Scope scope, long seed) throws IOException {
        if (shieldEnabled == null) {
            shieldEnabled = enableShield();
        }
        if (watcherEnabled == null) {
            watcherEnabled = enableWatcher();
        }

        logger.debug("--> shield {}", shieldEnabled ? "enabled" : "disabled");
        logger.debug("--> watcher {}", watcherEnabled ? "enabled" : "disabled");

        return super.buildTestCluster(scope, seed);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(XPackPlugin.featureEnabledSetting(Watcher.NAME), watcherEnabled)
                // we do this by default in core, but for monitoring this isn't needed and only adds noise.
                .put("index.store.mock.check_index_on_close", false);

        ShieldSettings.apply(shieldEnabled, builder);

        return builder.build();
    }

    @Override
    protected Settings transportClientSettings() {
        if (shieldEnabled) {
            return Settings.builder()
                    .put(super.transportClientSettings())
                    .put("client.transport.sniff", false)
                    .put(Security.USER_SETTING.getKey(), "test:changeme")
                    .build();
        }
        return Settings.builder().put(super.transportClientSettings())
                .put("xpack.security.enabled", false)
                .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        Set<Class<? extends Plugin>> plugins = new HashSet<>(super.getMockPlugins());
        plugins.remove(MockTransportService.TestPlugin.class); // shield has its own transport service
        plugins.remove(AssertingLocalTransport.TestPlugin.class); // shield has its own transport
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
        if (shieldEnabled == false) {
            return Function.identity();
        }
        Map<String, String> headers = Collections.singletonMap("Authorization",
                basicAuthHeaderValue(ShieldSettings.TEST_USERNAME, new SecuredString(ShieldSettings.TEST_PASSWORD.toCharArray())));
        return client -> (client instanceof NodeClient) ? client.filterWithHeader(headers) : client;
    }

    protected MonitoringClient monitoringClient() {
        Client client = shieldEnabled ? internalCluster().transportClient() : client();
        return randomBoolean() ? new XPackClient(client).monitoring() : new MonitoringClient(client);
    }

    @Override
    protected Set<String> excludeTemplates() {
        return monitoringTemplates().keySet();
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        startCollection();
    }

    @After
    public void tearDown() throws Exception {
        stopCollection();
        super.tearDown();
    }

    /**
     * Override and return {@code false} to force running without Security.
     */
    protected boolean enableShield() {
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

    protected void stopCollection() {
        for (AgentService agent : internalCluster().getInstances(AgentService.class)) {
            agent.stopCollection();
        }
    }

    protected void startCollection() {
        for (AgentService agent : internalCluster().getInstances(AgentService.class)) {
            agent.startCollection();
        }
    }

    protected void wipeMarvelIndices() throws Exception {
        CountDown retries = new CountDown(3);
        assertBusy(new Runnable() {
            @Override
            public void run() {
                try {
                    boolean exist = client().admin().indices().prepareExists(MONITORING_INDICES_PREFIX + "*")
                            .get().isExists();
                    if (exist) {
                        deleteMarvelIndices();
                    } else {
                        retries.countDown();
                    }
                } catch (IndexNotFoundException e) {
                    retries.countDown();
                }
                assertThat(retries.isCountedDown(), is(true));
            }
        });
    }

    protected void deleteMarvelIndices() {
        if (shieldEnabled) {
            try {
                assertAcked(client().admin().indices().prepareDelete(MONITORING_INDICES_PREFIX + "*"));
            } catch (IndexNotFoundException e) {
                // if shield couldn't resolve any marvel index, it'll throw index not found exception.
            }
        } else {
            assertAcked(client().admin().indices().prepareDelete(MONITORING_INDICES_PREFIX + "*"));
        }
    }

    protected void awaitMarvelDocsCount(Matcher<Long> matcher, String... types) throws Exception {
        assertBusy(() -> assertMarvelDocsCount(matcher, types), 30, TimeUnit.SECONDS);
    }

    protected void ensureMarvelIndicesYellow() {
        if (shieldEnabled) {
            try {
                ensureYellow(".monitoring-es-*");
            } catch (IndexNotFoundException e) {
                // might happen with shield...
            }
        } else {
            ensureYellow(".monitoring-es-*");
        }
    }

    protected void assertMarvelDocsCount(Matcher<Long> matcher, String... types) {
        try {
            securedFlushAndRefresh(MONITORING_INDICES_PREFIX + "*");
            long count = client().prepareSearch(MONITORING_INDICES_PREFIX + "*")
                    .setSize(0).setTypes(types).get().getHits().totalHits();
            logger.trace("--> searched for [{}] documents, found [{}]", Strings.arrayToCommaDelimitedString(types), count);
            assertThat(count, matcher);
        } catch (IndexNotFoundException e) {
            if (shieldEnabled) {
                assertThat(0L, matcher);
            } else {
                throw e;
            }
        }
    }

    protected Map<String, String> monitoringTemplates() {
        return StreamSupport.stream(new ResolversRegistry(Settings.EMPTY).spliterator(), false)
                .collect(Collectors.toMap(MonitoringIndexNameResolver::templateName, MonitoringIndexNameResolver::template, (a, b) -> a));
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

    protected void waitForMarvelTemplate(String name) throws Exception {
        assertBusy(new Runnable() {
            @Override
            public void run() {
                assertTemplateInstalled(name);
            }
        }, 30, TimeUnit.SECONDS);
    }

    protected void waitForMarvelTemplates() throws Exception {
        assertBusy(() -> monitoringTemplates().keySet().forEach(this::assertTemplateInstalled), 30, TimeUnit.SECONDS);
    }

    protected void waitForMarvelIndices() throws Exception {
        awaitIndexExists(MONITORING_INDICES_PREFIX + "*");
        assertBusy(this::ensureMarvelIndicesYellow);
    }

    protected void awaitIndexExists(final String index) throws Exception {
        assertBusy(() -> {
                try {
                    assertIndicesExists(index);
                } catch (IndexNotFoundException e) {
                    if (shieldEnabled) {
                        // with shield we might get that if wildcards were resolved to no indices
                        fail("IndexNotFoundException when checking for existence of index [" + index + "]");
                    } else {
                        throw e;
                    }
                }
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

    protected void securedRefresh() {
        if (shieldEnabled) {
            try {
                refresh();
            } catch (IndexNotFoundException e) {
                // with shield we might get that if wildcards were resolved to no indices
            }
        } else {
            refresh();
        }
    }

    protected void securedFlush(String... indices) {
        if (shieldEnabled) {
            try {
                flush(indices);
            } catch (IndexNotFoundException e) {
                // with shield we might get that if wildcards were resolved to no indices
            }
        } else {
            flush(indices);
        }
    }

    protected void securedFlushAndRefresh(String... indices) {
        if (shieldEnabled) {
            try {
                flushAndRefresh(indices);
            } catch (IndexNotFoundException e) {
                // with shield we might get that if wildcards were resolved to no indices
            }
        } else {
            flushAndRefresh(indices);
        }
    }

    protected void securedEnsureGreen(String... indices) {
        if (shieldEnabled) {
            try {
                ensureGreen(indices);
            } catch (IndexNotFoundException e) {
                // with shield we might get that if wildcards were resolved to no indices
            }
        } else {
            ensureGreen(indices);
        }
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

    protected void updateMarvelInterval(long value, TimeUnit timeUnit) {
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(
                Settings.builder().put(MonitoringSettings.INTERVAL.getKey(), value, timeUnit)));
    }

    public class MockDataIndexNameResolver extends MonitoringIndexNameResolver.Data<MonitoringDoc> {

        @Override
        public String type(MonitoringDoc document) {
            throw new UnsupportedOperationException("MockDataIndexNameResolver does not support resolving type");
        }

        @Override
        public String id(MonitoringDoc document) {
            throw new UnsupportedOperationException("MockDataIndexNameResolver does not support resolving id");
        }

        @Override
        protected void buildXContent(MonitoringDoc document, XContentBuilder builder, ToXContent.Params params) throws IOException {
            throw new UnsupportedOperationException("MockDataIndexNameResolver does not support resolving building XContent");
        }
    }

    protected class MockTimestampedIndexNameResolver extends MonitoringIndexNameResolver.Timestamped<MonitoringDoc> {

        public MockTimestampedIndexNameResolver(MonitoredSystem id, Settings settings) {
            super(id, settings);
        }

        public MockTimestampedIndexNameResolver(MonitoredSystem id) {
            this(id, Settings.EMPTY);
        }

        @Override
        public String type(MonitoringDoc document) {
            throw new UnsupportedOperationException("MockTimestampedIndexNameResolver does not support resolving type");
        }

        @Override
        protected void buildXContent(MonitoringDoc document, XContentBuilder builder, ToXContent.Params params) throws IOException {
            throw new UnsupportedOperationException("MockTimestampedIndexNameResolver does not support resolving building XContent");
        }
    }

    /** Shield related settings */

    public static class ShieldSettings {

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
                " 'cluster:monitor/task', 'cluster:admin/xpack/monitoring/bulk' ]\n" +
                "  indices:\n" +
                "    - names: '*'\n" +
                "      privileges: [ all ]\n" +
                "\n" +
                "admin:\n" +
                "  cluster: [ 'cluster:monitor/nodes/info', 'cluster:monitor/nodes/liveness' ]\n" +
                "transport_client:\n" +
                "  cluster: [ 'cluster:monitor/nodes/info', 'cluster:monitor/nodes/liveness' ]\n" +
                "\n" +
                "monitor:\n" +
                "  cluster: [ 'cluster:monitor/nodes/info', 'cluster:monitor/nodes/liveness' ]\n"
                ;


        public static void apply(boolean enabled, Settings.Builder builder)  {
            if (!enabled) {
                builder.put("xpack.security.enabled", false);
                return;
            }
            try {
                Path folder = createTempDir().resolve("marvel_shield");
                Files.createDirectories(folder);

                builder.put("xpack.security.enabled", true)
                        .put("xpack.security.authc.realms.esusers.type", FileRealm.TYPE)
                        .put("xpack.security.authc.realms.esusers.order", 0)
                        .put("xpack.security.authc.realms.esusers.files.users", writeFile(folder, "users", USERS))
                        .put("xpack.security.authc.realms.esusers.files.users_roles", writeFile(folder, "users_roles", USER_ROLES))
                        .put(FileRolesStore.ROLES_FILE_SETTING.getKey(), writeFile(folder, "roles.yml", ROLES))
                        .put(InternalCryptoService.FILE_SETTING.getKey(), writeFile(folder, "system_key.yml", systemKey))
                        .put("xpack.security.authc.sign_user_header", false)
                        .put("xpack.security.audit.enabled", auditLogsEnabled);
            } catch (IOException ex) {
                throw new RuntimeException("failed to build settings for shield", ex);
            }
        }

        static byte[] generateKey() {
            try {
                return InternalCryptoService.generateKey();
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
