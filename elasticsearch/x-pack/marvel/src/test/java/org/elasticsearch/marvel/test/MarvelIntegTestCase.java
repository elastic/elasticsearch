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
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.marvel.MarvelSettings;
import org.elasticsearch.marvel.MonitoredSystem;
import org.elasticsearch.marvel.agent.AgentService;
import org.elasticsearch.marvel.agent.exporter.MarvelTemplateUtils;
import org.elasticsearch.marvel.agent.exporter.MonitoringDoc;
import org.elasticsearch.marvel.agent.resolver.MonitoringIndexNameResolver;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.shield.Shield;
import org.elasticsearch.shield.authc.esusers.ESUsersRealm;
import org.elasticsearch.shield.authc.support.Hasher;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.crypto.InternalCryptoService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.TestCluster;
import org.elasticsearch.test.store.MockFSIndexStore;
import org.elasticsearch.test.transport.AssertingLocalTransport;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.watcher.Watcher;
import org.elasticsearch.xpack.XPackPlugin;
import org.hamcrest.Matcher;
import org.jboss.netty.util.internal.SystemPropertyUtil;

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

    protected static Boolean shieldEnabled;

    @Override
    protected TestCluster buildTestCluster(Scope scope, long seed) throws IOException {
        if (shieldEnabled == null) {
            shieldEnabled = enableShield();
        }
        logger.info("--> shield {}", shieldEnabled ? "enabled" : "disabled");
        return super.buildTestCluster(scope, seed);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))

                //TODO: for now lets isolate monitoring tests from watcher (randomize this later)
                .put(XPackPlugin.featureEnabledSetting(Watcher.NAME), false)
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
                    .put("shield.user", "test:changeme")
                    .build();
        }
        return Settings.builder().put(super.transportClientSettings())
                .put("shield.enabled", false)
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

    @Override
    protected Set<String> excludeTemplates() {
        Set<String> templates = new HashSet<>();
        templates.add(MarvelTemplateUtils.indexTemplateName());
        templates.add(MarvelTemplateUtils.dataTemplateName());
        return templates;
    }

    /**
     * Override and returns {@code false} to force running without shield
     */
    protected boolean enableShield() {
        return randomBoolean();
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
        assertNotNull("field name should not be null", field);
        assertNotNull("values map should not be null", values);

        int point = field.indexOf('.');
        if (point > -1) {
            assertThat(point, allOf(greaterThan(0), lessThan(field.length())));

            String segment = field.substring(0, point);
            assertTrue(Strings.hasText(segment));

            boolean fieldExists = values.containsKey(segment);
            assertTrue("expecting field [" + segment + "] to be present in monitoring document", fieldExists);

            Object value = values.get(segment);
            String next = field.substring(point + 1);
            if (next.length() > 0) {
                assertTrue(value instanceof Map);
                assertContains(next, (Map<String, Object>) value);
            } else {
                assertFalse(value instanceof Map);
            }
        } else {
            assertTrue("expecting field [" + field + "] to be present in monitoring document", values.containsKey(field));
        }
    }

    protected void updateMarvelInterval(long value, TimeUnit timeUnit) {
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(
                Settings.builder().put(MarvelSettings.INTERVAL.getKey(), value, timeUnit)));
    }

    protected class MockDataIndexNameResolver extends MonitoringIndexNameResolver.Data<MonitoringDoc> {

        public MockDataIndexNameResolver(int version) {
            super(version);
        }

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

        public MockTimestampedIndexNameResolver(MonitoredSystem id, int version, Settings settings) {
            super(id, version, settings);
        }

        public MockTimestampedIndexNameResolver(MonitoredSystem id, int version) {
            this(id, version, Settings.EMPTY);
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
                    " 'cluster:monitor/task']\n" +
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
                builder.put("shield.enabled", false);
                return;
            }
            try {
                Path folder = createTempDir().resolve("marvel_shield");
                Files.createDirectories(folder);

                builder.remove("index.queries.cache.type");

                builder.put("shield.enabled", true)
                        .put("shield.authc.realms.esusers.type", ESUsersRealm.TYPE)
                        .put("shield.authc.realms.esusers.order", 0)
                        .put("shield.authc.realms.esusers.files.users", writeFile(folder, "users", USERS))
                        .put("shield.authc.realms.esusers.files.users_roles", writeFile(folder, "users_roles", USER_ROLES))
                        .put("shield.authz.store.files.roles", writeFile(folder, "roles.yml", ROLES))
                        .put("shield.system_key.file", writeFile(folder, "system_key.yml", systemKey))
                        .put("shield.authc.sign_user_header", false)
                        .put("shield.audit.enabled", auditLogsEnabled)
                                // Test framework sometimes randomily selects the 'index' or 'none' cache and that makes the
                                // validation in ShieldPlugin fail. Shield can only run with this query cache impl
                        .put(IndexModule.INDEX_QUERY_CACHE_TYPE_SETTING.getKey(), Shield.OPT_OUT_QUERY_CACHE);
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
