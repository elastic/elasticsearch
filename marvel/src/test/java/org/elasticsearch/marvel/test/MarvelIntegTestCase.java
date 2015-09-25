/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.test;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.cache.IndexCacheModule;
import org.elasticsearch.license.plugin.LicensePlugin;
import org.elasticsearch.marvel.MarvelPlugin;
import org.elasticsearch.marvel.agent.AgentService;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.shield.ShieldPlugin;
import org.elasticsearch.shield.authc.esusers.ESUsersRealm;
import org.elasticsearch.shield.authc.support.Hasher;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.crypto.InternalCryptoService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.TestCluster;
import org.hamcrest.Matcher;
import org.jboss.netty.util.internal.SystemPropertyUtil;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.marvel.agent.exporter.Exporter.INDEX_TEMPLATE_NAME;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public abstract class MarvelIntegTestCase extends ESIntegTestCase {

    protected Boolean shieldEnabled = enableShield();

    @Override
    protected TestCluster buildTestCluster(Scope scope, long seed) throws IOException {
        logger.info("--> shield {}", shieldEnabled ? "enabled" : "disabled");
        return super.buildTestCluster(scope, seed);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                // we do this by default in core, but for marvel this isn't needed and only adds noise.
                .put("index.store.mock.check_index_on_close", false);

        if (shieldEnabled) {
            ShieldSettings.apply(builder);
        }
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
        return super.transportClientSettings();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        if (shieldEnabled) {
            return Arrays.asList(LicensePlugin.class, MarvelPlugin.class, ShieldPlugin.class);
        }
        return Arrays.asList(LicensePlugin.class, MarvelPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
    }

    /**
     * Override and returns {@code false} to force running without shield
     */
    protected boolean enableShield() {
        return true; //randomBoolean();
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

    protected void deleteMarvelIndices() {
        if (shieldEnabled) {
            try {
                assertAcked(client().admin().indices().prepareDelete(MarvelSettings.MARVEL_INDICES_PREFIX + "*"));
            } catch (Exception e) {
                // if shield couldn't resolve any marvel index, it'll throw index not found exception.
                if (!(e instanceof IndexNotFoundException)) {
                    throw e;
                }
            }
        } else {
            assertAcked(client().admin().indices().prepareDelete(MarvelSettings.MARVEL_INDICES_PREFIX + "*"));
        }
    }

    protected void awaitMarvelDocsCount(Matcher<Long> matcher, String... types) throws Exception {
        securedFlush();
        securedRefresh();
        assertBusy(new Runnable() {
            @Override
            public void run() {
                assertMarvelDocsCount(matcher, types);
            }
        }, 30, TimeUnit.SECONDS);
    }

    protected void assertMarvelDocsCount(Matcher<Long> matcher, String... types) {
        try {
            long count = client().prepareCount(MarvelSettings.MARVEL_INDICES_PREFIX + "*")
                    .setTypes(types).get().getCount();
            assertThat(count, matcher);
        } catch (IndexNotFoundException e) {
            if (shieldEnabled) {
                assertThat(0L, matcher);
            } else {
                throw e;
            }
        }
    }

    protected void assertMarvelTemplateInstalled() {
        for (IndexTemplateMetaData template : client().admin().indices().prepareGetTemplates(INDEX_TEMPLATE_NAME).get().getIndexTemplates()) {
            if (template.getName().equals(INDEX_TEMPLATE_NAME)) {
                return;
            }
        }
        fail("marvel template should exists");
    }

    protected void assertMarvelTemplateMissing() {
        for (IndexTemplateMetaData template : client().admin().indices().prepareGetTemplates(INDEX_TEMPLATE_NAME).get().getIndexTemplates()) {
            if (template.getName().equals(INDEX_TEMPLATE_NAME)) {
                fail("marvel template shouldn't exists");
            }
        }
    }

    protected void awaitIndexExists(final String... indices) throws Exception {
        assertBusy(new Runnable() {
            @Override
            public void run() {
                assertIndicesExists(indices);
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
            } catch (Exception e) {
                if (!(e instanceof IndexNotFoundException)) {
                    throw e;
                }
            }
        } else {
            refresh();
        }
    }

    protected void securedFlush(String... indices) {
        if (shieldEnabled) {
            try {
                flush(indices);
            } catch (Exception e) {
                if (!(e instanceof IndexNotFoundException)) {
                    throw e;
                }
            }
        } else {
            flush(indices);
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
            assertTrue("expecting field [" + segment + "] to be present in marvel document", fieldExists);

            Object value = values.get(segment);
            String next = field.substring(point + 1);
            if (next.length() > 0) {
                assertTrue(value instanceof Map);
                assertContains(next, (Map<String, Object>) value);
            } else {
                assertFalse(value instanceof Map);
            }
        } else {
            assertTrue("expecting field [" + field + "] to be present in marvel document", values.containsKey(field));
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
                "  cluster: cluster:monitor/nodes/info, cluster:monitor/nodes/stats, cluster:monitor/state, cluster:monitor/health, cluster:monitor/stats, cluster:admin/settings/update, cluster:admin/repository/delete, cluster:monitor/nodes/liveness, indices:admin/template/get, indices:admin/template/put, indices:admin/template/delete\n" +
                "  indices:\n" +
                "    '*': all\n" +
                "\n" +
                "admin:\n" +
                "  cluster: cluster:monitor/nodes/info, cluster:monitor/nodes/liveness\n" +
                "transport_client:\n" +
                "  cluster: cluster:monitor/nodes/info, cluster:monitor/nodes/liveness\n" +
                "\n" +
                "monitor:\n" +
                "  cluster: cluster:monitor/nodes/info, cluster:monitor/nodes/liveness\n"
                ;


        public static void apply(Settings.Builder builder)  {
            try {
                Path folder = createTempDir().resolve("marvel_shield");
                Files.createDirectories(folder);

                builder.remove("index.queries.cache.type");

                builder.put("shield.enabled", true)
                        .put("shield.user", "test:changeme")
                        .put("shield.authc.realms.esusers.type", ESUsersRealm.TYPE)
                        .put("shield.authc.realms.esusers.order", 0)
                        .put("shield.authc.realms.esusers.files.users", writeFile(folder, "users", USERS))
                        .put("shield.authc.realms.esusers.files.users_roles", writeFile(folder, "users_roles", USER_ROLES))
                        .put("shield.authz.store.files.roles", writeFile(folder, "roles.yml", ROLES))
                        .put("shield.transport.n2n.ip_filter.file", writeFile(folder, "ip_filter.yml", IP_FILTER))
                        .put("shield.system_key.file", writeFile(folder, "system_key.yml", systemKey))
                        .put("shield.authc.sign_user_header", false)
                        .put("shield.audit.enabled", auditLogsEnabled)
                                // Test framework sometimes randomily selects the 'index' or 'none' cache and that makes the
                                // validation in ShieldPlugin fail. Shield can only run with this query cache impl
                        .put(IndexCacheModule.QUERY_CACHE_TYPE, ShieldPlugin.OPT_OUT_QUERY_CACHE);
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
