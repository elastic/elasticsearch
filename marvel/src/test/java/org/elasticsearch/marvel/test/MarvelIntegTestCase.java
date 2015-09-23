/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.test;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.cache.IndexCacheModule;
import org.elasticsearch.license.plugin.LicensePlugin;
import org.elasticsearch.marvel.MarvelPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.shield.ShieldPlugin;
import org.elasticsearch.shield.authc.esusers.ESUsersRealm;
import org.elasticsearch.shield.authc.support.Hasher;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.crypto.InternalCryptoService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.TestCluster;
import org.jboss.netty.util.internal.SystemPropertyUtil;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;

/**
 *
 */
public class MarvelIntegTestCase extends ESIntegTestCase {

    protected static Boolean shieldEnabled;

    @Override
    protected TestCluster buildTestCluster(Scope scope, long seed) throws IOException {
        if (shieldEnabled == null) {
            shieldEnabled = false; // enableShield();
        }
        return super.buildTestCluster(scope, seed);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                // we do this by default in core, but for marvel this isn't needed and only adds noise.
                .put("index.store.mock.check_index_on_close", false)
                .put(ShieldSettings.settings(shieldEnabled))
                .build();
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
        return randomBoolean();
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
                        "  cluster: cluster:monitor/nodes/info, cluster:monitor/state, cluster:monitor/health, cluster:monitor/stats, cluster:admin/settings/update, cluster:admin/repository/delete, cluster:monitor/nodes/liveness, indices:admin/template/get, indices:admin/template/put, indices:admin/template/delete\n" +
                        "  indices:\n" +
                        "    '*': all\n" +
                        "\n" +
                        "admin:\n" +
                        "  cluster: manage_watcher, cluster:monitor/nodes/info, cluster:monitor/nodes/liveness\n" +
                        "transport_client:\n" +
                        "  cluster: cluster:monitor/nodes/info, cluster:monitor/nodes/liveness\n" +
                        "\n" +
                        "monitor:\n" +
                        "  cluster: monitor_watcher, cluster:monitor/nodes/info, cluster:monitor/nodes/liveness\n"
                ;


        public static Settings settings(boolean enabled)  {
            Settings.Builder builder = Settings.builder();
            if (!enabled) {
                return builder.put("shield.enabled", false).build();
            }
            try {
                Path folder = createTempDir().resolve("watcher_shield");
                Files.createDirectories(folder);
                return builder.put("shield.enabled", true)
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
                        .put(IndexCacheModule.QUERY_CACHE_TYPE, ShieldPlugin.OPT_OUT_QUERY_CACHE)
                        .build();
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
