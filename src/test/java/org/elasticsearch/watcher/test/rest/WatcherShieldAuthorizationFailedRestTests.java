/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.test.rest;

import com.carrotsearch.randomizedtesting.annotations.Name;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.support.Headers;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.plugin.LicensePlugin;
import org.elasticsearch.node.Node;
import org.elasticsearch.shield.ShieldPlugin;
import org.elasticsearch.shield.authc.esusers.ESUsersRealm;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.test.rest.RestTestCandidate;
import org.elasticsearch.watcher.WatcherPlugin;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTests;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsString;

/**
 */
public class WatcherShieldAuthorizationFailedRestTests extends WatcherRestTests {

    // Always run with Shield enabled:
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("plugin.types", WatcherPlugin.class.getName() + "," +
                                ShieldPlugin.class.getName() + "," +
                                LicensePlugin.class.getName())
                .put(ShieldSettings.settings(true));
        return builder.build();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("admin", new SecuredString("changeme".toCharArray()));
        return Settings.builder()
                .put(Headers.PREFIX + ".Authorization", token)
                .build();
    }

    @Override
    protected Settings transportClientSettings() {
        return Settings.builder()
                .put(super.transportClientSettings())
                .put("client.transport.sniff", false)
                .put("plugin.types", WatcherPlugin.class.getName() + "," + ShieldPlugin.class.getName() + ",")
                .put("shield.user", "admin:changeme")
                .put(Node.HTTP_ENABLED, true)
                .build();
    }

    public WatcherShieldAuthorizationFailedRestTests(@Name("yaml") RestTestCandidate testCandidate) {
        super(testCandidate);
    }

    @Test
    public void test() throws IOException {
        try {
            super.test();
            fail();
        } catch(AssertionError ae) {
            if (ae.getMessage() == null || ae.getMessage().contains("not supported")){
                //This was a test testing the "hijacked" methods
                return;
            }
            assertThat(ae.getMessage(), containsString("returned [403 Forbidden]"));
            assertThat(ae.getMessage(), containsString("is unauthorized for user [admin]"));
        }
    }

    public static class ShieldSettings {

        public static final String IP_FILTER = "allow: all\n";

        public static final String USERS = "test:{plain}changeme\n" +
                "admin:{plain}changeme\n" +
                "monitor:{plain}changeme";

        public static final String USER_ROLES = "test:test\n" +
                "admin:admin\n" +
                "monitor:monitor";

        public static final String ROLES =
                "test:\n" + // a user for the test infra.
                        "  cluster: all, cluster:monitor/state, cluster:monitor/health, indices:admin/template/delete, cluster:admin/repository/delete, indices:admin/template/put\n" +
                        "  indices:\n" +
                        "    '*': all\n" +
                        "\n" +
                        "admin:\n" +
                        "  cluster: cluster:monitor/nodes/info, cluster:monitor/state, cluster:monitor/health, cluster:admin/repository/delete\n" +
                        "  indices:\n" +
                        "    '*': all, indices:admin/template/delete\n" +
                        "\n";

        public static Settings settings(boolean enabled) {
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
                    .put("shield.authc.realms.esusers.files.users", AbstractWatcherIntegrationTests.ShieldSettings.writeFile(folder, "users", USERS))
                    .put("shield.authc.realms.esusers.files.users_roles", AbstractWatcherIntegrationTests.ShieldSettings.writeFile(folder, "users_roles", USER_ROLES))
                    .put("shield.authz.store.files.roles", AbstractWatcherIntegrationTests.ShieldSettings.writeFile(folder, "roles.yml", ROLES))
                    .put("shield.transport.n2n.ip_filter.file", AbstractWatcherIntegrationTests.ShieldSettings.writeFile(folder, "ip_filter.yml", IP_FILTER))
                    .put("shield.audit.enabled", true)
                    .build();
            } catch (IOException ex) {
                throw new RuntimeException("failed to build settings for shield", ex);
            }
        }
    }
}
