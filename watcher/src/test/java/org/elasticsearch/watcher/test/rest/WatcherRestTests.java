/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.test.rest;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.elasticsearch.client.support.Headers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.plugin.LicensePlugin;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.shield.ShieldPlugin;
import org.elasticsearch.shield.authc.esusers.ESUsersRealm;
import org.elasticsearch.shield.authc.support.Hasher;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.rest.ElasticsearchRestTestCase;
import org.elasticsearch.test.rest.RestTestCandidate;
import org.elasticsearch.test.rest.parser.RestTestParseException;
import org.elasticsearch.watcher.WatcherPlugin;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTests;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope.SUITE;


@ElasticsearchRestTestCase.Rest
@ClusterScope(scope = SUITE, numClientNodes = 1, transportClientRatio = 0, numDataNodes = 1, randomDynamicTemplates = false)
@TestLogging("_root:DEBUG")
public class WatcherRestTests extends ElasticsearchRestTestCase {

    final boolean shieldEnabled = enableShield();

    public WatcherRestTests(@Name("yaml") RestTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws IOException, RestTestParseException {
        return ElasticsearchRestTestCase.createParameters(0, 1);
    }

    protected boolean enableShield() {
        return randomBoolean();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("scroll.size", randomIntBetween(1, 100))
                .put("plugin.types", WatcherPlugin.class.getName() + ","
                        + (shieldEnabled ? ShieldPlugin.class.getName() + "," : "")
                        + "," + licensePluginClass().getName())
                .put(Node.HTTP_ENABLED, true)
                .put(ShieldSettings.settings(shieldEnabled))
        .build();
    }

    protected Class<? extends Plugin> licensePluginClass() {
        return LicensePlugin.class;
    }


    /**
     * Used to obtain settings for the REST client that is used to send REST requests.
     */
    @Override
    protected Settings restClientSettings() {
        if (shieldEnabled) {
            String token = basicAuthHeaderValue("admin", new SecuredString("changeme".toCharArray()));
            return Settings.builder()
                    .put(Headers.PREFIX + ".Authorization", token)
                    .build();
        } else {
            return Settings.EMPTY;
        }
    }

    @Override
    protected Settings transportClientSettings() {
        if (shieldEnabled) {
            return Settings.builder()
                    .put(super.transportClientSettings())
                    .put("client.transport.sniff", false)
                    .put("plugin.types", WatcherPlugin.class.getName() + ","
                            + (shieldEnabled ? ShieldPlugin.class.getName() + "," : ""))
                    .put("shield.user", "admin:changeme")
                    .put(Node.HTTP_ENABLED, true)
                    .build();
        }

        return Settings.builder()
                .put("plugin.types", WatcherPlugin.class.getName())
                .put(Node.HTTP_ENABLED, true)
                .put("plugin.types", WatcherPlugin.class.getName() + ","
                        + "," + LicensePlugin.class.getName())
                .build();
    }


    /** Shield related settings */

    public static class ShieldSettings {

        public static final String IP_FILTER = "allow: all\n";

        public static final String TEST_USERNAME = "test";
        public static final String TEST_PASSWORD = "changeme";
        private static final String TEST_PASSWORD_HASHED =  new String(Hasher.BCRYPT.hash(new SecuredString(TEST_PASSWORD.toCharArray())));

        public static final String USERS =
                "transport_client:" + TEST_PASSWORD_HASHED + "\n" +
                        TEST_USERNAME + ":" + TEST_PASSWORD_HASHED + "\n" +
                        "admin:" + TEST_PASSWORD_HASHED + "\n" +
                        "monitor:" + TEST_PASSWORD_HASHED;

        public static final String USER_ROLES = "test:test\n" +
                "admin:admin\n" +
                "monitor:monitor";

        public static final String ROLES =
                "test:\n" + // a user for the test infra.
                "  cluster: cluster:monitor/state, cluster:monitor/health, indices:admin/template/delete, cluster:admin/repository/delete, cluster:monitor/nodes/liveness, indices:admin/template/put, cluster:admin/delete\n" +
                "  indices:\n" +
                "    '*': all\n" +
                "\n" +
                "admin:\n" +
                "  cluster: manage_watcher, cluster:monitor/nodes/info, cluster:monitor/state, cluster:monitor/nodes/liveness, cluster:monitor/health, cluster:admin/repository/delete, cluster:admin/repository/put, indices:admin/template/put, indices:admin/template/get, cluster:monitor/nodes/stats, cluster:admin/reroute, cluster:admin/analyze, cluster:monitor/task, cluster:admin/repository/verify, cluster:admin/repository/get\n" +
                "  indices:\n" +
                "    '*': all, indices:admin/template/delete\n" +
                "\n" +
                "monitor:\n" +
                "  cluster: monitor_watcher, cluster:monitor/nodes/info, cluster:monitor/nodes/liveness\n" +
                "\n"
                ;

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
