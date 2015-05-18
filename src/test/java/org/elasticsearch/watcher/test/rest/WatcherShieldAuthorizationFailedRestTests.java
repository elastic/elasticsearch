/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.test.rest;

import com.carrotsearch.randomizedtesting.annotations.Name;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.support.Headers;
import org.elasticsearch.common.base.Charsets;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.plugin.LicensePlugin;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.shield.ShieldPlugin;
import org.elasticsearch.shield.authc.esusers.ESUsersRealm;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.test.rest.RestTestCandidate;
import org.elasticsearch.watcher.WatcherPlugin;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsString;

/**
 */
public class WatcherShieldAuthorizationFailedRestTests extends WatcherRestTests {

    // Always run with Shield enabled:
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        ImmutableSettings.Builder builder = ImmutableSettings.builder()
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
        return ImmutableSettings.builder()
                .put(Headers.PREFIX + ".Authorization", token)
                .build();
    }

    @Override
    protected Settings transportClientSettings() {
        return ImmutableSettings.builder()
                .put(super.transportClientSettings())
                .put("client.transport.sniff", false)
                .put("plugin.types", WatcherPlugin.class.getName() + "," + ShieldPlugin.class.getName() + ",")
                .put("shield.user", "admin:changeme")
                .put(InternalNode.HTTP_ENABLED, true)
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
            ImmutableSettings.Builder builder = ImmutableSettings.builder();
            if (!enabled) {
                return builder.put("shield.enabled", false).build();
            }

            File folder = createFolder(globalTempDir(), "watcher_shield");
            return builder.put("shield.enabled", true)
                    .put("shield.user", "test:changeme")
                    .put("shield.authc.realms.esusers.type", ESUsersRealm.TYPE)
                    .put("shield.authc.realms.esusers.order", 0)
                    .put("shield.authc.realms.esusers.files.users", writeFile(folder, "users", USERS))
                    .put("shield.authc.realms.esusers.files.users_roles", writeFile(folder, "users_roles", USER_ROLES))
                    .put("shield.authz.store.files.roles", writeFile(folder, "roles.yml", ROLES))
                    .put("shield.transport.n2n.ip_filter.file", writeFile(folder, "ip_filter.yml", IP_FILTER))
                    .put("shield.audit.enabled", true)
                    .build();
        }

        static File createFolder(File parent, String name) {
            File createdFolder = new File(parent, name);
            //the directory might exist e.g. if the global cluster gets restarted, then we recreate the directory as well
            if (createdFolder.exists()) {
                if (!FileSystemUtils.deleteRecursively(createdFolder)) {
                    throw new RuntimeException("could not delete existing temporary folder: " + createdFolder.getAbsolutePath());
                }
            }
            if (!createdFolder.mkdir()) {
                throw new RuntimeException("could not create temporary folder: " + createdFolder.getAbsolutePath());
            }
            return createdFolder;
        }

        static String writeFile(File folder, String name, String content) {
            return writeFile(folder, name, content.getBytes(Charsets.UTF_8));
        }

        static String writeFile(File folder, String name, byte[] content) {
            Path file = folder.toPath().resolve(name);
            try {
                Streams.copy(content, file.toFile());
            } catch (IOException e) {
                throw new ElasticsearchException("error writing file in test", e);
            }
            return file.toFile().getAbsolutePath();
        }
    }
}
