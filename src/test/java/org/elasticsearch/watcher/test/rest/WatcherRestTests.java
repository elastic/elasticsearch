/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.test.rest;

import com.carrotsearch.randomizedtesting.annotations.Name;
import org.apache.lucene.util.AbstractRandomizedTest;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.base.Charsets;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.plugin.LicensePlugin;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.shield.ShieldPlugin;
import org.elasticsearch.shield.authc.esusers.ESUsersRealm;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.rest.ElasticsearchRestTests;
import org.elasticsearch.test.rest.RestTestCandidate;
import org.elasticsearch.watcher.WatcherPlugin;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;


@AbstractRandomizedTest.Rest
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE, numClientNodes = 1, transportClientRatio = 0, numDataNodes = 1, randomDynamicTemplates = false)
@TestLogging("_root:DEBUG")
public class WatcherRestTests extends ElasticsearchRestTests {

    final boolean shieldEnabled = randomBoolean();

    public WatcherRestTests(@Name("yaml") RestTestCandidate testCandidate) {
        super(testCandidate);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("scroll.size", randomIntBetween(1, 100))
                .put("plugin.types", WatcherPlugin.class.getName() + ","
                        + (shieldEnabled ? ShieldPlugin.class.getName() + "," : "")
                        + "," + LicensePlugin.class.getName())
                .put(InternalNode.HTTP_ENABLED, true)
                .put("shield.user", "admin:changeme")
                .put(ShieldSettings.settings(shieldEnabled))
        .build();
    }

    /**
     * Used to obtain settings for the REST client that is used to send REST requests.
     */
    @Override
    protected Settings restClientSettings() {
        if (shieldEnabled) {
            return ImmutableSettings.builder()
                    .put("client.transport.sniff", false)
                    .put("plugin.types", WatcherPlugin.class.getName() + ","
                            + (shieldEnabled ? ShieldPlugin.class.getName() + "," : "")
                            + "," + LicensePlugin.class.getName())
                    .put(InternalNode.HTTP_ENABLED, true)
                    .put("shield.user", "admin:changeme")
                    .put(ShieldSettings.settings(shieldEnabled))
                    .build();
        }

        return ImmutableSettings.builder()
                .put("plugin.types", WatcherPlugin.class.getName())
                .put(InternalNode.HTTP_ENABLED, true)
                .put("plugin.types", WatcherPlugin.class.getName() + ","
                        + "," + LicensePlugin.class.getName())
                .build();
    }

    @Override
    protected Settings transportClientSettings() {
        if (shieldEnabled) {
            return ImmutableSettings.builder()
                    .put(super.transportClientSettings())
                    .put("client.transport.sniff", false)
                    .put("plugin.types", WatcherPlugin.class.getName() + ","
                            + (shieldEnabled ? ShieldPlugin.class.getName() + "," : ""))
                    .put(ShieldSettings.settings(shieldEnabled))
                    .put("shield.user", "admin:changeme")
                    .put(InternalNode.HTTP_ENABLED, true)
                    .build();
        }

        return ImmutableSettings.builder()
                .put("plugin.types", WatcherPlugin.class.getName())
                .put(InternalNode.HTTP_ENABLED, true)
                .put("plugin.types", WatcherPlugin.class.getName() + ","
                        + "," + LicensePlugin.class.getName())
                .build();
    }



    /** Shield related settings */

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
                "  cluster: all, manage_watcher\n" +
                "  indices:\n" +
                "    '*': all\n" +
                "\n" +
                "admin:\n" +
                "  cluster: manage_watcher, cluster:monitor/nodes/info, cluster:monitor/state, cluster:monitor/health, cluster:admin/repository/delete\n" +
                "  indices:\n" +
                "    '*': all, indices:admin/template/delete\n" +
                "\n" +
                "monitor:\n" +
                "  cluster: monitor_watcher, cluster:monitor/nodes/info\n" +
                "\n"
                ;

        public static Settings settings(boolean enabled) {
            ImmutableSettings.Builder builder = ImmutableSettings.builder();
            if (!enabled) {
                return builder.put("shield.enabled", false).build();
            }

            File folder = createFolder(globalTempDir(), "watcher_shield");
            return builder.put("shield.enabled", true)
                    .put("shield.user", "test:changeme")
                    .put("shield.authc.realms.esusers.type", ESUsersRealm.TYPE)
                    .put("shield.authc.anonymous.username","anonymous_user")
                    .put("shield.authc.anonymous.roles", "admin")
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
