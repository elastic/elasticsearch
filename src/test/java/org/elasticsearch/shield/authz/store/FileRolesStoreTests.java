/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz.store;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.base.Charsets;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.ShieldException;
import org.elasticsearch.shield.authz.Permission;
import org.elasticsearch.shield.authz.Privilege;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.*;

/**
 *
 */
public class FileRolesStoreTests extends ElasticsearchTestCase {

    @Test
    public void testParseFile() throws Exception {
        Path path = Paths.get(getClass().getResource("roles.yml").toURI());
        Map<String, Permission.Global.Role> roles = FileRolesStore.parseFile(path, logger);
        assertThat(roles, notNullValue());
        assertThat(roles.size(), is(4));

        Permission.Global.Role role = roles.get("role1");
        assertThat(role, notNullValue());
        assertThat(role.name(), equalTo("role1"));
        assertThat(role.cluster(), notNullValue());
        assertThat(role.cluster().privilege(), is(Privilege.Cluster.ALL));
        assertThat(role.indices(), notNullValue());
        assertThat(role.indices().groups(), notNullValue());
        assertThat(role.indices().groups().length, is(2));

        Permission.Global.Indices.Group group = role.indices().groups()[0];
        assertThat(group.indices(), notNullValue());
        assertThat(group.indices().length, is(2));
        assertThat(group.indices()[0], equalTo("idx1"));
        assertThat(group.indices()[1], equalTo("idx2"));
        assertThat(group.privilege(), notNullValue());
        assertThat(group.privilege(), is(Privilege.Index.READ));

        group = role.indices().groups()[1];
        assertThat(group.indices(), notNullValue());
        assertThat(group.indices().length, is(1));
        assertThat(group.indices()[0], equalTo("idx3"));
        assertThat(group.privilege(), notNullValue());
        assertThat(group.privilege(), is(Privilege.Index.CRUD));

        role = roles.get("role2");
        assertThat(role, notNullValue());
        assertThat(role.name(), equalTo("role2"));
        assertThat(role.cluster(), notNullValue());
        assertThat(role.cluster().privilege(), is(Privilege.Cluster.ALL)); // MONITOR is collapsed into ALL
        assertThat(role.indices(), notNullValue());
        assertThat(role.indices(), is(Permission.Indices.Core.NONE));

        role = roles.get("role3");
        assertThat(role, notNullValue());
        assertThat(role.name(), equalTo("role3"));
        assertThat(role.cluster(), notNullValue());
        assertThat(role.cluster(), is(Permission.Cluster.Core.NONE));
        assertThat(role.indices(), notNullValue());
        assertThat(role.indices().groups(), notNullValue());
        assertThat(role.indices().groups().length, is(1));

        group = role.indices().groups()[0];
        assertThat(group.indices(), notNullValue());
        assertThat(group.indices().length, is(1));
        assertThat(group.indices()[0], equalTo("/.*_.*/"));
        assertThat(group.privilege(), notNullValue());
        assertThat(group.privilege().isAlias(Privilege.Index.union(Privilege.Index.READ, Privilege.Index.WRITE)), is(true));

        role = roles.get("role4");
        assertThat(role, notNullValue());
        assertThat(role.name(), equalTo("role4"));
        assertThat(role.cluster(), notNullValue());
        assertThat(role.cluster(), is(Permission.Cluster.Core.NONE));
        assertThat(role.indices(), is(Permission.Indices.Core.NONE));
    }

    /**
     * This test is mainly to make sure we can read the default roles.yml config
     */
    @Test
    public void testDefaultRolesFile() throws Exception {
        Path path = Paths.get(getClass().getResource("default_roles.yml").toURI());
        Map<String, Permission.Global.Role> roles = FileRolesStore.parseFile(path, logger);
        assertThat(roles, notNullValue());
        assertThat(roles.size(), is(8));

        assertThat(roles, hasKey("admin"));
        assertThat(roles, hasKey("power_user"));
        assertThat(roles, hasKey("user"));
        assertThat(roles, hasKey("kibana3"));
        assertThat(roles, hasKey("kibana4"));
        assertThat(roles, hasKey("logstash"));
        assertThat(roles, hasKey("marvel_user"));
        assertThat(roles, hasKey("marvel_agent"));
    }

    @Test(expected = ShieldException.class)
    public void testInvalidRoleName() throws Exception {
        String roles = "\"$dlk39\":\n" +
                "  cluster: all";
        Path file = newTempFile().toPath();
        Files.write(file, roles.getBytes(UTF8));
        FileRolesStore.parseFile(file, null);
    }

    @Test
    public void testAutoReload() throws Exception {
        ThreadPool threadPool = null;
        ResourceWatcherService watcherService = null;
        try {
            Path users = Paths.get(getClass().getResource("roles.yml").toURI());
            Path tmp = newTempFile().toPath();
            Files.copy(users, Files.newOutputStream(tmp));

            Settings settings = ImmutableSettings.builder()
                    .put("watcher.interval.high", "500ms")
                    .put("shield.authz.store.files.roles", tmp.toAbsolutePath())
                    .build();

            Environment env = new Environment(settings);
            threadPool = new ThreadPool("test");
            watcherService = new ResourceWatcherService(settings, threadPool);
            final CountDownLatch latch = new CountDownLatch(1);
            FileRolesStore store = new FileRolesStore(settings, env, watcherService, new FileRolesStore.Listener() {
                @Override
                public void onRefresh() {
                    latch.countDown();
                }
            });

            Permission.Global.Role role = store.role("role1");
            assertThat(role, notNullValue());
            role = store.role("role5");
            assertThat(role, nullValue());

            watcherService.start();

            try (BufferedWriter writer = Files.newBufferedWriter(tmp, Charsets.UTF_8, StandardOpenOption.APPEND)) {
                writer.newLine();
                writer.newLine();
                writer.newLine();
                writer.append("role5:").append(System.lineSeparator());
                writer.append("  cluster: 'MONITOR'");
            }

            if (!latch.await(5, TimeUnit.SECONDS)) {
                fail("Waited too long for the updated file to be picked up");
            }

            role = store.role("role5");
            assertThat(role, notNullValue());
            assertThat(role.name(), equalTo("role5"));
            assertThat(role.cluster().check("cluster:monitor/foo/bar"), is(true));
            assertThat(role.cluster().check("cluster:admin/foo/bar"), is(false));

        } finally {
            if (watcherService != null) {
                watcherService.stop();
            }
            if (threadPool != null) {
                threadPool.shutdownNow();
            }
        }
    }

    @Test
    public void testThatEmptyFileDoesNotResultInLoop() throws Exception {
        File file = newTempFile();
        com.google.common.io.Files.write("#".getBytes(Charsets.UTF_8), file);
        Map<String, Permission.Global.Role> roles = FileRolesStore.parseFile(file.toPath(), logger);
        assertThat(roles.keySet(), is(empty()));
    }

    @Test(expected = ElasticsearchException.class)
    public void testThatInvalidYAMLThrowsElasticsearchException() throws Exception {
        File file = newTempFile();
        com.google.common.io.Files.write("user: cluster: ALL indices: '*': ALL".getBytes(Charsets.UTF_8), file);
        FileRolesStore.parseFile(file.toPath(), logger);
    }
}
