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
import org.elasticsearch.shield.authz.Permission;
import org.elasticsearch.shield.authz.Privilege;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

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

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testParseFile() throws Exception {
        Path path = Paths.get(getClass().getResource("roles.yml").toURI());
        Map<String, Permission.Global> roles = FileRolesStore.parseFile(path);
        assertThat(roles, notNullValue());
        assertThat(roles.size(), is(3));

        Permission.Global permission = roles.get("role1");
        assertThat(permission, notNullValue());
        assertThat(permission.cluster(), notNullValue());
        assertThat(permission.cluster().privilege(), is(Privilege.Cluster.ALL));
        assertThat(permission.indices(), notNullValue());
        assertThat(permission.indices().groups(), notNullValue());
        assertThat(permission.indices().groups().length, is(2));

        Permission.Global.Indices.Group group = permission.indices().groups()[0];
        assertThat(group.indices(), notNullValue());
        assertThat(group.indices().length, is(2));
        assertThat(group.indices()[0], equalTo("idx1"));
        assertThat(group.indices()[1], equalTo("idx2"));
        assertThat(group.privilege(), notNullValue());
        assertThat(group.privilege(), is(Privilege.Index.READ));

        group = permission.indices().groups()[1];
        assertThat(group.indices(), notNullValue());
        assertThat(group.indices().length, is(1));
        assertThat(group.indices()[0], equalTo("idx3"));
        assertThat(group.privilege(), notNullValue());
        assertThat(group.privilege(), is(Privilege.Index.CRUD));

        permission = roles.get("role2");
        assertThat(permission, notNullValue());
        assertThat(permission.cluster(), notNullValue());
        assertThat(permission.cluster().privilege(), is(Privilege.Cluster.ALL)); // MONITOR is collapsed into ALL
        assertThat(permission.indices(), notNullValue());
        assertThat(permission.indices(), is(Permission.Global.Indices.NONE));

        permission = roles.get("role3");
        assertThat(permission, notNullValue());
        assertThat(permission.cluster(), notNullValue());
        assertThat(permission.cluster(), is(Permission.Global.Cluster.NONE));
        assertThat(permission.indices(), notNullValue());
        assertThat(permission.indices().groups(), notNullValue());
        assertThat(permission.indices().groups().length, is(1));

        group = permission.indices().groups()[0];
        assertThat(group.indices(), notNullValue());
        assertThat(group.indices().length, is(1));
        assertThat(group.indices()[0], equalTo(".*_.*"));
        assertThat(group.privilege(), notNullValue());
        assertThat(group.privilege().isAlias(Privilege.Index.union(Privilege.Index.READ, Privilege.Index.WRITE)), is(true));
    }

    @Test
    public void testAutoReload() throws Exception {
        ThreadPool threadPool = null;
        ResourceWatcherService watcherService = null;
        try {
            Path users = Paths.get(getClass().getResource("roles.yml").toURI());
            Path tmp = tempFolder.newFile().toPath();
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

            Permission.Global permission = store.permission("role1");
            assertThat(permission, notNullValue());
            permission = store.permission("role4");
            assertThat(permission, nullValue());

            watcherService.start();

            try (BufferedWriter writer = Files.newBufferedWriter(tmp, Charsets.UTF_8, StandardOpenOption.APPEND)) {
                writer.newLine();
                writer.newLine();
                writer.newLine();
                writer.append("role4:").append(System.lineSeparator());
                writer.append("  cluster: 'MONITOR'");
            }

            if (!latch.await(5, TimeUnit.SECONDS)) {
                fail("Waited too long for the updated file to be picked up");
            }

            permission = store.permission("role4");
            assertThat(permission, notNullValue());
            assertThat(permission.check("cluster:monitor/foo/bar", null, null), is(true));
            assertThat(permission.check("cluster:admin/foo/bar", null, null), is(false));

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
        File file = tempFolder.newFile();
        com.google.common.io.Files.write("#".getBytes(Charsets.UTF_8), file);
        Map<String, Permission.Global> roles = FileRolesStore.parseFile(file.toPath());
        assertThat(roles.keySet(), is(empty()));
    }

    @Test(expected = ElasticsearchException.class)
    public void testThatInvalidYAMLThrowsElasticsearchException() throws Exception {
        File file = tempFolder.newFile();
        com.google.common.io.Files.write("user: cluster: ALL indices: '.*': ALL".getBytes(Charsets.UTF_8), file);
        FileRolesStore.parseFile(file.toPath());
    }
}
