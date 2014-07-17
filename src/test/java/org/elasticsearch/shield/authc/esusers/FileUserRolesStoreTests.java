/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.esusers;

import com.carrotsearch.ant.tasks.junit4.dependencies.com.google.common.base.Charsets;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.Test;

import java.io.BufferedWriter;
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
public class FileUserRolesStoreTests extends ElasticsearchTestCase {

    @Test
    public void testParseFile() throws Exception {
        Path path = Paths.get(getClass().getResource("users_roles").toURI());
        Map<String, String[]> usersRoles = FileUserRolesStore.parseFile(path, null);
        assertThat(usersRoles, notNullValue());
        assertThat(usersRoles.size(), is(3));
        assertThat(usersRoles.get("user1"), notNullValue());
        assertThat(usersRoles.get("user1").length, is(3));
        assertThat(usersRoles.get("user1"), arrayContaining("role1", "role2", "role3"));
        assertThat(usersRoles.get("user2"), notNullValue());
        assertThat(usersRoles.get("user2").length, is(2));
        assertThat(usersRoles.get("user2"), arrayContaining("role2", "role3"));
        assertThat(usersRoles.get("user3"), notNullValue());
        assertThat(usersRoles.get("user3").length, is(1));
        assertThat(usersRoles.get("user3"), arrayContaining("role3"));
    }

    @Test
    public void testAutoReload() throws Exception {
        ThreadPool threadPool = null;
        ResourceWatcherService watcherService = null;
        try {
            Path users = Paths.get(getClass().getResource("users_roles").toURI());
            Path tmp = Files.createTempFile(null, null);
            Files.copy(users, Files.newOutputStream(tmp));

            Settings settings = ImmutableSettings.builder()
                    .put("watcher.interval", "2s")
                    .put("shield.authc.esusers.file.users_roles", tmp.toAbsolutePath())
                    .build();

            Environment env = new Environment(settings);
            threadPool = new ThreadPool("test");
            watcherService = new ResourceWatcherService(settings, threadPool);
            final CountDownLatch latch = new CountDownLatch(1);
            FileUserRolesStore store = new FileUserRolesStore(settings, env, watcherService, new FileUserRolesStore.Listener() {
                @Override
                public void onRefresh() {
                    latch.countDown();
                }
            });

            String[] roles = store.roles("user1");
            assertThat(roles, notNullValue());
            assertThat(roles.length, is(3));
            assertThat(roles, arrayContaining("role1", "role2", "role3"));
            assertThat(store.roles("user4"), nullValue());

            watcherService.start();

            try (BufferedWriter writer = Files.newBufferedWriter(tmp, Charsets.UTF_8, StandardOpenOption.APPEND)) {
                writer.newLine();
                writer.append("user4:role4,role5");
            }

            if (!latch.await(5, TimeUnit.SECONDS)) {
                fail("Waited too long for the updated file to be picked up");
            }

            roles = store.roles("user4");
            assertThat(roles, notNullValue());
            assertThat(roles.length, is(2));
            assertThat(roles, arrayContaining("role4", "role5"));

        } finally {
            if (watcherService != null) {
                watcherService.stop();
            }
            if (threadPool != null) {
                threadPool.shutdownNow();
            }
        }

    }
}
