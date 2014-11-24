/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.esusers;

import com.carrotsearch.ant.tasks.junit4.dependencies.com.google.common.base.Charsets;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.authc.support.RefreshListener;
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
import java.util.Locale;
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
                    .put("watcher.interval.high", "2s")
                    .build();

            Settings esusersSettings = ImmutableSettings.builder()
                    .put("files.users_roles", tmp.toAbsolutePath())
                    .build();

            Environment env = new Environment(settings);
            threadPool = new ThreadPool("test");
            watcherService = new ResourceWatcherService(settings, threadPool);
            final CountDownLatch latch = new CountDownLatch(1);
            FileUserRolesStore store = new FileUserRolesStore(esusersSettings, env, watcherService, new RefreshListener() {
                @Override
                public void onRefresh() {
                    latch.countDown();
                }
            });

            String[] roles = store.roles("user1");
            assertThat(roles, notNullValue());
            assertThat(roles.length, is(3));
            assertThat(roles, arrayContaining("role1", "role2", "role3"));
            assertThat(store.roles("user4"), equalTo(Strings.EMPTY_ARRAY));

            watcherService.start();

            try (BufferedWriter writer = Files.newBufferedWriter(tmp, Charsets.UTF_8, StandardOpenOption.APPEND)) {
                writer.newLine();
                writer.append("role4:user4\nrole5:user4\n");
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

    @Test
    public void testThatEmptyRolesDoesNotCauseNPE() throws Exception {
        ThreadPool threadPool = null;
        try {
            threadPool = new ThreadPool("test");
            File usersRoles = writeUsersRoles("role1:admin");

            Settings settings = ImmutableSettings.builder()
                    .put("watcher.enabled", "false")
                    .build();

            Settings esusersSettings = ImmutableSettings.builder()
                    .put("files.users_roles", usersRoles.toPath().toAbsolutePath())
                    .build();

            Environment env = new Environment(settings);
            ResourceWatcherService watcherService = new ResourceWatcherService(settings, threadPool);
            FileUserRolesStore store = new FileUserRolesStore(esusersSettings, env, watcherService);
            assertThat(store.roles("user"), equalTo(Strings.EMPTY_ARRAY));
        } finally {
            if (threadPool != null) {
                threadPool.shutdownNow();
            }
        }
    }

    @Test
    public void testThatEmptyFileIsParsed() throws Exception {
        assertInvalidInputIsSilentlyIgnored("");
        assertInvalidInputIsSilentlyIgnored("#");
    }

    @Test
    public void testThatEmptyRoleNameDoesNotThrowException() throws Exception {
        assertInvalidInputIsSilentlyIgnored(":user1,user2");
        assertInvalidInputIsSilentlyIgnored(" :user1,user2");
    }

    @Test
    public void testThatEmptyRoleDoesNotThrowException() throws Exception {
        assertInvalidInputIsSilentlyIgnored("role:");
        assertInvalidInputIsSilentlyIgnored("role: ");
        assertInvalidInputIsSilentlyIgnored("role: , ");
    }

    private File writeUsersRoles(String input) throws Exception {
        File file = newTempFile();
        com.google.common.io.Files.write(input.getBytes(Charsets.UTF_8), file);
        return file;
    }

    private void assertInvalidInputIsSilentlyIgnored(String input) throws Exception {
        File file = newTempFile();
        com.google.common.io.Files.write(input.getBytes(Charsets.UTF_8), file);
        Map<String, String[]> usersRoles = FileUserRolesStore.parseFile(file.toPath(), null);
        assertThat(String.format(Locale.ROOT, "Expected userRoles to be empty, but was %s", usersRoles.keySet()), usersRoles.keySet(), hasSize(0));
    }
}
