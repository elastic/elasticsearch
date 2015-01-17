/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.esusers;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.base.Charsets;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.audit.logfile.CapturingLogger;
import org.elasticsearch.shield.authc.RealmConfig;
import org.elasticsearch.shield.authc.support.RefreshListener;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.contains;
import static org.mockito.Mockito.*;

/**
 *
 */
public class FileUserRolesStoreTests extends ElasticsearchTestCase {

    private Settings settings;
    private Environment env;
    private ThreadPool threadPool;

    @Before
    public void init() {
        settings = ImmutableSettings.builder()
                .put("watcher.interval.high", "2s")
                .build();
        env = new Environment(settings);
        threadPool = new ThreadPool("test");
    }

    @After
    public void shutdown() {
        threadPool.shutdownNow();
    }

    @Test
    public void testStore_ConfiguredWithUnreadableFile() throws Exception {

        File file = newTempFile();
        List<String> lines = new ArrayList<>();
        lines.add("aldlfkjldjdflkjd");

        // writing in utf_16 should cause a parsing error as we try to read the file in utf_8
        Files.write(file.toPath(), lines, Charsets.UTF_16);

        Settings esusersSettings = ImmutableSettings.builder()
                .put("files.users_roles", file.toPath().toAbsolutePath())
                .build();

        RealmConfig config = new RealmConfig("esusers-test", esusersSettings, settings, env);
        ResourceWatcherService watcherService = new ResourceWatcherService(settings, threadPool);
        FileUserRolesStore store = new FileUserRolesStore(config, watcherService);
        assertThat(store.entriesCount(), is(0));
    }

    @Test
    public void testStore_AutoReload() throws Exception {
        Path users = Paths.get(getClass().getResource("users_roles").toURI());
        Path tmp = Files.createTempFile(null, null);
        Files.copy(users, Files.newOutputStream(tmp));

        Settings esusersSettings = ImmutableSettings.builder()
                .put("files.users_roles", tmp.toAbsolutePath())
                .build();

        RealmConfig config = new RealmConfig("esusers-test", esusersSettings, settings, env);
        ResourceWatcherService watcherService = new ResourceWatcherService(settings, threadPool);
        final CountDownLatch latch = new CountDownLatch(1);

        FileUserRolesStore store = new FileUserRolesStore(config, watcherService, new RefreshListener() {
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
    }

    @Test
    public void testStore_AutoReload_WithParseFailure() throws Exception {
        Path users = Paths.get(getClass().getResource("users_roles").toURI());
        Path tmp = Files.createTempFile(null, null);
        Files.copy(users, Files.newOutputStream(tmp));

        Settings esusersSettings = ImmutableSettings.builder()
                .put("files.users_roles", tmp.toAbsolutePath())
                .build();

        RealmConfig config = new RealmConfig("esusers-test", esusersSettings, settings, env);
        ResourceWatcherService watcherService = new ResourceWatcherService(settings, threadPool);
        final CountDownLatch latch = new CountDownLatch(1);

        FileUserRolesStore store = new FileUserRolesStore(config, watcherService, new RefreshListener() {
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

        // now replacing the content of the users file with something that cannot be read
        Files.write(tmp, ImmutableList.of("aldlfkjldjdflkjd"), Charsets.UTF_16);

        if (!latch.await(5, TimeUnit.SECONDS)) {
            fail("Waited too long for the updated file to be picked up");
        }

        assertThat(store.entriesCount(), is(0));
    }

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
    public void testParseFile_Empty() throws Exception {
        File empty = newTempFile();
        ESLogger log = ESLoggerFactory.getLogger("test");
        log = spy(log);
        FileUserRolesStore.parseFile(empty.toPath(), log);
        verify(log, times(1)).warn(contains("no entries found"), eq(empty.toPath().toAbsolutePath()));
    }

    @Test
    public void testParseFile_WhenFileDoesNotExist() throws Exception {
        File file = new File(randomAsciiOfLength(10));
        CapturingLogger logger = new CapturingLogger(CapturingLogger.Level.INFO);
        Map<String, String[]> usersRoles = FileUserRolesStore.parseFile(file.toPath(), logger);
        assertThat(usersRoles, notNullValue());
        assertThat(usersRoles.isEmpty(), is(true));
    }

    @Test
    public void testParseFile_WhenCannotReadFile() throws Exception {
        File file = newTempFile();
        List<String> lines = new ArrayList<>();
        lines.add("aldlfkjldjdflkjd");

        // writing in utf_16 should cause a parsing error as we try to read the file in utf_8
        Files.write(file.toPath(), lines, Charsets.UTF_16);
        CapturingLogger logger = new CapturingLogger(CapturingLogger.Level.INFO);
        try {
            FileUserRolesStore.parseFile(file.toPath(), logger);
            fail("expected a parse failure");
        } catch (Throwable t) {
            this.logger.info("expected", t);
        }
    }

    @Test
    public void testParseFile_EmptyRolesDoesNotCauseNPE() throws Exception {
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
            RealmConfig config = new RealmConfig("esusers-test", esusersSettings, settings, env);
            ResourceWatcherService watcherService = new ResourceWatcherService(settings, threadPool);
            FileUserRolesStore store = new FileUserRolesStore(config, watcherService);
            assertThat(store.roles("user"), equalTo(Strings.EMPTY_ARRAY));
        } finally {
            if (threadPool != null) {
                threadPool.shutdownNow();
            }
        }
    }

    @Test
    public void testParseFile_EmptyFileIsParsed() throws Exception {
        assertInvalidInputIsSilentlyIgnored("");
        assertInvalidInputIsSilentlyIgnored("#");
    }

    @Test
    public void testParseFile_EmptyRoleNameDoesNotThrowException() throws Exception {
        assertInvalidInputIsSilentlyIgnored(":user1,user2");
        assertInvalidInputIsSilentlyIgnored(" :user1,user2");
    }

    @Test
    public void testParseFile_EmptyRoleDoesNotThrowException() throws Exception {
        assertInvalidInputIsSilentlyIgnored("role:");
        assertInvalidInputIsSilentlyIgnored("role: ");
        assertInvalidInputIsSilentlyIgnored("role: , ");
    }

    @Test
    public void testParseFileLenient_WhenCannotReadFile() throws Exception {
        File file = newTempFile();
        List<String> lines = new ArrayList<>();
        lines.add("aldlfkjldjdflkjd");

        // writing in utf_16 should cause a parsing error as we try to read the file in utf_8
        Files.write(file.toPath(), lines, Charsets.UTF_16);
        CapturingLogger logger = new CapturingLogger(CapturingLogger.Level.INFO);
        Map<String, String[]> usersRoles = FileUserRolesStore.parseFileLenient(file.toPath(), logger);
        assertThat(usersRoles, notNullValue());
        assertThat(usersRoles.isEmpty(), is(true));
        List<CapturingLogger.Msg> msgs = logger.output(CapturingLogger.Level.ERROR);
        assertThat(msgs.size(), is(1));
        assertThat(msgs.get(0).text, containsString("failed to parse users_roles file"));
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
