/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.file;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.SuppressLoggerChecks;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.audit.logfile.CapturingLogger;
import org.elasticsearch.shield.authc.RealmConfig;
import org.elasticsearch.shield.authc.support.RefreshListener;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.watcher.Watcher;
import org.elasticsearch.xpack.XPackPlugin;
import org.junit.After;
import org.junit.Before;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Matchers.contains;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 *
 */
public class FileUserRolesStoreTests extends ESTestCase {
    private Settings settings;
    private Environment env;
    private ThreadPool threadPool;

    @Before
    public void init() {
        settings = Settings.builder()
                .put("resource.reload.interval.high", "2s")
                .put("path.home", createTempDir())
                .build();
        env = new Environment(settings);
        threadPool = new ThreadPool("test");
    }

    @After
    public void shutdown() throws InterruptedException {
        terminate(threadPool);
    }

    public void testStore_ConfiguredWithUnreadableFile() throws Exception {
        Path file = createTempFile();
        List<String> lines = new ArrayList<>();
        lines.add("aldlfkjldjdflkjd");

        // writing in utf_16 should cause a parsing error as we try to read the file in utf_8
        Files.write(file, lines, StandardCharsets.UTF_16);

        Settings fileSettings = Settings.builder()
                .put("files.users_roles", file.toAbsolutePath())
                .build();

        RealmConfig config = new RealmConfig("file-test", fileSettings, settings, env);
        ResourceWatcherService watcherService = new ResourceWatcherService(settings, threadPool);
        FileUserRolesStore store = new FileUserRolesStore(config, watcherService);
        assertThat(store.entriesCount(), is(0));
    }

    public void testStoreAutoReload() throws Exception {
        Path users = getDataPath("users_roles");
        Path tmp = createTempFile();
        Files.copy(users, tmp, StandardCopyOption.REPLACE_EXISTING);

        Settings fileSettings = Settings.builder()
                .put("files.users_roles", tmp.toAbsolutePath())
                .build();

        RealmConfig config = new RealmConfig("file-test", fileSettings, settings, env);
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

        try (BufferedWriter writer = Files.newBufferedWriter(tmp, StandardCharsets.UTF_8, StandardOpenOption.APPEND)) {
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

    public void testStoreAutoReloadWithParseFailure() throws Exception {
        Path users = getDataPath("users_roles");
        Path tmp = createTempFile();
        Files.copy(users, tmp, StandardCopyOption.REPLACE_EXISTING);

        Settings fileSettings = Settings.builder()
                .put("files.users_roles", tmp.toAbsolutePath())
                .build();

        RealmConfig config = new RealmConfig("file-test", fileSettings, settings, env);
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
        Files.write(tmp, Collections.singletonList("aldlfkjldjdflkjd"), StandardCharsets.UTF_16);

        if (!latch.await(5, TimeUnit.SECONDS)) {
            fail("Waited too long for the updated file to be picked up");
        }

        assertThat(store.entriesCount(), is(0));
    }

    public void testParseFile() throws Exception {
        Path path = getDataPath("users_roles");
        Map<String, String[]> usersRoles = FileUserRolesStore.parseFile(path, null);
        assertThat(usersRoles, notNullValue());
        assertThat(usersRoles.size(), is(4));
        assertThat(usersRoles.get("user1"), notNullValue());
        assertThat(usersRoles.get("user1").length, is(3));
        assertThat(usersRoles.get("user1"), arrayContaining("role1", "role2", "role3"));
        assertThat(usersRoles.get("user2"), notNullValue());
        assertThat(usersRoles.get("user2").length, is(2));
        assertThat(usersRoles.get("user2"), arrayContaining("role2", "role3"));
        assertThat(usersRoles.get("user3"), notNullValue());
        assertThat(usersRoles.get("user3").length, is(1));
        assertThat(usersRoles.get("user3"), arrayContaining("role3"));
        assertThat(usersRoles.get("period.user").length, is(1));
        assertThat(usersRoles.get("period.user"), arrayContaining("role4"));
    }

    @SuppressLoggerChecks(reason = "mock usage")
    public void testParseFileEmpty() throws Exception {
        Path empty = createTempFile();
        ESLogger log = ESLoggerFactory.getLogger("test");
        log = spy(log);
        FileUserRolesStore.parseFile(empty, log);
        verify(log, times(1)).warn(contains("no entries found"), eq(empty.toAbsolutePath()));
    }

    public void testParseFileWhenFileDoesNotExist() throws Exception {
        Path file = createTempDir().resolve(randomAsciiOfLength(10));
        CapturingLogger logger = new CapturingLogger(CapturingLogger.Level.INFO);
        Map<String, String[]> usersRoles = FileUserRolesStore.parseFile(file, logger);
        assertThat(usersRoles, notNullValue());
        assertThat(usersRoles.isEmpty(), is(true));
    }

    public void testParseFileWhenCannotReadFile() throws Exception {
        Path file = createTempFile();
        List<String> lines = new ArrayList<>();
        lines.add("aldlfkjldjdflkjd");

        // writing in utf_16 should cause a parsing error as we try to read the file in utf_8
        Files.write(file, lines, StandardCharsets.UTF_16);
        CapturingLogger logger = new CapturingLogger(CapturingLogger.Level.INFO);
        try {
            FileUserRolesStore.parseFile(file, logger);
            fail("expected a parse failure");
        } catch (Throwable t) {
            this.logger.info("expected", t);
        }
    }

    public void testParseFileEmptyRolesDoesNotCauseNPE() throws Exception {
        ThreadPool threadPool = null;
        try {
            threadPool = new ThreadPool("test");
            Path usersRoles = writeUsersRoles("role1:admin");

            Settings settings = Settings.builder()
                    .put(XPackPlugin.featureEnabledSetting(Watcher.NAME), "false")
                    .put("path.home", createTempDir())
                    .build();

            Settings fileSettings = Settings.builder()
                    .put("files.users_roles", usersRoles.toAbsolutePath())
                    .build();

            Environment env = new Environment(settings);
            RealmConfig config = new RealmConfig("file-test", fileSettings, settings, env);
            ResourceWatcherService watcherService = new ResourceWatcherService(settings, threadPool);
            FileUserRolesStore store = new FileUserRolesStore(config, watcherService);
            assertThat(store.roles("user"), equalTo(Strings.EMPTY_ARRAY));
        } finally {
            terminate(threadPool);
        }
    }

    public void testParseFileEmptyFileIsParsed() throws Exception {
        assertInvalidInputIsSilentlyIgnored("");
        assertInvalidInputIsSilentlyIgnored("#");
    }

    public void testParseFileEmptyRoleNameDoesNotThrowException() throws Exception {
        assertInvalidInputIsSilentlyIgnored(":user1,user2");
        assertInvalidInputIsSilentlyIgnored(" :user1,user2");
    }

    public void testParseFileEmptyRoleDoesNotThrowException() throws Exception {
        assertInvalidInputIsSilentlyIgnored("role:");
        assertInvalidInputIsSilentlyIgnored("role: ");
        assertInvalidInputIsSilentlyIgnored("role: , ");
    }

    public void testParseFileLenientWhenCannotReadFile() throws Exception {
        Path file = createTempFile();
        List<String> lines = new ArrayList<>();
        lines.add("aldlfkjldjdflkjd");

        // writing in utf_16 should cause a parsing error as we try to read the file in utf_8
        Files.write(file, lines, StandardCharsets.UTF_16);
        CapturingLogger logger = new CapturingLogger(CapturingLogger.Level.INFO);
        Map<String, String[]> usersRoles = FileUserRolesStore.parseFileLenient(file, logger);
        assertThat(usersRoles, notNullValue());
        assertThat(usersRoles.isEmpty(), is(true));
        List<CapturingLogger.Msg> msgs = logger.output(CapturingLogger.Level.ERROR);
        assertThat(msgs.size(), is(1));
        assertThat(msgs.get(0).text, containsString("failed to parse users_roles file"));
    }

    private Path writeUsersRoles(String input) throws Exception {
        Path file = createTempFile();
        Files.write(file, input.getBytes(StandardCharsets.UTF_8));
        return file;
    }

    private void assertInvalidInputIsSilentlyIgnored(String input) throws Exception {
        Path file = createTempFile();
        Files.write(file, input.getBytes(StandardCharsets.UTF_8));
        Map<String, String[]> usersRoles = FileUserRolesStore.parseFile(file, null);
        String reason = String.format(Locale.ROOT, "Expected userRoles to be empty, but was %s", usersRoles.keySet());
        assertThat(reason, usersRoles.keySet(), hasSize(0));
    }
}
