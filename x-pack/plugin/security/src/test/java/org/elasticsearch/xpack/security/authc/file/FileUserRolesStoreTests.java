/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.file;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.audit.logfile.CapturingLogger;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.junit.After;
import org.junit.Before;

import java.io.BufferedWriter;
import java.io.IOException;
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
import static org.hamcrest.core.IsNull.nullValue;

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
        env = TestEnvironment.newEnvironment(settings);
        threadPool = new TestThreadPool("test");
    }

    @After
    public void shutdown() throws InterruptedException {
        terminate(threadPool);
    }

    public void testStore_ConfiguredWithUnreadableFile() throws Exception {
        Path file = getUsersRolesPath();
        List<String> lines = new ArrayList<>();
        lines.add("aldlfkjldjdflkjd");

        // writing in utf_16 should cause a parsing error as we try to read the file in utf_8
        Files.write(file, lines, StandardCharsets.UTF_16);

        RealmConfig.RealmIdentifier realmId = new RealmConfig.RealmIdentifier("file", "file-test");
        RealmConfig config = new RealmConfig(realmId, settings, env, new ThreadContext(Settings.EMPTY));
        ResourceWatcherService watcherService = new ResourceWatcherService(settings, threadPool);
        FileUserRolesStore store = new FileUserRolesStore(config, watcherService);
        assertThat(store.entriesCount(), is(0));
    }

    public void testStoreAutoReload() throws Exception {
        Path users = getDataPath("users_roles");
        Path tmp = getUsersRolesPath();
        Files.copy(users, tmp, StandardCopyOption.REPLACE_EXISTING);


        final RealmConfig.RealmIdentifier realmId = new RealmConfig.RealmIdentifier("file", "file-test");
        RealmConfig config = new RealmConfig(realmId, settings, env, new ThreadContext(Settings.EMPTY));
        ResourceWatcherService watcherService = new ResourceWatcherService(settings, threadPool);
        final CountDownLatch latch = new CountDownLatch(1);

        FileUserRolesStore store = new FileUserRolesStore(config, watcherService, latch::countDown);

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
        Path tmp = getUsersRolesPath();
        Files.copy(users, tmp, StandardCopyOption.REPLACE_EXISTING);

        final RealmConfig.RealmIdentifier realmId = new RealmConfig.RealmIdentifier("file", "file-test");
        RealmConfig config = new RealmConfig(realmId, settings, env, new ThreadContext(Settings.EMPTY));
        ResourceWatcherService watcherService = new ResourceWatcherService(settings, threadPool);
        final CountDownLatch latch = new CountDownLatch(1);

        FileUserRolesStore store = new FileUserRolesStore(config, watcherService, latch::countDown);

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

    public void testParseFileEmpty() throws Exception {
        Path empty = createTempFile();
        Logger log = CapturingLogger.newCapturingLogger(Level.DEBUG, null);
        FileUserRolesStore.parseFile(empty, log);
        List<String> events = CapturingLogger.output(log.getName(), Level.DEBUG);
        assertThat(events.size(), is(1));
        assertThat(events.get(0), containsString("parsed [0] user to role mappings"));
    }

    public void testParseFileWhenFileDoesNotExist() throws Exception {
        Path file = createTempDir().resolve(randomAlphaOfLength(10));
        Logger logger = CapturingLogger.newCapturingLogger(Level.INFO, null);
        Map<String, String[]> usersRoles = FileUserRolesStore.parseFile(file, logger);
        assertThat(usersRoles, nullValue());
        usersRoles = FileUserRolesStore.parseFileLenient(file, logger);
        assertThat(usersRoles, notNullValue());
        assertThat(usersRoles.isEmpty(), is(true));
    }

    public void testParseFileWhenCannotReadFile() throws Exception {
        Path file = createTempFile();
        List<String> lines = new ArrayList<>();
        lines.add("aldlfkjldjdflkjd");

        // writing in utf_16 should cause a parsing error as we try to read the file in utf_8
        Files.write(file, lines, StandardCharsets.UTF_16);
        Logger logger = CapturingLogger.newCapturingLogger(Level.DEBUG, null);
        try {
            FileUserRolesStore.parseFile(file, logger);
            fail("expected a parse failure");
        } catch (Exception e) {
            this.logger.info("expected", e);
        }
    }

    public void testParseFileEmptyRolesDoesNotCauseNPE() throws Exception {
        ThreadPool threadPool = null;
        try {
            threadPool = new TestThreadPool("test");
            Path usersRoles = writeUsersRoles("role1:admin");

            Settings settings = Settings.builder()
                    .put(XPackSettings.WATCHER_ENABLED.getKey(), "false")
                    .put("path.home", createTempDir())
                    .build();

            Environment env = TestEnvironment.newEnvironment(settings);
            final RealmConfig.RealmIdentifier realmId = new RealmConfig.RealmIdentifier("file", "file-test");
            RealmConfig config = new RealmConfig(realmId, settings, env, new ThreadContext(Settings.EMPTY));
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
        Logger logger = CapturingLogger.newCapturingLogger(Level.DEBUG, null);
        Map<String, String[]> usersRoles = FileUserRolesStore.parseFileLenient(file, logger);
        assertThat(usersRoles, notNullValue());
        assertThat(usersRoles.isEmpty(), is(true));
        List<String> events = CapturingLogger.output(logger.getName(), Level.ERROR);
        assertThat(events.size(), is(1));
        assertThat(events.get(0), containsString("failed to parse users_roles file"));
    }

    private Path writeUsersRoles(String input) throws Exception {
        Path file = getUsersRolesPath();
        Files.write(file, input.getBytes(StandardCharsets.UTF_8));
        return file;
    }

    private Path getUsersRolesPath() throws IOException {
        Path xpackConf = env.configFile();
        Files.createDirectories(xpackConf);
        return xpackConf.resolve("users_roles");
    }

    private void assertInvalidInputIsSilentlyIgnored(String input) throws Exception {
        Path file = createTempFile();
        Files.write(file, input.getBytes(StandardCharsets.UTF_8));
        Map<String, String[]> usersRoles = FileUserRolesStore.parseFile(file, null);
        String reason = String.format(Locale.ROOT, "Expected userRoles to be empty, but was %s", usersRoles.keySet());
        assertThat(reason, usersRoles.keySet(), hasSize(0));
    }

}
