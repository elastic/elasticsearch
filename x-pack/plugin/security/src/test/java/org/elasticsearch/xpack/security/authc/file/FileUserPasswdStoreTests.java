/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.file;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.security.audit.logfile.CapturingLogger;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.user.User;
import org.junit.After;
import org.junit.Before;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;

public class FileUserPasswdStoreTests extends ESTestCase {

    private Settings settings;
    private Environment env;
    private ThreadPool threadPool;

    @Before
    public void init() {
        final String hashingAlgorithm = inFipsJvm()
            ? randomFrom("pbkdf2", "pbkdf2_1000", "pbkdf2_50000", "pbkdf2_stretch")
            : randomFrom("bcrypt", "bcrypt11", "pbkdf2", "pbkdf2_1000", "pbkdf2_50000", "pbkdf2_stretch");
        settings = Settings.builder()
            .put("resource.reload.interval.high", "100ms")
            .put("path.home", createTempDir())
            .put("xpack.security.authc.password_hashing.algorithm", hashingAlgorithm)
            .build();
        env = TestEnvironment.newEnvironment(settings);
        threadPool = new TestThreadPool("test");
    }

    @After
    public void shutdown() {
        terminate(threadPool);
    }

    public void testStore_ConfiguredWithUnreadableFile() throws Exception {
        Path configDir = env.configDir();
        Files.createDirectories(configDir);
        Path file = configDir.resolve("users");

        // writing in utf_16 should cause a parsing error as we try to read the file in utf_8
        Files.write(file, Collections.singletonList("aldlfkjldjdflkjd"), StandardCharsets.UTF_16);

        RealmConfig config = getRealmConfig();
        try (ResourceWatcherService watcherService = new ResourceWatcherService(settings, threadPool)) {
            FileUserPasswdStore store = new FileUserPasswdStore(config, watcherService);
            assertThat(store.usersCount(), is(0));
        }
    }

    public void testStore_AutoReload() throws Exception {
        Path users = getDataPath("users");
        Path configDir = env.configDir();
        Files.createDirectories(configDir);
        Path file = configDir.resolve("users");
        Files.copy(users, file, StandardCopyOption.REPLACE_EXISTING);
        final Hasher hasher = Hasher.resolve(settings.get("xpack.security.authc.password_hashing.algorithm"));
        RealmConfig config = getRealmConfig();
        try (ResourceWatcherService watcherService = new ResourceWatcherService(settings, threadPool)) {
            final CountDownLatch latch = new CountDownLatch(1);

            FileUserPasswdStore store = new FileUserPasswdStore(config, watcherService, latch::countDown);
            // Test users share the hashing algorithm name for convenience
            String username = settings.get("xpack.security.authc.password_hashing.algorithm");
            User user = new User(username);
            assertThat(store.userExists(username), is(true));
            final String password = username.startsWith("pbkdf2") ? "longertestpassword" : "test123";
            AuthenticationResult<User> result = store.verifyPassword(username, new SecureString(password), () -> user);
            assertThat(result.getStatus(), is(AuthenticationResult.Status.SUCCESS));
            assertThat(result.getValue(), is(user));

            try (BufferedWriter writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8, StandardOpenOption.APPEND)) {
                writer.append("\n");
            }

            watcherService.notifyNow(ResourceWatcherService.Frequency.HIGH);
            if (latch.getCount() != 1) {
                fail("Listener should not be called as users passwords are not changed.");
            }

            assertThat(store.userExists(username), is(true));
            result = store.verifyPassword(username, new SecureString(password), () -> user);
            assertThat(result.getStatus(), is(AuthenticationResult.Status.SUCCESS));
            assertThat(result.getValue(), is(user));

            try (BufferedWriter writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8, StandardOpenOption.APPEND)) {
                writer.newLine();
                writer.append("foobar:").append(new String(hasher.hash(new SecureString("longtestpassword"))));
            }

            if (latch.await(5, TimeUnit.SECONDS) == false) {
                fail("Waited too long for the updated file to be picked up");
            }

            assertThat(store.userExists("foobar"), is(true));
            result = store.verifyPassword("foobar", new SecureString("longtestpassword"), () -> user);
            assertThat(result.getStatus(), is(AuthenticationResult.Status.SUCCESS));
            assertThat(result.getValue(), is(user));
        }
    }

    private RealmConfig getRealmConfig() {
        final RealmConfig.RealmIdentifier identifier = new RealmConfig.RealmIdentifier("file", "file-test");
        return new RealmConfig(
            identifier,
            Settings.builder().put(settings).put(RealmSettings.getFullSettingKey(identifier, RealmSettings.ORDER_SETTING), 0).build(),
            env,
            threadPool.getThreadContext()
        );
    }

    public void testStore_AutoReload_WithParseFailures() throws Exception {
        Path users = getDataPath("users");
        Path confDir = env.configDir();
        Files.createDirectories(confDir);
        Path testUsers = confDir.resolve("users");
        Files.copy(users, testUsers, StandardCopyOption.REPLACE_EXISTING);

        RealmConfig config = getRealmConfig();
        try (ResourceWatcherService watcherService = new ResourceWatcherService(settings, threadPool)) {
            final CountDownLatch latch = new CountDownLatch(1);

            FileUserPasswdStore store = new FileUserPasswdStore(config, watcherService, latch::countDown);
            // Test users share the hashing algorithm name for convenience
            String username = settings.get("xpack.security.authc.password_hashing.algorithm");
            User user = new User(username);
            final String password = username.startsWith("pbkdf2") ? "longertestpassword" : "test123";
            final AuthenticationResult<User> result = store.verifyPassword(username, new SecureString(password), () -> user);
            assertThat(result.getStatus(), is(AuthenticationResult.Status.SUCCESS));
            assertThat(result.getValue(), is(user));

            // now replacing the content of the users file with something that cannot be read
            Files.write(testUsers, Collections.singletonList("aldlfkjldjdflkjd"), StandardCharsets.UTF_16);

            if (latch.await(5, TimeUnit.SECONDS) == false) {
                fail("Waited too long for the updated file to be picked up");
            }

            assertThat(store.usersCount(), is(0));
        }
    }

    public void testParseFile() throws Exception {
        Path path = getDataPath("users");
        Map<String, char[]> users = FileUserPasswdStore.parseFile(path, null, Settings.EMPTY);
        assertThat(users, notNullValue());
        assertThat(users.size(), is(12));
        assertThat(users.get("bcrypt"), notNullValue());
        assertThat(new String(users.get("bcrypt")), equalTo("$2a$05$zxnP0vdREMxnEpkLCDI2OuSaSk/QEKA2.A42iOpI6U2u.RLLOWm1e"));
        assertThat(users.get("bcrypt10"), notNullValue());
        assertThat(new String(users.get("bcrypt10")), equalTo("$2a$10$cFxpMx6YDrH/PXwLpTlux.KVykN1TG2Pgdl5oJX5/G/KYp3G6jbFG"));
        assertThat(users.get("md5"), notNullValue());
        assertThat(new String(users.get("md5")), equalTo("$apr1$R3DdqiAZ$aljIkaIVPSarmDMlJUBBP."));
        assertThat(users.get("crypt"), notNullValue());
        assertThat(new String(users.get("crypt")), equalTo("hsP1PYSLsEEvs"));
        assertThat(users.get("plain"), notNullValue());
        assertThat(new String(users.get("plain")), equalTo("{plain}test123"));
        assertThat(users.get("sha"), notNullValue());
        assertThat(new String(users.get("sha")), equalTo("{SHA}cojt0Pw//L6ToM8G41aOKFIWh7w="));
        assertThat(users.get("pbkdf2"), notNullValue());
        assertThat(
            new String(users.get("pbkdf2")),
            equalTo("{PBKDF2}10000$NB6kwTrIPrwJJTu+KXiPUkW5bMf1oG2BMzDJLA479Bk=$CvCgHb5UkalUiNPicqMDOzIsnh3ppyz3SZOp+Gjv+hc=")
        );
        assertThat(users.get("pbkdf2_1000"), notNullValue());
        assertThat(
            new String(users.get("pbkdf2_1000")),
            equalTo("{PBKDF2}1000$cofpEhehEObS+tNtS8/t9Zpf6UgwqkgkQFct2hhmGWA=$9Qb0S04fkF+Ebz1sGIaB9S6huZAXDihopPc6Z748f3E=")
        );
        assertThat(users.get("pbkdf2_50000"), notNullValue());
        assertThat(
            new String(users.get("pbkdf2_50000")),
            equalTo("{PBKDF2}50000$riPhBgfrNIpsN91QmF5mQNCwxHfJm0q2XtGt0x5+PRM=$v2j/DD+aFIRrusEeSDUO+eX3IrBPiG+ysgc9y0RDmhs=")
        );
        assertThat(
            new String(users.get("pbkdf2_stretch")),
            equalTo("{PBKDF2_STRETCH}10000$s1y/xv1T1iJxS9BKQ1FkZpSO19dSs6vsGgOb14d+KkU=$PtdgZoRGCSaim033lz/RcEoyhXQ/3WU4E6hfeKGsGes=")
        );
    }

    public void testParseFile_Empty() throws Exception {
        Path empty = createTempFile();
        Logger logger = CapturingLogger.newCapturingLogger(Level.DEBUG, null);
        Map<String, char[]> users = FileUserPasswdStore.parseFile(empty, logger, Settings.EMPTY);
        assertThat(users, notNullValue());
        assertThat(users.isEmpty(), is(true));
        List<String> events = CapturingLogger.output(logger.getName(), Level.DEBUG);
        assertThat(events.size(), is(1));
        assertThat(events.get(0), containsString("parsed [0] users"));
    }

    public void testParseFile_WhenFileDoesNotExist() throws Exception {
        Path file = createTempDir().resolve(randomAlphaOfLength(10));
        Logger logger = CapturingLogger.newCapturingLogger(Level.INFO, null);
        Map<String, char[]> users = FileUserPasswdStore.parseFile(file, logger, Settings.EMPTY);
        assertThat(users, nullValue());
        users = FileUserPasswdStore.parseFileLenient(file, logger, Settings.EMPTY);
        assertThat(users, notNullValue());
        assertThat(users.isEmpty(), is(true));
    }

    public void testParseFile_WhenCannotReadFile() throws Exception {
        Path file = createTempFile();
        // writing in utf_16 should cause a parsing error as we try to read the file in utf_8
        Files.write(file, Collections.singletonList("aldlfkjldjdflkjd"), StandardCharsets.UTF_16);
        Logger logger = CapturingLogger.newCapturingLogger(Level.INFO, null);
        try {
            FileUserPasswdStore.parseFile(file, logger, Settings.EMPTY);
            fail("expected a parse failure");
        } catch (IllegalStateException se) {
            this.logger.info("expected", se);
        }
    }

    public void testParseFile_InvalidLineDoesNotResultInLoggerNPE() throws Exception {
        Path file = createTempFile();
        Files.write(file, Arrays.asList("NotValidUsername=Password", "user:pass"), StandardCharsets.UTF_8);
        Map<String, char[]> users = FileUserPasswdStore.parseFile(file, null, Settings.EMPTY);
        assertThat(users, notNullValue());
        assertThat(users.keySet(), hasSize(1));
    }

    public void testParseFileLenient_WhenCannotReadFile() throws Exception {
        Path file = createTempFile();
        // writing in utf_16 should cause a parsing error as we try to read the file in utf_8
        Files.write(file, Collections.singletonList("aldlfkjldjdflkjd"), StandardCharsets.UTF_16);
        Logger logger = CapturingLogger.newCapturingLogger(Level.INFO, null);
        Map<String, char[]> users = FileUserPasswdStore.parseFileLenient(file, logger, Settings.EMPTY);
        assertThat(users, notNullValue());
        assertThat(users.isEmpty(), is(true));
        List<String> events = CapturingLogger.output(logger.getName(), Level.ERROR);
        assertThat(events.size(), is(1));
        assertThat(events.get(0), containsString("failed to parse users file"));
    }

    public void testParseFileWithLineWithEmptyPasswordAndWhitespace() throws Exception {
        Path file = createTempFile();
        Files.write(file, Collections.singletonList("user: "), StandardCharsets.UTF_8);
        Map<String, char[]> users = FileUserPasswdStore.parseFile(file, null, Settings.EMPTY);
        assertThat(users, notNullValue());
        assertThat(users.keySet(), is(empty()));
    }

}
