/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.esusers;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.audit.logfile.CapturingLogger;
import org.elasticsearch.shield.authc.RealmConfig;
import org.elasticsearch.shield.authc.support.Hasher;
import org.elasticsearch.shield.authc.support.RefreshListener;
import org.elasticsearch.shield.authc.support.SecuredStringTests;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.contains;
import static org.mockito.Mockito.*;

/**
 *
 */
public class FileUserPasswdStoreTests extends ESTestCase {

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

    @Test
    public void testStore_ConfiguredWithUnreadableFile() throws Exception {

        Path file = createTempFile();

        // writing in utf_16 should cause a parsing error as we try to read the file in utf_8
        Files.write(file, ImmutableList.of("aldlfkjldjdflkjd"), Charsets.UTF_16);

        Settings esusersSettings = Settings.builder()
                .put("files.users", file.toAbsolutePath())
                .build();

        RealmConfig config = new RealmConfig("esusers-test", esusersSettings, settings, env);
        ResourceWatcherService watcherService = new ResourceWatcherService(settings, threadPool);
        FileUserPasswdStore store = new FileUserPasswdStore(config, watcherService);
        assertThat(store.usersCount(), is(0));
    }

    @Test
    public void testStore_AutoReload() throws Exception {
        Path users = getDataPath("users");
        Path tmp = createTempFile();
        Files.copy(users, tmp, StandardCopyOption.REPLACE_EXISTING);

        Settings esusersSettings = Settings.builder()
                .put("files.users", tmp.toAbsolutePath())
                .build();


        RealmConfig config = new RealmConfig("esusers-test", esusersSettings, settings, env);
        ResourceWatcherService watcherService = new ResourceWatcherService(settings, threadPool);
        final CountDownLatch latch = new CountDownLatch(1);

        FileUserPasswdStore store = new FileUserPasswdStore(config, watcherService, new RefreshListener() {
            @Override
            public void onRefresh() {
                latch.countDown();
            }
        });

        assertThat(store.verifyPassword("bcrypt", SecuredStringTests.build("test123")), is(true));

        watcherService.start();

        try (BufferedWriter writer = Files.newBufferedWriter(tmp, Charsets.UTF_8, StandardOpenOption.APPEND)) {
            writer.newLine();
            writer.append("foobar:").append(new String(Hasher.BCRYPT.hash(SecuredStringTests.build("barfoo"))));
        }

        if (!latch.await(5, TimeUnit.SECONDS)) {
            fail("Waited too long for the updated file to be picked up");
        }

        assertThat(store.verifyPassword("foobar", SecuredStringTests.build("barfoo")), is(true));

    }

    @Test
    public void testStore_AutoReload_WithParseFailures() throws Exception {
        Path users = getDataPath("users");
        Path tmp = createTempFile();
        Files.copy(users, tmp, StandardCopyOption.REPLACE_EXISTING);

        Settings esusersSettings = Settings.builder()
                .put("files.users", tmp.toAbsolutePath())
                .build();


        RealmConfig config = new RealmConfig("esusers-test", esusersSettings, settings, env);
        ResourceWatcherService watcherService = new ResourceWatcherService(settings, threadPool);
        final CountDownLatch latch = new CountDownLatch(1);

        FileUserPasswdStore store = new FileUserPasswdStore(config, watcherService, new RefreshListener() {
            @Override
            public void onRefresh() {
                latch.countDown();
            }
        });

        assertTrue(store.verifyPassword("bcrypt", SecuredStringTests.build("test123")));

        watcherService.start();

        // now replacing the content of the users file with something that cannot be read
        Files.write(tmp, ImmutableList.of("aldlfkjldjdflkjd"), Charsets.UTF_16);

        if (!latch.await(5, TimeUnit.SECONDS)) {
            fail("Waited too long for the updated file to be picked up");
        }

        assertThat(store.usersCount(), is(0));
    }

    @Test
    public void testParseFile() throws Exception {
        Path path = getDataPath("users");
        Map<String, char[]> users = FileUserPasswdStore.parseFile(path, null);
        assertThat(users, notNullValue());
        assertThat(users.size(), is(6));
        assertThat(users.get("bcrypt"), notNullValue());
        assertThat(new String(users.get("bcrypt")), equalTo("$2a$05$zxnP0vdREMxnEpkLCDI2OuSaSk/QEKA2.A42iOpI6U2u.RLLOWm1e"));
        assertThat(users.get("bcrypt10"), notNullValue());
        assertThat(new String(users.get("bcrypt10")), equalTo("$2y$10$FMhmFjwU5.qxQ/BsEciS9OqcJVkFMgXMo4uH5CelOR1j4N9zIv67e"));
        assertThat(users.get("md5"), notNullValue());
        assertThat(new String(users.get("md5")), equalTo("$apr1$R3DdqiAZ$aljIkaIVPSarmDMlJUBBP."));
        assertThat(users.get("crypt"), notNullValue());
        assertThat(new String(users.get("crypt")), equalTo("hsP1PYSLsEEvs"));
        assertThat(users.get("plain"), notNullValue());
        assertThat(new String(users.get("plain")), equalTo("{plain}test123"));
        assertThat(users.get("sha"), notNullValue());
        assertThat(new String(users.get("sha")), equalTo("{SHA}cojt0Pw//L6ToM8G41aOKFIWh7w="));
    }

    @Test
    public void testParseFile_Empty() throws Exception {
        Path empty = createTempFile();
        ESLogger log = ESLoggerFactory.getLogger("test");
        log = spy(log);
        ImmutableMap<String, char[]> users = FileUserPasswdStore.parseFile(empty, log);
        assertThat(users.isEmpty(), is(true));
        verify(log, times(1)).warn(contains("no users found"), eq(empty));
    }

    @Test
    public void testParseFile_WhenFileDoesNotExist() throws Exception {
        Path file = createTempDir().resolve(randomAsciiOfLength(10));
        CapturingLogger logger = new CapturingLogger(CapturingLogger.Level.INFO);
        Map<String, char[]> users = FileUserPasswdStore.parseFile(file, logger);
        assertThat(users, notNullValue());
        assertThat(users.isEmpty(), is(true));
    }

    @Test
    public void testParseFile_WhenCannotReadFile() throws Exception {
        Path file = createTempFile();
        // writing in utf_16 should cause a parsing error as we try to read the file in utf_8
        Files.write(file, ImmutableList.of("aldlfkjldjdflkjd"), Charsets.UTF_16);
        CapturingLogger logger = new CapturingLogger(CapturingLogger.Level.INFO);
        try {
            FileUserPasswdStore.parseFile(file, logger);
            fail("expected a parse failure");
        } catch (IllegalStateException se) {
            this.logger.info("expected", se);
        }
    }

    @Test
    public void testParseFile_InvalidLineDoesNotResultInLoggerNPE() throws Exception {
        Path file = createTempFile();
        Files.write(file, ImmutableList.of("NotValidUsername=Password", "user:pass"), Charsets.UTF_8);
        Map<String, char[]> users = FileUserPasswdStore.parseFile(file, null);
        assertThat(users, notNullValue());
        assertThat(users.keySet(), hasSize(1));
    }

    @Test
    public void testParseFileLenient_WhenCannotReadFile() throws Exception {
        Path file = createTempFile();
        // writing in utf_16 should cause a parsing error as we try to read the file in utf_8
        Files.write(file, ImmutableList.of("aldlfkjldjdflkjd"), Charsets.UTF_16);
        CapturingLogger logger = new CapturingLogger(CapturingLogger.Level.INFO);
        Map<String, char[]> users = FileUserPasswdStore.parseFileLenient(file, logger);
        assertThat(users, notNullValue());
        assertThat(users.isEmpty(), is(true));
        List<CapturingLogger.Msg> msgs = logger.output(CapturingLogger.Level.ERROR);
        assertThat(msgs.size(), is(1));
        assertThat(msgs.get(0).text, containsString("failed to parse users file"));
    }

}
