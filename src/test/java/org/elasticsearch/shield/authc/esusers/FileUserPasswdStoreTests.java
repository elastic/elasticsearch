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
import org.elasticsearch.shield.authc.support.Hasher;
import org.elasticsearch.shield.authz.Permission;
import org.elasticsearch.shield.authz.store.FileRolesStore;
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
public class FileUserPasswdStoreTests extends ElasticsearchTestCase {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testParseFile() throws Exception {
        Path path = Paths.get(getClass().getResource("users").toURI());
        Map<String, char[]> users = FileUserPasswdStore.parseFile(path, null);
        assertThat(users, notNullValue());
        assertThat(users.size(), is(6));
        assertThat(users.get("bcrypt"), notNullValue());
        assertThat(new String(users.get("bcrypt")), equalTo("$2y$05$zxnP0vdREMxnEpkLCDI2OuSaSk/QEKA2.A42iOpI6U2u.RLLOWm1e"));
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
    public void testAutoReload() throws Exception {
        ThreadPool threadPool = null;
        ResourceWatcherService watcherService = null;
        try {
            Path users = Paths.get(getClass().getResource("users").toURI());
            Path tmp = Files.createTempFile(null, null);
            Files.copy(users, Files.newOutputStream(tmp));

            Settings settings = ImmutableSettings.builder()
                    .put("watcher.interval", "2s")
                    .put("shield.authc.esusers.files.users", tmp.toAbsolutePath())
                    .build();

            Environment env = new Environment(settings);
            threadPool = new ThreadPool("test");
            watcherService = new ResourceWatcherService(settings, threadPool);
            final CountDownLatch latch = new CountDownLatch(1);
            FileUserPasswdStore store = new FileUserPasswdStore(settings, env, watcherService, new FileUserPasswdStore.Listener() {
                @Override
                public void onRefresh() {
                    latch.countDown();
                }
            });

            assertTrue(store.verifyPassword("bcrypt", "test123".toCharArray()));

            watcherService.start();

            try (BufferedWriter writer = Files.newBufferedWriter(tmp, Charsets.UTF_8, StandardOpenOption.APPEND)) {
                writer.newLine();
                writer.append("foobar:" + new String(Hasher.HTPASSWD.hash("barfoo".toCharArray())));
            }

            if (!latch.await(5, TimeUnit.SECONDS)) {
                fail("Waited too long for the updated file to be picked up");
            }

            assertTrue(store.verifyPassword("foobar", "barfoo".toCharArray()));

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
    public void testThatInvalidLineDoesNotResultInLoggerNPE() throws Exception {
        File file = tempFolder.newFile();
        com.google.common.io.Files.write("NotValidUsername=Password\nuser:pass".getBytes(org.elasticsearch.common.base.Charsets.UTF_8), file);
        Map<String, char[]> users = FileUserPasswdStore.parseFile(file.toPath(), null);
        assertThat(users, notNullValue());
        assertThat(users.keySet(), hasSize(1));
    }
}
