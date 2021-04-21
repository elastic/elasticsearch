/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.security.action.service.TokenInfo;
import org.elasticsearch.xpack.core.security.audit.logfile.CapturingLogger;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.security.authc.service.ServiceAccount.ServiceAccountId;
import org.elasticsearch.xpack.security.support.CacheInvalidatorRegistry;
import org.junit.After;
import org.junit.Before;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class FileServiceAccountTokenStoreTests extends ESTestCase {

    private static Map<String, String> TOKENS = Map.of(
        "bcrypt", "46ToAwIHZWxhc3RpYwVmbGVldAZiY3J5cHQWWEU5MGVBYW9UMWlXMVctdkpmMzRxdwAAAAAAAAA",
        "bcrypt10", "46ToAwIHZWxhc3RpYwVmbGVldAhiY3J5cHQxMBY1MmVqWGxhelJCYWZMdXpHTTVoRmNnAAAAAAAAAAAAAAAAAA",
        "pbkdf2", "46ToAwIHZWxhc3RpYwVmbGVldAZwYmtkZjIWNURqUkNfWFJTQXFsNUhsYW1weXY3UQAAAAAAAAA",
        "pbkdf2_50000", "46ToAwIHZWxhc3RpYwVmbGVldAxwYmtkZjJfNTAwMDAWd24wWGZ4NUlSSHkybE9LU2N2ZndyZwAAAAAAAAAAAA",
        "pbkdf2_stretch", "46ToAwIHZWxhc3RpYwVmbGVldA5wYmtkZjJfc3RyZXRjaBZhSV8wUUxSZlJ5R0JQMVU2MFNieTJ3AAAAAAAAAA"
    );

    private Settings settings;
    private Environment env;
    private ThreadPool threadPool;

    @Before
    public void init() {
        final String hashingAlgorithm = inFipsJvm() ? randomFrom("pbkdf2", "pbkdf2_50000", "pbkdf2_stretch") :
            randomFrom("bcrypt", "bcrypt10", "pbkdf2", "pbkdf2_50000", "pbkdf2_stretch");
        settings = Settings.builder()
            .put("resource.reload.interval.high", "100ms")
            .put("path.home", createTempDir())
            .put("xpack.security.authc.service_token_hashing.algorithm", hashingAlgorithm)
            .build();
        env = TestEnvironment.newEnvironment(settings);
        threadPool = new TestThreadPool("test");
    }

    @After
    public void shutdown() {
        terminate(threadPool);
    }

    public void testParseFile() throws Exception {
        Path path = getDataPath("service_tokens");
        Map<String, char[]> parsedTokenHashes = FileServiceAccountTokenStore.parseFile(path, null);
        assertThat(parsedTokenHashes, notNullValue());
        assertThat(parsedTokenHashes.size(), is(5));

        assertThat(new String(parsedTokenHashes.get("elastic/fleet-server/bcrypt")),
            equalTo("$2a$10$uuCzGHRrEz/QMB/.bmL8qOKXHhPNt57dYBbWCH/Hbb3SjUyZ.Hf1i"));
        assertThat(new String(parsedTokenHashes.get("elastic/fleet-server/bcrypt10")),
            equalTo("$2a$10$ML0BUUxdzs8ApPNf1ayAwuh61ZhfqlzN/1DgZWZn6vNiUhpu1GKTe"));

        assertThat(new String(parsedTokenHashes.get("elastic/fleet-server/pbkdf2")),
            equalTo("{PBKDF2}10000$0N2h5/AsDS5uO0/A+B6y8AnTCJ3Tqo8nygbzu1gkgpo=$5aTcCtteHf2g2ye7Y3p6jSZBoGhNJ7l6F3tmUhPTwRo="));
        assertThat(new String(parsedTokenHashes.get("elastic/fleet-server/pbkdf2_50000")),
            equalTo("{PBKDF2}50000$IMzlphNClmrP/du40yxGM3fNjklg8CuACds12+Ry0jM=$KEC1S9a0NOs3OJKM4gEeBboU18EP4+3m/pyIA4MBDGk="));
        assertThat(new String(parsedTokenHashes.get("elastic/fleet-server/pbkdf2_stretch")),
            equalTo("{PBKDF2_STRETCH}10000$Pa3oNkj8xTD8j2gTgjWnTvnE6jseKApWMFjcNCLxX1U=$84ECweHFZQ2DblHEjHTRWA+fG6h5bVMyTSJUmFvTo1o="));

        assertThat(parsedTokenHashes.get("elastic/fleet-server/plain"), nullValue());
    }

    public void testParseFileNotExists() throws IllegalAccessException, IOException {
        Logger logger = CapturingLogger.newCapturingLogger(Level.TRACE, null);
        final Map<String, char[]> tokenHashes =
            FileServiceAccountTokenStore.parseFile(getDataPath("service_tokens").getParent().resolve("does-not-exist"), logger);
        assertThat(tokenHashes.isEmpty(), is(true));
        final List<String> events = CapturingLogger.output(logger.getName(), Level.TRACE);
        assertThat(events.size(), equalTo(2));
        assertThat(events.get(1), containsString("does not exist"));
    }

    public void testAutoReload() throws Exception {
        Path serviceTokensSourceFile = getDataPath("service_tokens");
        Path configDir = env.configFile();
        Files.createDirectories(configDir);
        Path targetFile = configDir.resolve("service_tokens");
        Files.copy(serviceTokensSourceFile, targetFile, StandardCopyOption.REPLACE_EXISTING);
        final Hasher hasher = Hasher.resolve(settings.get("xpack.security.authc.service_token_hashing.algorithm"));
        try (ResourceWatcherService watcherService = new ResourceWatcherService(settings, threadPool)) {
            final CountDownLatch latch = new CountDownLatch(5);

            FileServiceAccountTokenStore store = new FileServiceAccountTokenStore(env, watcherService, threadPool,
                mock(CacheInvalidatorRegistry.class));
            store.addListener(latch::countDown);
            //Token name shares the hashing algorithm name for convenience
            String tokenName = settings.get("xpack.security.authc.service_token_hashing.algorithm");
            final String qualifiedTokenName = "elastic/fleet-server/" + tokenName;
            assertThat(store.getTokenHashes().containsKey(qualifiedTokenName), is(true));

            // A blank line should not trigger update
            try (BufferedWriter writer = Files.newBufferedWriter(targetFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)) {
                writer.append("\n");
            }
            watcherService.notifyNow(ResourceWatcherService.Frequency.HIGH);
            if (latch.getCount() != 5) {
                fail("Listener should not be called as service tokens are not changed.");
            }
            assertThat(store.getTokenHashes().containsKey(qualifiedTokenName), is(true));

            // Add a new entry
            final char[] newTokenHash =
                hasher.hash(new SecureString("46ToAwIHZWxhc3RpYwVmbGVldAZ0b2tlbjEWWkYtQ3dlWlVTZldJX3p5Vk9ySnlSQQAAAAAAAAA".toCharArray()));
            try (BufferedWriter writer = Files.newBufferedWriter(targetFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)) {
                writer.newLine();
                writer.append("elastic/fleet-server/token1:").append(new String(newTokenHash));
            }
            assertBusy(() -> assertEquals("Waited too long for the updated file to be picked up", 4, latch.getCount()),
                5, TimeUnit.SECONDS);
            assertThat(store.getTokenHashes().containsKey("elastic/fleet-server/token1"), is(true));

            // Remove the new entry
            Files.copy(serviceTokensSourceFile, targetFile, StandardCopyOption.REPLACE_EXISTING);
            assertBusy(() -> assertEquals("Waited too long for the updated file to be picked up", 3, latch.getCount()),
                5, TimeUnit.SECONDS);
            assertThat(store.getTokenHashes().containsKey("elastic/fleet-server/token1"), is(false));
            assertThat(store.getTokenHashes().containsKey(qualifiedTokenName), is(true));

            // Write a mal-formatted line
            if (randomBoolean()) {
                try (BufferedWriter writer = Files.newBufferedWriter(targetFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)) {
                    writer.newLine();
                    writer.append("elastic/fleet-server/tokenxfoobar");
                }
            } else {
                // writing in utf_16 should cause a parsing error as we try to read the file in utf_8
                try (BufferedWriter writer = Files.newBufferedWriter(targetFile, StandardCharsets.UTF_16, StandardOpenOption.APPEND)) {
                    writer.newLine();
                    writer.append("elastic/fleet-server/tokenx:").append(new String(newTokenHash));
                }
            }
            assertBusy(() -> assertEquals("Waited too long for the updated file to be picked up", 2, latch.getCount()),
                5, TimeUnit.SECONDS);
            assertThat(store.getTokenHashes().isEmpty(), is(true));

            // Restore to original file again
            Files.copy(serviceTokensSourceFile, targetFile, StandardCopyOption.REPLACE_EXISTING);
            assertBusy(() -> assertEquals("Waited too long for the updated file to be picked up", 1, latch.getCount()),
                5, TimeUnit.SECONDS);
            assertThat(store.getTokenHashes().containsKey(qualifiedTokenName), is(true));

            // Duplicate entry
            try (BufferedWriter writer = Files.newBufferedWriter(targetFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)) {
                writer.newLine();
                writer.append(qualifiedTokenName + ":").append(new String(newTokenHash));
            }
            assertBusy(() -> assertEquals("Waited too long for the updated file to be picked up", 0, latch.getCount()),
                5, TimeUnit.SECONDS);
            assertThat(store.getTokenHashes().get(qualifiedTokenName), equalTo(newTokenHash));
        }
    }

    public void testFindTokensFor() throws IOException {
        Path serviceTokensSourceFile = getDataPath("service_tokens");
        Path configDir = env.configFile();
        Files.createDirectories(configDir);
        Path targetFile = configDir.resolve("service_tokens");
        Files.copy(serviceTokensSourceFile, targetFile, StandardCopyOption.REPLACE_EXISTING);
        FileServiceAccountTokenStore store = new FileServiceAccountTokenStore(env, mock(ResourceWatcherService.class), threadPool,
            mock(CacheInvalidatorRegistry.class));

        final ServiceAccountId accountId = new ServiceAccountId("elastic", "fleet-server");
        final PlainActionFuture<Collection<TokenInfo>> future1 = new PlainActionFuture<>();
        store.findTokensFor(accountId, future1);
        final Collection<TokenInfo> tokenInfos1 = future1.actionGet();
        assertThat(tokenInfos1.size(), equalTo(5));
    }
}
