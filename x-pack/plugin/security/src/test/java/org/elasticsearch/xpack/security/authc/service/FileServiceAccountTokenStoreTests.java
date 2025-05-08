/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
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
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FileServiceAccountTokenStoreTests extends ESTestCase {

    private Settings settings;
    private Environment env;
    private ThreadPool threadPool;
    private ClusterService clusterService;

    @Before
    public void init() {
        final String hashingAlgorithm = inFipsJvm()
            ? randomFrom("pbkdf2", "pbkdf2_50000", "pbkdf2_stretch")
            : randomFrom("bcrypt", "bcrypt10", "pbkdf2", "pbkdf2_50000", "pbkdf2_stretch");
        settings = Settings.builder()
            .put("resource.reload.interval.high", "100ms")
            .put("path.home", createTempDir())
            .put("xpack.security.authc.service_token_hashing.algorithm", hashingAlgorithm)
            .build();
        env = TestEnvironment.newEnvironment(settings);
        threadPool = new TestThreadPool("test");
        clusterService = mock(ClusterService.class);
        final DiscoveryNode discoveryNode = mock(DiscoveryNode.class);
        when(clusterService.localNode()).thenReturn(discoveryNode);
        when(discoveryNode.getName()).thenReturn("node");
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

        assertThat(
            new String(parsedTokenHashes.get("elastic/fleet-server/bcrypt")),
            equalTo("$2a$10$uuCzGHRrEz/QMB/.bmL8qOKXHhPNt57dYBbWCH/Hbb3SjUyZ.Hf1i")
        );
        assertThat(
            new String(parsedTokenHashes.get("elastic/fleet-server/bcrypt10")),
            equalTo("$2a$10$ML0BUUxdzs8ApPNf1ayAwuh61ZhfqlzN/1DgZWZn6vNiUhpu1GKTe")
        );

        assertThat(
            new String(parsedTokenHashes.get("elastic/fleet-server/pbkdf2")),
            equalTo("{PBKDF2}10000$0N2h5/AsDS5uO0/A+B6y8AnTCJ3Tqo8nygbzu1gkgpo=$5aTcCtteHf2g2ye7Y3p6jSZBoGhNJ7l6F3tmUhPTwRo=")
        );
        assertThat(
            new String(parsedTokenHashes.get("elastic/fleet-server/pbkdf2_50000")),
            equalTo("{PBKDF2}50000$IMzlphNClmrP/du40yxGM3fNjklg8CuACds12+Ry0jM=$KEC1S9a0NOs3OJKM4gEeBboU18EP4+3m/pyIA4MBDGk=")
        );
        assertThat(
            new String(parsedTokenHashes.get("elastic/fleet-server/pbkdf2_stretch")),
            equalTo("{PBKDF2_STRETCH}10000$Pa3oNkj8xTD8j2gTgjWnTvnE6jseKApWMFjcNCLxX1U=$84ECweHFZQ2DblHEjHTRWA+fG6h5bVMyTSJUmFvTo1o=")
        );

        assertThat(parsedTokenHashes.get("elastic/fleet-server/plain"), nullValue());
    }

    public void testParseFileNotExists() throws IllegalAccessException, IOException {
        Logger logger = CapturingLogger.newCapturingLogger(Level.TRACE, null);
        final List<String> events = CapturingLogger.output(logger.getName(), Level.TRACE);
        events.clear();
        final Map<String, char[]> tokenHashes = FileServiceAccountTokenStore.parseFile(
            getDataPath("service_tokens").getParent().resolve("does-not-exist"),
            logger
        );
        assertThat(tokenHashes.isEmpty(), is(true));
        assertThat(events, hasSize(2));
        assertThat(events.get(1), containsString("does not exist"));
    }

    public void testAutoReload() throws Exception {
        Path serviceTokensSourceFile = getDataPath("service_tokens");
        Path configDir = env.configDir();
        Files.createDirectories(configDir);
        Path targetFile = configDir.resolve("service_tokens");
        Files.copy(serviceTokensSourceFile, targetFile, StandardCopyOption.REPLACE_EXISTING);
        final String hashingAlgo = settings.get("xpack.security.authc.service_token_hashing.algorithm");
        final Hasher hasher = Hasher.resolve(hashingAlgo);
        try (ResourceWatcherService watcherService = new ResourceWatcherService(settings, threadPool)) {
            final AtomicInteger counter = new AtomicInteger(0);

            FileServiceAccountTokenStore store = new FileServiceAccountTokenStore(
                env,
                watcherService,
                threadPool,
                clusterService,
                mock(CacheInvalidatorRegistry.class)
            );
            store.addListener(counter::getAndIncrement);
            // Token name shares the hashing algorithm name for convenience
            final String qualifiedTokenName = "elastic/fleet-server/" + hashingAlgo;
            assertThat(store.getTokenHashes().containsKey(qualifiedTokenName), is(true));

            final int oldValue1 = counter.get();
            // A blank line should not trigger update
            try (BufferedWriter writer = Files.newBufferedWriter(targetFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)) {
                writer.append("\n");
            }
            watcherService.notifyNow(ResourceWatcherService.Frequency.HIGH);
            if (counter.get() != oldValue1) {
                fail("Listener should not be called as service tokens are not changed.");
            }
            assertThat(store.getTokenHashes().containsKey(qualifiedTokenName), is(true));

            // Add a new entry
            final int oldValue2 = counter.get();
            final char[] newTokenHash = hasher.hash(
                new SecureString("46ToAwIHZWxhc3RpYwVmbGVldAZ0b2tlbjEWWkYtQ3dlWlVTZldJX3p5Vk9ySnlSQQAAAAAAAAA".toCharArray())
            );
            try (BufferedWriter writer = Files.newBufferedWriter(targetFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)) {
                writer.newLine();
                writer.append("elastic/fleet-server/token1:").append(new String(newTokenHash));
            }
            assertBusy(() -> {
                assertThat("Waited too long for the updated file to be picked up", counter.get(), greaterThan(oldValue2));
                assertThat(store.getTokenHashes().containsKey("elastic/fleet-server/token1"), is(true));
            }, 5, TimeUnit.SECONDS);

            // Remove the new entry
            final int oldValue3 = counter.get();
            Files.copy(serviceTokensSourceFile, targetFile, StandardCopyOption.REPLACE_EXISTING);
            assertBusy(() -> {
                assertThat("Waited too long for the updated file to be picked up", counter.get(), greaterThan(oldValue3));
                assertThat(store.getTokenHashes().containsKey("elastic/fleet-server/token1"), is(false));
                assertThat(store.getTokenHashes().containsKey(qualifiedTokenName), is(true));
            }, 5, TimeUnit.SECONDS);

            // Write a mal-formatted line
            final int oldValue4 = counter.get();
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
            assertBusy(() -> {
                assertThat("Waited too long for the updated file to be picked up", counter.get(), greaterThan(oldValue4));
                assertThat(store.getTokenHashes().isEmpty(), is(true));
            }, 5, TimeUnit.SECONDS);

            // Restore to original file again
            final int oldValue5 = counter.get();
            Files.copy(serviceTokensSourceFile, targetFile, StandardCopyOption.REPLACE_EXISTING);
            assertBusy(() -> {
                assertThat("Waited too long for the updated file to be picked up", counter.get(), greaterThan(oldValue5));
                assertThat(store.getTokenHashes().containsKey(qualifiedTokenName), is(true));
            }, 5, TimeUnit.SECONDS);

            // Duplicate entry
            final int oldValue6 = counter.get();
            try (BufferedWriter writer = Files.newBufferedWriter(targetFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)) {
                writer.newLine();
                writer.append(qualifiedTokenName).append(":").append(new String(newTokenHash));
            }
            assertBusy(() -> {
                assertThat("Waited too long for the updated file to be picked up", counter.get(), greaterThan(oldValue6));
                assertThat(store.getTokenHashes().get(qualifiedTokenName), equalTo(newTokenHash));
            }, 5, TimeUnit.SECONDS);
        }
    }

    public void testFindTokensFor() throws IOException {
        Path serviceTokensSourceFile = getDataPath("service_tokens");
        Path configDir = env.configDir();
        Files.createDirectories(configDir);
        Path targetFile = configDir.resolve("service_tokens");
        Files.copy(serviceTokensSourceFile, targetFile, StandardCopyOption.REPLACE_EXISTING);
        FileServiceAccountTokenStore store = new FileServiceAccountTokenStore(
            env,
            mock(ResourceWatcherService.class),
            threadPool,
            clusterService,
            mock(CacheInvalidatorRegistry.class)
        );

        final ServiceAccountId accountId = new ServiceAccountId("elastic", "fleet-server");
        final List<TokenInfo> tokenInfos = store.findTokensFor(accountId);
        assertThat(tokenInfos, hasSize(5));
        assertThat(
            tokenInfos.stream().map(TokenInfo::getName).collect(Collectors.toUnmodifiableSet()),
            equalTo(Set.of("pbkdf2", "bcrypt10", "pbkdf2_stretch", "pbkdf2_50000", "bcrypt"))
        );
        assertThat(
            tokenInfos.stream().map(TokenInfo::getSource).collect(Collectors.toUnmodifiableSet()),
            equalTo(EnumSet.of(TokenInfo.TokenSource.FILE))
        );
        assertThat(
            tokenInfos.stream().map(TokenInfo::getNodeNames).collect(Collectors.toUnmodifiableSet()),
            equalTo(Set.of(List.of("node")))
        );
    }
}
