/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.operator;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.security.audit.logfile.CapturingLogger;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.junit.After;
import org.junit.Before;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Set;

public class OperatorUserDescriptorTests extends ESTestCase {

    private Settings settings;
    private Environment env;
    private ThreadPool threadPool;

    @Before
    public void init() {
        settings = Settings.builder()
            .put("resource.reload.interval.high", "100ms")
            .put("path.home", createTempDir())
            .build();
        env = TestEnvironment.newEnvironment(settings);
        threadPool = new TestThreadPool("test");
    }

    @After
    public void shutdown() throws InterruptedException {
        terminate(threadPool);
    }

    public void testFileAutoReload() throws Exception {
        Path operatorUsers = getDataPath("operator_users.yml");
        Path tmp = getOperatorUsersPath();
        Files.copy(operatorUsers, tmp, StandardCopyOption.REPLACE_EXISTING);

        try (ResourceWatcherService watcherService = new ResourceWatcherService(settings, threadPool)) {
            final OperatorUserDescriptor operatorUserDescriptor = new OperatorUserDescriptor(env, watcherService);
            final List<OperatorUserDescriptor.Group> groups = operatorUserDescriptor.getGroups();

            assertEquals(1, groups.size());
            assertEquals(new OperatorUserDescriptor.Group(Set.of("operator_1", "operator_2"),
                "file", "file", Authentication.AuthenticationType.REALM), groups.get(0));

            // Content does not change, the groups should not be updated
            try (BufferedWriter writer = Files.newBufferedWriter(tmp, StandardCharsets.UTF_8, StandardOpenOption.APPEND)) {
                writer.append("\n");
            }
            watcherService.notifyNow(ResourceWatcherService.Frequency.HIGH);
            assertSame(groups, operatorUserDescriptor.getGroups());

            // Add one more entry
            try (BufferedWriter writer = Files.newBufferedWriter(tmp, StandardCharsets.UTF_8, StandardOpenOption.APPEND)) {
                writer.append("  - usernames: [ 'operator_3' ]\n");
            }
            assertBusy(() -> {
                final List<OperatorUserDescriptor.Group> newGroups = operatorUserDescriptor.getGroups();
                assertEquals(2, newGroups.size());
                assertEquals(new OperatorUserDescriptor.Group(Set.of("operator_1", "operator_2"),
                    "file", "file", Authentication.AuthenticationType.REALM), newGroups.get(0));
                assertEquals(new OperatorUserDescriptor.Group(Set.of("operator_3")), newGroups.get(1));
            });

            // Add mal-formatted entry
            try (BufferedWriter writer = Files.newBufferedWriter(tmp, StandardCharsets.UTF_8, StandardOpenOption.APPEND)) {
                writer.append("  - blah\n");
            }
            assertBusy(() -> {
                assertEquals(0, operatorUserDescriptor.getGroups().size());
            });
        }
    }

    public void testMalFormattedOrEmptyFile() throws IOException {
        // Mal-formatted file is functionally equivalent to an empty file
        writeOperatorUsers(randomBoolean() ? "foobar" : "");
        try (ResourceWatcherService watcherService = new ResourceWatcherService(settings, threadPool)) {
            final OperatorUserDescriptor operatorUserDescriptor = new OperatorUserDescriptor(env, watcherService);
            assertEquals(0, operatorUserDescriptor.getGroups().size());
        }
    }

    public void testParseFileWhenFileDoesNotExist() throws Exception {
        Path file = createTempDir().resolve(randomAlphaOfLength(10));
        Logger logger = CapturingLogger.newCapturingLogger(Level.DEBUG, null);
        final List<OperatorUserDescriptor.Group> groups = OperatorUserDescriptor.parseFileLenient(file, logger);
        assertEquals(0, groups.size());
        List<String> events = CapturingLogger.output(logger.getName(), Level.DEBUG);
        assertEquals(1, events.size());
        assertEquals("Skip reading operator user file since it does not exist", events.get(0));
    }

    public void testParseConfig() throws IOException {
        final String config = ""
            + "operator:\n"
            + "  - usernames: [\"operator_1\",\"operator_2\"]\n"
            + "    realm_name: \"found\"\n"
            + "    realm_type: \"file\"\n"
            + "    auth_type: \"REALM\"\n"
            + "  - usernames: [\"internal_system\"]\n";

        try (ByteArrayInputStream in = new ByteArrayInputStream(config.getBytes(StandardCharsets.UTF_8))) {
            final List<OperatorUserDescriptor.Group> groups = OperatorUserDescriptor.parseConfig(in);
            assertEquals(2, groups.size());
            assertEquals(new OperatorUserDescriptor.Group(Set.of("operator_1", "operator_2"), "found"), groups.get(0));
            assertEquals(new OperatorUserDescriptor.Group(Set.of("internal_system")), groups.get(1));
        }
    }

    private Path getOperatorUsersPath() throws IOException {
        Path xpackConf = env.configFile();
        Files.createDirectories(xpackConf);
        return xpackConf.resolve("operator_users.yml");
    }

    private Path writeOperatorUsers(String input) throws IOException {
        Path file = getOperatorUsersPath();
        Files.write(file, input.getBytes(StandardCharsets.UTF_8));
        return file;
    }
}
