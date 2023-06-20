/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.operator;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xpack.core.security.audit.logfile.CapturingLogger;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.user.User;
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

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;

public class FileOperatorUsersStoreTests extends ESTestCase {

    private Settings settings;
    private Environment env;
    private ThreadPool threadPool;

    @Before
    public void init() {
        settings = Settings.builder().put("resource.reload.interval.high", "100ms").put("path.home", createTempDir()).build();
        env = TestEnvironment.newEnvironment(settings);
        threadPool = new TestThreadPool("test");
    }

    @After
    public void shutdown() {
        terminate(threadPool);
    }

    public void testIsOperator() throws IOException {
        Path sampleFile = getDataPath("operator_users.yml");
        Path inUseFile = getOperatorUsersPath();
        Files.copy(sampleFile, inUseFile, StandardCopyOption.REPLACE_EXISTING);
        final ResourceWatcherService resourceWatcherService = mock(ResourceWatcherService.class);
        final FileOperatorUsersStore fileOperatorUsersStore = new FileOperatorUsersStore(env, resourceWatcherService);

        // user operator_1 from file realm is an operator
        final Authentication.RealmRef fileRealm = new Authentication.RealmRef("file", "file", randomAlphaOfLength(8));
        final User operator_1 = new User("operator_1", randomRoles());
        assertTrue(
            fileOperatorUsersStore.isOperatorUser(
                AuthenticationTestHelper.builder().realm().user(operator_1).realmRef(fileRealm).build(false)
            )
        );

        // user operator_3 is an operator and its file realm can have any name
        final Authentication.RealmRef anotherFileRealm = new Authentication.RealmRef(
            randomAlphaOfLengthBetween(3, 8),
            "file",
            randomAlphaOfLength(8)
        );
        assertTrue(
            fileOperatorUsersStore.isOperatorUser(
                AuthenticationTestHelper.builder()
                    .realm()
                    .user(new User("operator_3", randomRoles()))
                    .realmRef(anotherFileRealm)
                    .build(false)
            )
        );

        // user operator_1 from a different realm is not an operator
        final Authentication.RealmRef differentRealm = randomFrom(
            new Authentication.RealmRef("file", randomAlphaOfLengthBetween(5, 8), randomAlphaOfLength(8)),
            new Authentication.RealmRef(randomAlphaOfLengthBetween(5, 8), "file", randomAlphaOfLength(8)),
            new Authentication.RealmRef(randomAlphaOfLengthBetween(5, 8), randomAlphaOfLengthBetween(5, 8), randomAlphaOfLength(8))
        );
        assertFalse(
            fileOperatorUsersStore.isOperatorUser(
                AuthenticationTestHelper.builder().realm().user(operator_1).realmRef(differentRealm).build(false)
            )
        );

        // user operator_1 with non realm auth type is not an operator
        assertFalse(fileOperatorUsersStore.isOperatorUser(Authentication.newRealmAuthentication(operator_1, fileRealm).token()));
    }

    public void testFileAutoReload() throws Exception {
        Path sampleFile = getDataPath("operator_users.yml");
        Path inUseFile = getOperatorUsersPath();
        Files.copy(sampleFile, inUseFile, StandardCopyOption.REPLACE_EXISTING);

        final Logger logger = LogManager.getLogger(FileOperatorUsersStore.class);
        final MockLogAppender appender = new MockLogAppender();
        appender.start();
        Loggers.addAppender(logger, appender);
        Loggers.setLevel(logger, Level.TRACE);

        try (ResourceWatcherService watcherService = new ResourceWatcherService(settings, threadPool)) {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "1st file parsing",
                    logger.getName(),
                    Level.INFO,
                    "parsed [2] group(s) with a total of [3] operator user(s) from file [" + inUseFile.toAbsolutePath() + "]"
                )
            );

            final FileOperatorUsersStore fileOperatorUsersStore = new FileOperatorUsersStore(env, watcherService);
            final List<FileOperatorUsersStore.Group> groups = fileOperatorUsersStore.getOperatorUsersDescriptor().getGroups();

            assertEquals(2, groups.size());
            assertEquals(new FileOperatorUsersStore.Group(Set.of("operator_1", "operator_2"), "file"), groups.get(0));
            assertEquals(new FileOperatorUsersStore.Group(Set.of("operator_3"), null), groups.get(1));
            appender.assertAllExpectationsMatched();

            // Content does not change, the groups should not be updated
            try (BufferedWriter writer = Files.newBufferedWriter(inUseFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)) {
                writer.append("\n");
            }
            watcherService.notifyNow(ResourceWatcherService.Frequency.HIGH);
            assertSame(groups, fileOperatorUsersStore.getOperatorUsersDescriptor().getGroups());
            appender.assertAllExpectationsMatched();

            // Add one more entry
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "updating",
                    logger.getName(),
                    Level.INFO,
                    "operator users file [" + inUseFile.toAbsolutePath() + "] changed. updating operator users"
                )
            );
            try (BufferedWriter writer = Files.newBufferedWriter(inUseFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)) {
                writer.append("  - usernames: [ 'operator_4' ]\n");
            }
            assertBusy(() -> {
                final List<FileOperatorUsersStore.Group> newGroups = fileOperatorUsersStore.getOperatorUsersDescriptor().getGroups();
                assertEquals(3, newGroups.size());
                assertEquals(new FileOperatorUsersStore.Group(Set.of("operator_4")), newGroups.get(2));
            });
            appender.assertAllExpectationsMatched();

            // Add mal-formatted entry
            appender.addExpectation(
                new MockLogAppender.ExceptionSeenEventExpectation(
                    "mal-formatted",
                    logger.getName(),
                    Level.ERROR,
                    "Failed to parse operator users file",
                    XContentParseException.class,
                    "[10:1] [operator_privileges.operator] failed to parse field [operator]"
                )
            );
            try (BufferedWriter writer = Files.newBufferedWriter(inUseFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)) {
                writer.append("  - blah\n");
            }
            watcherService.notifyNow(ResourceWatcherService.Frequency.HIGH);
            assertEquals(3, fileOperatorUsersStore.getOperatorUsersDescriptor().getGroups().size());
            appender.assertAllExpectationsMatched();

            // Delete the file will remove all the operator users
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "file not exist warning",
                    logger.getName(),
                    Level.WARN,
                    "Operator privileges [xpack.security.operator_privileges.enabled] is enabled, "
                        + "but operator user file does not exist. No user will be able to perform operator-only actions."
                )
            );
            Files.delete(inUseFile);
            assertBusy(() -> assertEquals(0, fileOperatorUsersStore.getOperatorUsersDescriptor().getGroups().size()));
            appender.assertAllExpectationsMatched();

            // Back to original content
            Files.copy(sampleFile, inUseFile, StandardCopyOption.REPLACE_EXISTING);
            assertBusy(() -> assertEquals(2, fileOperatorUsersStore.getOperatorUsersDescriptor().getGroups().size()));
        } finally {
            Loggers.removeAppender(logger, appender);
            appender.stop();
            Loggers.setLevel(logger, (Level) null);
        }
    }

    public void testMalFormattedOrEmptyFile() throws IOException {
        // Mal-formatted file is functionally equivalent to an empty file
        writeOperatorUsers(randomBoolean() ? "foobar" : "");
        try (ResourceWatcherService watcherService = new ResourceWatcherService(settings, threadPool)) {
            final ElasticsearchParseException e = expectThrows(
                ElasticsearchParseException.class,
                () -> new FileOperatorUsersStore(env, watcherService)
            );
            assertThat(e.getMessage(), containsString("Error parsing operator users file"));
        }
    }

    public void testParseFileWhenFileDoesNotExist() throws Exception {
        Path file = createTempDir().resolve(randomAlphaOfLength(10));
        Logger logger = CapturingLogger.newCapturingLogger(Level.DEBUG, null);
        final List<FileOperatorUsersStore.Group> groups = FileOperatorUsersStore.parseFile(file, logger).getGroups();
        assertEquals(0, groups.size());
        List<String> events = CapturingLogger.output(logger.getName(), Level.WARN);
        assertEquals(1, events.size());
        assertThat(events.get(0), containsString("operator user file does not exist"));
    }

    public void testParseConfig() throws IOException {
        String config = """
            operator:
              - usernames: ["operator_1"]
            """;
        try (ByteArrayInputStream in = new ByteArrayInputStream(config.getBytes(StandardCharsets.UTF_8))) {
            final List<FileOperatorUsersStore.Group> groups = FileOperatorUsersStore.parseConfig(in).getGroups();
            assertEquals(1, groups.size());
            assertEquals(new FileOperatorUsersStore.Group(Set.of("operator_1")), groups.get(0));
        }

        config = """
            operator:
              - usernames: ["operator_1","operator_2"]
                realm_name: "file1"
                realm_type: "file"
                auth_type: "realm"
              - usernames: ["internal_system"]
            """;

        try (ByteArrayInputStream in = new ByteArrayInputStream(config.getBytes(StandardCharsets.UTF_8))) {
            final List<FileOperatorUsersStore.Group> groups = FileOperatorUsersStore.parseConfig(in).getGroups();
            assertEquals(2, groups.size());
            assertEquals(new FileOperatorUsersStore.Group(Set.of("operator_1", "operator_2"), "file1"), groups.get(0));
            assertEquals(new FileOperatorUsersStore.Group(Set.of("internal_system")), groups.get(1));
        }

        config = """
            operator:
              - realm_name: "file1"
                usernames: ["internal_system"]
              - auth_type: "realm"
                usernames: ["operator_1","operator_2"]
            """;

        try (ByteArrayInputStream in = new ByteArrayInputStream(config.getBytes(StandardCharsets.UTF_8))) {
            final List<FileOperatorUsersStore.Group> groups = FileOperatorUsersStore.parseConfig(in).getGroups();
            assertEquals(2, groups.size());
            assertEquals(new FileOperatorUsersStore.Group(Set.of("internal_system"), "file1"), groups.get(0));
            assertEquals(new FileOperatorUsersStore.Group(Set.of("operator_1", "operator_2")), groups.get(1));
        }

        config = """
            operator:
              - realm_name: "file1"
                usernames: ["internal_system"]
              - realm_type: "_service_account"
                usernames: ["elastic/fleet-server"]
                auth_type: "token"
                token_source: "index"
                token_names: ["token1", "token2"]
            """;

        try (ByteArrayInputStream in = new ByteArrayInputStream(config.getBytes(StandardCharsets.UTF_8))) {
            final List<FileOperatorUsersStore.Group> groups = FileOperatorUsersStore.parseConfig(in).getGroups();
            assertEquals(2, groups.size());
            assertEquals(new FileOperatorUsersStore.Group(Set.of("internal_system"), "file1"), groups.get(0));
            assertEquals(
                new FileOperatorUsersStore.Group(
                    Set.of("elastic/fleet-server"),
                    null,
                    "_service_account",
                    "token",
                    "index",
                    List.of("token1", "token2")
                ),
                groups.get(1)
            );
        }
    }

    public void testParseInvalidConfig() throws IOException {
        String config = """
            operator:
              - usernames: ["operator_1"]
                realm_type: "native"
            """;
        try (ByteArrayInputStream in = new ByteArrayInputStream(config.getBytes(StandardCharsets.UTF_8))) {
            final XContentParseException e = expectThrows(XContentParseException.class, () -> FileOperatorUsersStore.parseConfig(in));
            assertThat(e.getCause().getCause().getMessage(), containsString("[realm_type] only supports [file]"));
        }

        config = """
            operator:
              - usernames: ["operator_1"]
                auth_type: "token"
            """;

        try (ByteArrayInputStream in = new ByteArrayInputStream(config.getBytes(StandardCharsets.UTF_8))) {
            final XContentParseException e = expectThrows(XContentParseException.class, () -> FileOperatorUsersStore.parseConfig(in));
            assertThat(e.getCause().getCause().getMessage(), containsString("[token_source] must be set when [auth_type] is [token]"));
            assertThat(e.getCause().getCause().getMessage(), containsString("[token_names] must be set when [auth_type] is [token]"));
        }

        config = """
            operator:
              - usernames: ["operator_1"]
                auth_type: "token"
                token_source: "invalid"
                token_names: ["token1", "token2"]
            """;

        try (ByteArrayInputStream in = new ByteArrayInputStream(config.getBytes(StandardCharsets.UTF_8))) {
            final XContentParseException e = expectThrows(XContentParseException.class, () -> FileOperatorUsersStore.parseConfig(in));
            assertThat(
                e.getCause().getCause().getMessage(),
                containsString("[token_source] must be one of the following values [index,file]")
            );
        }
        config = """
            operator:
                auth_type: "realm"
            """;
        try (ByteArrayInputStream in = new ByteArrayInputStream(config.getBytes(StandardCharsets.UTF_8))) {
            final XContentParseException e = expectThrows(XContentParseException.class, () -> FileOperatorUsersStore.parseConfig(in));
            assertThat(e.getCause().getMessage(), containsString("Required [usernames]"));
        }

        config = """
            operator:
              - usernames: ["operator_1"]
                realm_type: "jwt"
            """;
        try (ByteArrayInputStream in = new ByteArrayInputStream(config.getBytes(StandardCharsets.UTF_8))) {
            final XContentParseException e = expectThrows(XContentParseException.class, () -> FileOperatorUsersStore.parseConfig(in));
            assertThat(
                e.getCause().getCause().getMessage(),
                containsString("[realm_name] must be specified for realm types other than [_service_account,file,native,reserved]")
            );
            assertThat(
                e.getCause().getCause().getMessage(),
                containsString("[realm_type] only supports [file] when [auth_type] is [realm] or not specified")
            );
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

    private String[] randomRoles() {
        return randomArray(0, 3, String[]::new, () -> randomAlphaOfLengthBetween(3, 8));
    }
}
