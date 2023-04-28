/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.Constants;
import org.elasticsearch.Version;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.plugins.Platforms;
import org.elasticsearch.plugins.PluginTestUtil;
import org.elasticsearch.test.GraalVMThreadsFilter;
import org.elasticsearch.test.MockLogAppender;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Create a simple "daemon controller", put it in the right place and check that it runs.
 *
 * Extends LuceneTestCase rather than ESTestCase as ESTestCase installs a system call filter, and
 * that prevents the Spawner class from doing its job. Also needs to run in a separate JVM to other
 * tests that extend ESTestCase for the same reason.
 */
@ThreadLeakFilters(filters = { GraalVMThreadsFilter.class })
public class SpawnerNoBootstrapTests extends LuceneTestCase {

    private static final String CONTROLLER_SOURCE = """
        #!/bin/bash

        echo I am alive
        echo "I am an error" >&2

        read SOMETHING
        """;

    static {
        // normally done by ESTestCase, but need here because spawner depends on logging
        LogConfigurator.loadLog4jPlugins();
    }

    static class ExpectedStreamMessage implements MockLogAppender.LoggingExpectation {
        final String expectedLogger;
        final String expectedMessage;
        final CountDownLatch matchCalledLatch;
        boolean saw;

        ExpectedStreamMessage(String logger, String message, CountDownLatch matchCalledLatch) {
            this.expectedLogger = logger;
            this.expectedMessage = message;
            this.matchCalledLatch = matchCalledLatch;
        }

        @Override
        public void match(LogEvent event) {
            if (event.getLoggerName().equals(expectedLogger)
                && event.getLevel().equals(Level.WARN)
                && event.getMessage().getFormattedMessage().equals(expectedMessage)) {
                saw = true;
            }
            matchCalledLatch.countDown();
        }

        @Override
        public void assertMatched() {
            assertTrue("Expected to see message [" + expectedMessage + "] on logger [" + expectedLogger + "]", saw);
        }
    }

    private MockLogAppender addMockLogger(String loggerName) throws Exception {
        MockLogAppender appender = new MockLogAppender();
        appender.start();
        final Logger testLogger = LogManager.getLogger(loggerName);
        Loggers.addAppender(testLogger, appender);
        Loggers.setLevel(testLogger, Level.TRACE);
        return appender;
    }

    private void removeMockLogger(String loggerName, MockLogAppender appender) {
        Loggers.removeAppender(LogManager.getLogger(loggerName), appender);
        appender.stop();
    }

    /**
     * Simplest case: a module with no controller daemon.
     */
    public void testNoControllerSpawn() throws IOException {
        Path esHome = createTempDir().resolve("esHome");
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(Environment.PATH_HOME_SETTING.getKey(), esHome.toString());
        Settings settings = settingsBuilder.build();

        Environment environment = TestEnvironment.newEnvironment(settings);

        // This plugin will NOT have a controller daemon
        Path plugin = environment.modulesFile().resolve("a_plugin");
        Files.createDirectories(environment.modulesFile());
        Files.createDirectories(plugin);
        PluginTestUtil.writePluginProperties(
            plugin,
            "description",
            "a_plugin",
            "version",
            Version.CURRENT.toString(),
            "elasticsearch.version",
            Version.CURRENT.toString(),
            "name",
            "a_plugin",
            "java.version",
            "1.8",
            "classname",
            "APlugin",
            "has.native.controller",
            "false"
        );

        try (Spawner spawner = new Spawner()) {
            spawner.spawnNativeControllers(environment);
            assertThat(spawner.getProcesses(), hasSize(0));
        }
    }

    /**
     * Two plugins - one with a controller daemon and one without.
     */
    public void testControllerSpawn() throws Exception {
        assertControllerSpawns(Environment::pluginsFile, false);
        assertControllerSpawns(Environment::modulesFile, true);
    }

    private void assertControllerSpawns(final Function<Environment, Path> pluginsDirFinder, boolean expectSpawn) throws Exception {
        /*
         * On Windows you can not directly run a batch file - you have to run cmd.exe with the batch
         * file as an argument and that's out of the remit of the controller daemon process spawner.
         */
        assumeFalse("This test does not work on Windows", Constants.WINDOWS);

        Path esHome = createTempDir().resolve("esHome");
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(Environment.PATH_HOME_SETTING.getKey(), esHome.toString());
        Settings settings = settingsBuilder.build();

        Environment environment = TestEnvironment.newEnvironment(settings);

        // this plugin will have a controller daemon
        Path plugin = pluginsDirFinder.apply(environment).resolve("test_plugin");
        Files.createDirectories(environment.modulesFile());
        Files.createDirectories(environment.pluginsFile());
        Files.createDirectories(plugin);
        PluginTestUtil.writePluginProperties(
            plugin,
            "description",
            "test_plugin",
            "version",
            Version.CURRENT.toString(),
            "elasticsearch.version",
            Version.CURRENT.toString(),
            "name",
            "test_plugin",
            "java.version",
            "1.8",
            "classname",
            "TestPlugin",
            "has.native.controller",
            "true"
        );
        Path controllerProgram = Platforms.nativeControllerPath(plugin);
        createControllerProgram(controllerProgram);

        // this plugin will not have a controller daemon
        Path otherPlugin = pluginsDirFinder.apply(environment).resolve("other_plugin");
        Files.createDirectories(otherPlugin);
        PluginTestUtil.writePluginProperties(
            otherPlugin,
            "description",
            "other_plugin",
            "version",
            Version.CURRENT.toString(),
            "elasticsearch.version",
            Version.CURRENT.toString(),
            "name",
            "other_plugin",
            "java.version",
            "1.8",
            "classname",
            "OtherPlugin",
            "has.native.controller",
            "false"
        );

        String stdoutLoggerName = "test_plugin-controller-stdout";
        String stderrLoggerName = "test_plugin-controller-stderr";
        MockLogAppender stdoutAppender = addMockLogger(stdoutLoggerName);
        MockLogAppender stderrAppender = addMockLogger(stderrLoggerName);
        CountDownLatch messagesLoggedLatch = new CountDownLatch(2);
        if (expectSpawn) {
            stdoutAppender.addExpectation(new ExpectedStreamMessage(stdoutLoggerName, "I am alive", messagesLoggedLatch));
            stderrAppender.addExpectation(new ExpectedStreamMessage(stderrLoggerName, "I am an error", messagesLoggedLatch));
        }

        try {
            Spawner spawner = new Spawner();
            spawner.spawnNativeControllers(environment);

            List<Process> processes = spawner.getProcesses();

            if (expectSpawn) {
                // as there should only be a reference in the list for the module that had the controller daemon, we expect one here
                assertThat(processes, hasSize(1));
                Process process = processes.get(0);
                // fail if we don't get the expected log messages within one second; usually it will be even quicker
                assertTrue(messagesLoggedLatch.await(1, TimeUnit.SECONDS));
                spawner.close();
                // fail if the process does not die within one second; usually it will be even quicker but it depends on OS scheduling
                assertTrue(process.waitFor(1, TimeUnit.SECONDS));
            } else {
                assertThat(processes, hasSize(0));
            }
            stdoutAppender.assertAllExpectationsMatched();
            stderrAppender.assertAllExpectationsMatched();
        } finally {
            removeMockLogger(stdoutLoggerName, stdoutAppender);
            removeMockLogger(stderrLoggerName, stderrAppender);
        }
    }

    public void testControllerSpawnWithIncorrectDescriptor() throws IOException {
        // this plugin will have a controller daemon
        Path esHome = createTempDir().resolve("esHome");
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(Environment.PATH_HOME_SETTING.getKey(), esHome.toString());
        Settings settings = settingsBuilder.build();

        Environment environment = TestEnvironment.newEnvironment(settings);

        Path plugin = environment.modulesFile().resolve("test_plugin");
        Files.createDirectories(plugin);
        PluginTestUtil.writePluginProperties(
            plugin,
            "description",
            "test_plugin",
            "version",
            Version.CURRENT.toString(),
            "elasticsearch.version",
            Version.CURRENT.toString(),
            "name",
            "test_plugin",
            "java.version",
            "1.8",
            "classname",
            "TestPlugin",
            "has.native.controller",
            "false"
        );
        Path controllerProgram = Platforms.nativeControllerPath(plugin);
        createControllerProgram(controllerProgram);

        Spawner spawner = new Spawner();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> spawner.spawnNativeControllers(environment));
        assertThat(e.getMessage(), equalTo("module [test_plugin] does not have permission to fork native controller"));
    }

    public void testSpawnerHandlingOfDesktopServicesStoreFiles() throws IOException {
        final Path esHome = createTempDir().resolve("home");
        final Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), esHome.toString()).build();

        final Environment environment = TestEnvironment.newEnvironment(settings);

        Files.createDirectories(environment.modulesFile());
        Files.createDirectories(environment.pluginsFile());

        final Path desktopServicesStore = environment.modulesFile().resolve(".DS_Store");
        Files.createFile(desktopServicesStore);

        final Spawner spawner = new Spawner();
        if (Constants.MAC_OS_X) {
            // if the spawner were not skipping the Desktop Services Store files on macOS this would explode
            spawner.spawnNativeControllers(environment);
        } else {
            // we do not ignore these files on non-macOS systems
            var e = expectThrows(IllegalStateException.class, () -> spawner.spawnNativeControllers(environment));
            assertThat(e.getMessage(), equalTo("Plugin [.DS_Store] is missing a descriptor properties file."));
        }
    }

    private void createControllerProgram(final Path outputFile) throws IOException {
        final Path outputDir = outputFile.getParent();
        Files.createDirectories(outputDir);
        Files.write(outputFile, CONTROLLER_SOURCE.getBytes(StandardCharsets.UTF_8));
        final PosixFileAttributeView view = Files.getFileAttributeView(outputFile, PosixFileAttributeView.class);
        if (view != null) {
            final Set<PosixFilePermission> perms = new HashSet<>();
            perms.add(PosixFilePermission.OWNER_READ);
            perms.add(PosixFilePermission.OWNER_WRITE);
            perms.add(PosixFilePermission.OWNER_EXECUTE);
            perms.add(PosixFilePermission.GROUP_READ);
            perms.add(PosixFilePermission.GROUP_EXECUTE);
            perms.add(PosixFilePermission.OTHERS_READ);
            perms.add(PosixFilePermission.OTHERS_EXECUTE);
            Files.setPosixFilePermissions(outputFile, perms);
        }
    }

}
