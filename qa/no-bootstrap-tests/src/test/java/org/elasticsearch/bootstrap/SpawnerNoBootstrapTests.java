/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.bootstrap;

import org.apache.lucene.util.Constants;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.plugins.PluginTestUtil;
import org.elasticsearch.plugins.Platforms;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Create a simple "daemon controller", put it in the right place and check that it runs.
 *
 * Extends LuceneTestCase rather than ESTestCase as ESTestCase installs a system call filter, and
 * that prevents the Spawner class from doing its job. Also needs to run in a separate JVM to other
 * tests that extend ESTestCase for the same reason.
 */
public class SpawnerNoBootstrapTests extends LuceneTestCase {

    private static final String CONTROLLER_SOURCE = "#!/bin/bash\n"
            + "\n"
            + "echo I am alive\n"
            + "\n"
            + "read SOMETHING\n";

    /**
     * Simplest case: a module with no controller daemon.
     */
    public void testNoControllerSpawn() throws IOException, InterruptedException {
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
                "description", "a_plugin",
                "version", Version.CURRENT.toString(),
                "elasticsearch.version", Version.CURRENT.toString(),
                "name", "a_plugin",
                "java.version", "1.8",
                "classname", "APlugin",
                "has.native.controller", "false");

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
            "description", "test_plugin",
            "version", Version.CURRENT.toString(),
            "elasticsearch.version", Version.CURRENT.toString(),
            "name", "test_plugin",
            "java.version", "1.8",
            "classname", "TestPlugin",
            "has.native.controller", "true");
        Path controllerProgram = Platforms.nativeControllerPath(plugin);
        createControllerProgram(controllerProgram);

        // this plugin will not have a controller daemon
        Path otherPlugin = pluginsDirFinder.apply(environment).resolve("other_plugin");
        Files.createDirectories(otherPlugin);
        PluginTestUtil.writePluginProperties(
            otherPlugin,
            "description", "other_plugin",
            "version", Version.CURRENT.toString(),
            "elasticsearch.version", Version.CURRENT.toString(),
            "name", "other_plugin",
            "java.version", "1.8",
            "classname", "OtherPlugin",
            "has.native.controller", "false");

        Spawner spawner = new Spawner();
        spawner.spawnNativeControllers(environment);

        List<Process> processes = spawner.getProcesses();

        if (expectSpawn) {
             // as there should only be a reference in the list for the module that had the controller daemon, we expect one here
            assertThat(processes, hasSize(1));
            Process process = processes.get(0);
            final InputStreamReader in = new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8);
            try (BufferedReader stdoutReader = new BufferedReader(in)) {
                String line = stdoutReader.readLine();
                assertEquals("I am alive", line);
                spawner.close();
                // fail if the process does not die within one second; usually it will be even quicker but it depends on OS scheduling
                assertTrue(process.waitFor(1, TimeUnit.SECONDS));
            }
        } else {
            assertThat(processes, hasSize(0));
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
                "description", "test_plugin",
                "version", Version.CURRENT.toString(),
                "elasticsearch.version", Version.CURRENT.toString(),
                "name", "test_plugin",
                "java.version", "1.8",
                "classname", "TestPlugin",
                "has.native.controller", "false");
        Path controllerProgram = Platforms.nativeControllerPath(plugin);
        createControllerProgram(controllerProgram);

        Spawner spawner = new Spawner();
        IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> spawner.spawnNativeControllers(environment));
        assertThat(
                e.getMessage(),
                equalTo("module [test_plugin] does not have permission to fork native controller"));
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
            final FileSystemException e = expectThrows(FileSystemException.class, () -> spawner.spawnNativeControllers(environment));
            if (Constants.WINDOWS) {
                assertThat(e, instanceOf(NoSuchFileException.class));
            } else {
                assertThat(e, hasToString(containsString("Not a directory")));
            }
        }
    }

    private void createControllerProgram(final Path outputFile) throws IOException {
        final Path outputDir = outputFile.getParent();
        Files.createDirectories(outputDir);
        Files.write(outputFile, CONTROLLER_SOURCE.getBytes(StandardCharsets.UTF_8));
        final PosixFileAttributeView view =
                Files.getFileAttributeView(outputFile, PosixFileAttributeView.class);
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
