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

package org.elasticsearch.plugins;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.Version;
import org.elasticsearch.common.cli.CliToolTestCase.CaptureOutputTerminal;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.*;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static java.nio.file.attribute.PosixFilePermission.*;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.plugins.PluginInfoTests.writeProperties;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.*;

// there are some lucene file systems that seem to cause problems (deleted files, dirs instead of files)
@LuceneTestCase.SuppressFileSystems("*")
public class PluginManagerPermissionTests extends ESTestCase {

    private String pluginName = "my-plugin";
    private CaptureOutputTerminal terminal = new CaptureOutputTerminal();
    private Environment environment;
    private boolean supportsPermissions;

    @Before
    public void setup() {
        Path tempDir = createTempDir();
        Settings.Builder settingsBuilder = settingsBuilder().put("path.home", tempDir);
        if (randomBoolean()) {
            settingsBuilder.put("path.plugins", createTempDir());
        }

        if (randomBoolean()) {
            settingsBuilder.put("path.conf", createTempDir());
        }

        environment = new Environment(settingsBuilder.build());

        supportsPermissions = tempDir.getFileSystem().supportedFileAttributeViews().contains("posix");
    }

    public void testThatUnaccessibleBinDirectoryAbortsPluginInstallation() throws Exception {
        assumeTrue("File system does not support permissions, skipping", supportsPermissions);

        URL pluginUrl = createPlugin(true, randomBoolean());

        Path binPath = environment.binFile().resolve(pluginName);
        Files.createDirectories(binPath);
        try {
            Files.setPosixFilePermissions(binPath, PosixFilePermissions.fromString("---------"));

            PluginManager pluginManager = new PluginManager(environment, pluginUrl, PluginManager.OutputMode.VERBOSE, TimeValue.timeValueSeconds(10));
            pluginManager.downloadAndExtract(pluginName, terminal, true);

            fail("Expected IOException but did not happen");
        } catch (IOException e) {
            assertFileNotExists(environment.pluginsFile().resolve(pluginName));
            assertFileNotExists(environment.configFile().resolve(pluginName));
            // exists, because of our weird permissions above
            assertDirectoryExists(environment.binFile().resolve(pluginName));

            assertThat(terminal.getTerminalOutput(), hasItem(containsString("Error copying bin directory ")));
        } finally {
            Files.setPosixFilePermissions(binPath, PosixFilePermissions.fromString("rwxrwxrwx"));
        }
    }

    public void testThatUnaccessiblePluginConfigDirectoryAbortsPluginInstallation() throws Exception {
        assumeTrue("File system does not support permissions, skipping", supportsPermissions);

        URL pluginUrl = createPlugin(randomBoolean(), true);

        Path path = environment.configFile().resolve(pluginName);
        Files.createDirectories(path);
        Files.createFile(path.resolve("my-custom-config.yaml"));
        Path binPath = environment.binFile().resolve(pluginName);
        Files.createDirectories(binPath);

        try {
            Files.setPosixFilePermissions(path.resolve("my-custom-config.yaml"), PosixFilePermissions.fromString("---------"));
            Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("---------"));

            PluginManager pluginManager = new PluginManager(environment, pluginUrl, PluginManager.OutputMode.VERBOSE, TimeValue.timeValueSeconds(10));
            pluginManager.downloadAndExtract(pluginName, terminal, true);

            fail("Expected IOException but did not happen, terminal output was " + terminal.getTerminalOutput());
        } catch (IOException e) {
            assertFileNotExists(environment.pluginsFile().resolve(pluginName));
            assertFileNotExists(environment.binFile().resolve(pluginName));
            // exists, because of our weird permissions above
            assertDirectoryExists(environment.configFile().resolve(pluginName));

            assertThat(terminal.getTerminalOutput(), hasItem(containsString("Error copying config directory ")));
        } finally {
            Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rwxrwxrwx"));
            Files.setPosixFilePermissions(path.resolve("my-custom-config.yaml"), PosixFilePermissions.fromString("rwxrwxrwx"));
        }
    }

    // config/bin are not writable, but the plugin does not need to put anything into it
    public void testThatPluginWithoutBinAndConfigWorksEvenIfPermissionsAreWrong() throws Exception {
        assumeTrue("File system does not support permissions, skipping", supportsPermissions);

        URL pluginUrl = createPlugin(false, false);
        Path path = environment.configFile().resolve(pluginName);
        Files.createDirectories(path);
        Files.createFile(path.resolve("my-custom-config.yaml"));
        Path binPath = environment.binFile().resolve(pluginName);
        Files.createDirectories(binPath);

        try {
            Files.setPosixFilePermissions(path.resolve("my-custom-config.yaml"), PosixFilePermissions.fromString("---------"));
            Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("---------"));
            Files.setPosixFilePermissions(binPath, PosixFilePermissions.fromString("---------"));

            PluginManager pluginManager = new PluginManager(environment, pluginUrl, PluginManager.OutputMode.VERBOSE, TimeValue.timeValueSeconds(10));
            pluginManager.downloadAndExtract(pluginName, terminal, true);
        } finally {
            Files.setPosixFilePermissions(binPath, PosixFilePermissions.fromString("rwxrwxrwx"));
            Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rwxrwxrwx"));
            Files.setPosixFilePermissions(path.resolve("my-custom-config.yaml"), PosixFilePermissions.fromString("rwxrwxrwx"));
        }

    }

    // plugins directory no accessible, should leave no other left over directories
    public void testThatNonWritablePluginsDirectoryLeavesNoLeftOver() throws Exception {
        assumeTrue("File system does not support permissions, skipping", supportsPermissions);

        URL pluginUrl = createPlugin(true, true);
        Files.createDirectories(environment.pluginsFile());

        try {
            Files.setPosixFilePermissions(environment.pluginsFile(), PosixFilePermissions.fromString("---------"));
            PluginManager pluginManager = new PluginManager(environment, pluginUrl, PluginManager.OutputMode.VERBOSE, TimeValue.timeValueSeconds(10));
            try {
                pluginManager.downloadAndExtract(pluginName, terminal, true);
                fail("Expected IOException due to read-only plugins/ directory");
            } catch (IOException e) {
                assertFileNotExists(environment.binFile().resolve(pluginName));
                assertFileNotExists(environment.configFile().resolve(pluginName));

                Files.setPosixFilePermissions(environment.pluginsFile(), PosixFilePermissions.fromString("rwxrwxrwx"));
                assertDirectoryExists(environment.pluginsFile());
                assertFileNotExists(environment.pluginsFile().resolve(pluginName));
            }
        } finally {
            Files.setPosixFilePermissions(environment.pluginsFile(), PosixFilePermissions.fromString("rwxrwxrwx"));
        }
    }

    public void testThatUnwriteableBackupFilesInConfigurationDirectoryAreReplaced() throws Exception {
        assumeTrue("File system does not support permissions, skipping", supportsPermissions);

        boolean pluginContainsExecutables = randomBoolean();
        URL pluginUrl = createPlugin(pluginContainsExecutables, true);
        Files.createDirectories(environment.configFile().resolve(pluginName));

        Path configFile = environment.configFile().resolve(pluginName).resolve("my-custom-config.yaml");
        Files.createFile(configFile);
        Path backupConfigFile = environment.configFile().resolve(pluginName).resolve("my-custom-config.yaml.new");
        Files.createFile(backupConfigFile);
        Files.write(backupConfigFile, "foo".getBytes(Charset.forName("UTF-8")));

        PluginManager pluginManager = new PluginManager(environment, pluginUrl, PluginManager.OutputMode.VERBOSE, TimeValue.timeValueSeconds(10));
        try {
            Files.setPosixFilePermissions(backupConfigFile, PosixFilePermissions.fromString("---------"));

            pluginManager.downloadAndExtract(pluginName, terminal, true);

            if (pluginContainsExecutables) {
                assertDirectoryExists(environment.binFile().resolve(pluginName));
            }
            assertDirectoryExists(environment.pluginsFile().resolve(pluginName));
            assertDirectoryExists(environment.configFile().resolve(pluginName));

            assertFileExists(backupConfigFile);
            Files.setPosixFilePermissions(backupConfigFile, PosixFilePermissions.fromString("rw-rw-rw-"));
            String content = new String(Files.readAllBytes(backupConfigFile), Charset.forName("UTF-8"));
            assertThat(content, is(not("foo")));
        } finally {
            Files.setPosixFilePermissions(backupConfigFile, PosixFilePermissions.fromString("rw-rw-rw-"));
        }
    }

    public void testThatConfigDirectoryBeingAFileAbortsInstallationAndDoesNotAccidentallyDeleteThisFile() throws Exception {
        assumeTrue("File system does not support permissions, skipping", supportsPermissions);

        Files.createDirectories(environment.configFile());
        Files.createFile(environment.configFile().resolve(pluginName));
        URL pluginUrl = createPlugin(randomBoolean(), true);

        PluginManager pluginManager = new PluginManager(environment, pluginUrl, PluginManager.OutputMode.VERBOSE, TimeValue.timeValueSeconds(10));

        try {
            pluginManager.downloadAndExtract(pluginName, terminal, true);
            fail("Expected plugin installation to fail, but didnt");
        } catch (IOException e) {
            assertFileExists(environment.configFile().resolve(pluginName));
            assertFileNotExists(environment.binFile().resolve(pluginName));
            assertFileNotExists(environment.pluginsFile().resolve(pluginName));
        }
    }

    public void testThatBinDirectoryBeingAFileAbortsInstallationAndDoesNotAccidentallyDeleteThisFile() throws Exception {
        assumeTrue("File system does not support permissions, skipping", supportsPermissions);

        Files.createDirectories(environment.binFile());
        Files.createFile(environment.binFile().resolve(pluginName));
        URL pluginUrl = createPlugin(true, randomBoolean());

        PluginManager pluginManager = new PluginManager(environment, pluginUrl, PluginManager.OutputMode.VERBOSE, TimeValue.timeValueSeconds(10));

        try {
            pluginManager.downloadAndExtract(pluginName, terminal, true);
            fail("Expected plugin installation to fail, but didnt");
        } catch (IOException e) {
            assertFileExists(environment.binFile().resolve(pluginName));
            assertFileNotExists(environment.configFile().resolve(pluginName));
            assertFileNotExists(environment.pluginsFile().resolve(pluginName));
        }
    }

    public void testConfigDirectoryOwnerGroupAndPermissions() throws IOException {
        assumeTrue("File system does not support permissions, skipping", supportsPermissions);
        URL pluginUrl = createPlugin(false, true);
        PluginManager pluginManager = new PluginManager(environment, pluginUrl, PluginManager.OutputMode.VERBOSE, TimeValue.timeValueSeconds(10));
        pluginManager.downloadAndExtract(pluginName, terminal, true);
        PosixFileAttributes parentFileAttributes = Files.getFileAttributeView(environment.configFile(), PosixFileAttributeView.class).readAttributes();
        Path configPath = environment.configFile().resolve(pluginName);
        PosixFileAttributes pluginConfigDirAttributes = Files.getFileAttributeView(configPath, PosixFileAttributeView.class).readAttributes();
        assertThat(pluginConfigDirAttributes.owner(), equalTo(parentFileAttributes.owner()));
        assertThat(pluginConfigDirAttributes.group(), equalTo(parentFileAttributes.group()));
        assertThat(pluginConfigDirAttributes.permissions(), equalTo(parentFileAttributes.permissions()));
        Path configFile = configPath.resolve("my-custom-config.yaml");
        PosixFileAttributes pluginConfigFileAttributes = Files.getFileAttributeView(configFile, PosixFileAttributeView.class).readAttributes();
        assertThat(pluginConfigFileAttributes.owner(), equalTo(parentFileAttributes.owner()));
        assertThat(pluginConfigFileAttributes.group(), equalTo(parentFileAttributes.group()));
        Set<PosixFilePermission> expectedFilePermissions = new HashSet<>();
        for (PosixFilePermission parentPermission : parentFileAttributes.permissions()) {
            switch(parentPermission) {
                case OWNER_EXECUTE:
                case GROUP_EXECUTE:
                case OTHERS_EXECUTE:
                    break;
                default:
                    expectedFilePermissions.add(parentPermission);
            }
        }
        assertThat(pluginConfigFileAttributes.permissions(), equalTo(expectedFilePermissions));
    }

    public void testBinDirectoryOwnerGroupAndPermissions() throws IOException {
        assumeTrue("File system does not support permissions, skipping", supportsPermissions);
        URL pluginUrl = createPlugin(true, false);
        PluginManager pluginManager = new PluginManager(environment, pluginUrl, PluginManager.OutputMode.VERBOSE, TimeValue.timeValueSeconds(10));
        pluginManager.downloadAndExtract(pluginName, terminal, true);
        PosixFileAttributes parentFileAttributes = Files.getFileAttributeView(environment.binFile(), PosixFileAttributeView.class).readAttributes();
        Path binPath = environment.binFile().resolve(pluginName);
        PosixFileAttributes pluginBinDirAttributes = Files.getFileAttributeView(binPath, PosixFileAttributeView.class).readAttributes();
        assertThat(pluginBinDirAttributes.owner(), equalTo(parentFileAttributes.owner()));
        assertThat(pluginBinDirAttributes.group(), equalTo(parentFileAttributes.group()));
        assertThat(pluginBinDirAttributes.permissions(), equalTo(parentFileAttributes.permissions()));
        Path executableFile = binPath.resolve("my-binary");
        PosixFileAttributes pluginExecutableFileAttributes = Files.getFileAttributeView(executableFile, PosixFileAttributeView.class).readAttributes();
        assertThat(pluginExecutableFileAttributes.owner(), equalTo(parentFileAttributes.owner()));
        assertThat(pluginExecutableFileAttributes.group(), equalTo(parentFileAttributes.group()));
        Set<PosixFilePermission> expectedFilePermissions = new HashSet<>();
        expectedFilePermissions.add(OWNER_EXECUTE);
        expectedFilePermissions.add(GROUP_EXECUTE);
        expectedFilePermissions.add(OTHERS_EXECUTE);
        for (PosixFilePermission parentPermission : parentFileAttributes.permissions()) {
            switch(parentPermission) {
                case OWNER_EXECUTE:
                case GROUP_EXECUTE:
                case OTHERS_EXECUTE:
                    break;
                default:
                    expectedFilePermissions.add(parentPermission);
            }
        }

        assertThat(pluginExecutableFileAttributes.permissions(), equalTo(expectedFilePermissions));
    }

    private URL createPlugin(boolean withBinDir, boolean withConfigDir) throws IOException {
        final Path structure = createTempDir().resolve("fake-plugin");
        writeProperties(structure, "description", "fake desc",
                "version", "1.0",
                "elasticsearch.version", Version.CURRENT.toString(),
                "jvm", "true",
                "java.version", "1.7",
                "name", pluginName,
                "classname", pluginName);
        if (withBinDir) {
            // create bin dir
            Path binDir = structure.resolve("bin");
            Files.createDirectory(binDir);
            Files.setPosixFilePermissions(binDir, PosixFilePermissions.fromString("rwxr-xr-x"));

            // create executable
            Path executable = binDir.resolve("my-binary");
            Files.createFile(executable);
            Files.setPosixFilePermissions(executable, PosixFilePermissions.fromString("rw-r--r--"));
        }
        if (withConfigDir) {
            // create bin dir
            Path configDir = structure.resolve("config");
            Files.createDirectory(configDir);
            Files.setPosixFilePermissions(configDir, PosixFilePermissions.fromString("rwxr-xr-x"));

            // create config file
            Path configFile = configDir.resolve("my-custom-config.yaml");
            Files.createFile(configFile);
            Files.write(configFile, "my custom config content".getBytes(Charset.forName("UTF-8")));
            Files.setPosixFilePermissions(configFile, PosixFilePermissions.fromString("rw-r--r--"));
        }

        Path zip = createTempDir().resolve(structure.getFileName() + ".zip");
        try (ZipOutputStream stream = new ZipOutputStream(Files.newOutputStream(zip))) {
            Files.walkFileTree(structure, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    stream.putNextEntry(new ZipEntry(structure.relativize(file).toString()));
                    Files.copy(file, stream);
                    return FileVisitResult.CONTINUE;
                }
            });
        }
        return zip.toUri().toURL();
    }
}
