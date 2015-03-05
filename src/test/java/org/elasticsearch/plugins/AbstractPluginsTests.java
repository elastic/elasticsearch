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

import com.carrotsearch.ant.tasks.junit4.dependencies.com.google.common.collect.Maps;
import com.carrotsearch.ant.tasks.junit4.dependencies.com.google.common.collect.Sets;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.apache.tools.zip.ZipEntry;
import org.apache.tools.zip.ZipOutputStream;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import org.junit.Test;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.settings.ImmutableSettings.Builder.EMPTY_SETTINGS;
import static org.elasticsearch.plugins.AbstractPluginsTests.ZippedFileBuilder.createZippedFile;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = Scope.SUITE, numDataNodes = 0, numClientNodes = 0, transportClientRatio = 0.0)
public abstract class AbstractPluginsTests extends ElasticsearchIntegrationTest {

    /**
     * Method to be implemented by sub classes to test different plugin installations.
     *
     * @param fqn          the fully qualified name of the plugin
     * @param zip          Zip file of the plugin
     * @param files        List of expected files to be installed
     * @param env          Environment
     * @throws Exception
     */
    protected abstract void testPlugin(String fqn, Path zip, Collection<String> files, Environment env) throws Exception;

    /**
     * Test a plugin with the following zipped content (see issue #7152):
     * <p/>
     * |
     * +-- bin/
     *      +-- tool
     */
    @Test
    public void testPluginWithBinOnly() throws Exception {
        Path zip = createZippedFile(newTempDirPath(), "plugin_with_bin_only.zip")
                .addDir("bin/")
                .addExecutableFile("bin/tool", "This is the tool file")
                .toPath();

        List<String> files = Lists.newArrayList("bin/plugin-with-bin-only/tool");

        try (InMemoryEnvironment env = new InMemoryEnvironment()) {
            testPlugin("plugin-with-bin-only", zip, files, env);
        }
    }

    /**
     * Test a plugin with the following zipped content (see issue #7152):
     * <p/>
     * |
     * +-- config/
     *      +-- plugin-with-config.yml
     */
    @Test
    public void testPluginWithConfigOnly() throws Exception {
        Path zip = createZippedFile(newTempDirPath(), "plugin_with_config_only.zip")
                .addDir("config/")
                .addFile("config/plugin-with-config.yml", "This is the config file")
                .toPath();

        List<String> files = Lists.newArrayList("config/plugin-with-config-only/plugin-with-config.yml");

        try (InMemoryEnvironment env = new InMemoryEnvironment()) {
            testPlugin("plugin-with-config-only", zip, files, env);
        }
    }

    /**
     * Test a plugin with the following zipped content :
     * <p/>
     * |
     * |-- bin/
     * |    +-- tool
     * +-- config/
     *      +-- config_file
     */
    @Test
    public void testPluginWithBinAndConfig() throws Exception {
        Path zip = createZippedFile(newTempDirPath(), "plugin_with_bin_and_config.zip")
                .addDir("bin/")
                .addFile("bin/tool", "This is the tool file")
                .addDir("config/")
                .addFile("config/config_file", "This is the config file")
                .toPath();

        List<String> files = Lists.newArrayList("bin/plugin-with-bin-and-config/tool",
                "config/plugin-with-bin-and-config/config_file");

        try (InMemoryEnvironment env = new InMemoryEnvironment()) {
            testPlugin("plugin-with-bin-and-config", zip, files, env);
        }
    }

    /**
     * Test a plugin with the following zipped content :
     * <p/>
     * |
     * +-- index.html
     */
    @Test
    public void testPluginWithoutFolders() throws Exception {
        Path zip = createZippedFile(newTempDirPath(), "plugin_without_folders.zip")
                .addFile("index.html", "Index HTML file")
                .toPath();

        List<String> files = Lists.newArrayList("plugins/plugin-without-folders/_site/index.html");

        try (InMemoryEnvironment env = new InMemoryEnvironment()) {
            testPlugin("plugin-without-folders", zip, files, env);
        }
    }

    /**
     * Test a plugin with the following zipped content :
     * <p/>
     * |
     * +-- plugin_123456/
     *       +-- index.html
     */
    @Test
    public void testPluginSingleFolder() throws Exception {
        Path zip = createZippedFile(newTempDirPath(), "plugin_single_folder.zip")
                .addDir("plugin_123456/")
                .addFile("plugin_123456/index.html", "Index HTML file")
                .toPath();

        List<String> files = Lists.newArrayList("plugins/plugin-single-folder/_site/index.html");

        try (InMemoryEnvironment env = new InMemoryEnvironment()) {
            testPlugin("plugin-single-folder", zip, files, env);
        }
    }


    /**
     * Test a plugin with the following zipped content :
     * <p/>
     * |
     * +-- archive/
     * +-- bin/
     * |    |-- cmd.sh
     * |    +-- tools/
     * |         +-- bin.sh
     * |
     * |-- config/
     * |    +-- config.yml
     * |
     * +-- plugin-sub-folder.jar
     * |
     * +-- other/
     *      |-- other.txt
     *      +-- sub/
     *           |-- sub.txt
     *           +-- dir/
     *                +-- dir.txt
     */
    @Test
    public void testPluginWithExtraFolder() throws Exception {
        Path zip = createZippedFile(newTempDirPath(), "plugin_sub_folder.zip")
                .addDir("archive/")
                .addDir("archive/bin/")
                .addExecutableFile("archive/bin/cmd.sh", "Command file")
                .addDir("archive/bin/tools/")
                .addExecutableFile("archive/bin/tools/bin.sh", "Bin file")
                .addDir("archive/config/")
                .addFile("archive/config/config.yml", "Config file")
                .addFile("archive/plugin-sub-folder.jar",
                        createZippedFile()
                                .addFile("es-plugin.properties", "version=1.0.1\n")
                                .toByteArray()
                )
                .addDir("archive/other/")
                .addFile("archive/other/other.txt", "Other file")
                .addDir("archive/other/sub/")
                .addFile("archive/other/sub/sub.txt", "Sub file")
                .addDir("archive/other/sub/dir/")
                .addFile("archive/other/sub/dir/dir.txt", "Dir file")
                .toPath();

        List<String> files = Lists.newArrayList("bin/plugin-sub-folder/cmd.sh",
                "bin/plugin-sub-folder/tools/bin.sh",
                "config/plugin-sub-folder/config.yml",
                "plugins/plugin-sub-folder/plugin-sub-folder.jar",
                "plugins/plugin-sub-folder/other/other.txt",
                "plugins/plugin-sub-folder/other/sub/sub.txt",
                "plugins/plugin-sub-folder/other/sub/dir/dir.txt");

        try (InMemoryEnvironment env = new InMemoryEnvironment()) {
            testPlugin("elasticsearch/plugin-sub-folder/1.0.1", zip, files, env);
        }
    }

    /**
     * Test a plugin with the following zipped content :
     * <p/>
     * |
     * +-- _site/
     *       +-- index.html
     */
    @Test
    public void testPluginWithSiteFolder() throws Exception {
        Path zip = createZippedFile(newTempDirPath(), "plugin_folder_site.zip")
                .addDir("_site/")
                .addFile("_site/index.html", "Index HTML file")
                .toPath();

        List<String> files = Lists.newArrayList("plugins/plugin-site-folder/_site/index.html");

        try (InMemoryEnvironment env = new InMemoryEnvironment()) {
            testPlugin("plugin-site-folder", zip, files, env);
        }
    }

    /**
     * Test a plugin with the following zipped content :
     * <p/>
     * |
     * |-- index.html
     * +-- plugin/
     *      +-- test.html
     */
    @Test
    public void testPluginWithFolderAndFile() throws Exception {
        Path zip = createZippedFile(newTempDirPath(), "plugin_folder_file.zip")
                .addFile("index.html", "Index HTML file")
                .addDir("plugin/")
                .addFile("plugin/test.html", "Test HTML file")
                .toPath();

        List<String> files = Lists.newArrayList("plugins/plugin-with-folder-and-file/_site/index.html",
                "plugins/plugin-with-folder-and-file/_site/plugin/test.html");

        try (InMemoryEnvironment env = new InMemoryEnvironment()) {
            testPlugin("plugin-with-folder-and-file", zip, files, env);
        }
    }

    /**
     * Test a plugin with the following zipped content :
     * <p/>
     * |
     * +-- README.class
     */
    @Test
    public void testPluginWithClassFile() throws Exception {
        Path zip = createZippedFile(newTempDirPath(), "plugin_with_classfile.zip")
                .addFile("README.class", "Read me class file")
                .toPath();

        List<String> files = Lists.newArrayList("plugins/plugin-with-classfile/README.class");

        try (InMemoryEnvironment env = new InMemoryEnvironment()) {
            testPlugin("plugin-with-classfile", zip, files, env);
        }
    }

    protected void assertFileExist(Path dir, String fileName) {
        assertNotNull(dir);
        assertNotNull(fileName);
        assertTrue(Files.exists(dir.resolve(fileName)));
    }

    protected void assertFileContains(Path file, String expectedContent) {
        assertNotNull(file);
        assertNotNull(expectedContent);
        try {
            assertThat(Streams.copyToString(Files.newBufferedReader(file)), equalTo(expectedContent));
        } catch (IOException e) {
            fail("Exception when checking file content: " + e.getMessage());
        }
    }

    /**
     * An in-memory environment where files are stored using JIMFS and dropped once close() is executed.
     */
    public static class InMemoryEnvironment extends Environment implements AutoCloseable {

        private final FileSystem fs;
        private final Path plugins;
        private final Path config;
        private final Path home;
        private final Path work;

        public InMemoryEnvironment() throws IOException {
            this(EMPTY_SETTINGS);
        }

        public InMemoryEnvironment(Settings settings) throws IOException {
            super(settings);
            fs = Jimfs.newFileSystem(randomFrom(Configuration.unix().toBuilder().setAttributeViews("basic", "owner", "posix", "unix").build(),
                    Configuration.osX().toBuilder().setAttributeViews("basic", "owner", "posix", "unix").build(),
                    Configuration.windows()));

            Path root = fs.getRootDirectories().iterator().next();
            home = root.resolve("in-memory").resolve("home");
            plugins = home.resolve("plugins");
            config = home.resolve("config");
            work = home.resolve("work");

            Files.createDirectories(home);
            Files.createDirectories(config);
            Files.createDirectories(work);
        }

        @Override
        public Path homeFile() {
            return home;
        }

        @Override
        public Path pluginsFile() {
            return plugins;
        }

        @Override
        public Path configFile() {
            return config;
        }

        @Override
        public Path workFile() {
            return work;
        }

        @Override
        public void close() throws Exception {
            if (fs != null) {
                try {
                    fs.close();
                } catch (Exception e) {
                }
            }
        }
    }


    /**
     * Builder class to create a Zip file that contains a given set of files and directories.
     */
    public static class ZippedFileBuilder {

        private final Path zipped;

        // Contains the file name as key, and the file content + executable flag as value.
        // Value is always null for directories
        private final Map<String, Tuple<byte[], Boolean>> files = Maps.newHashMap();

        private ZippedFileBuilder(Path zipped) {
            this.zipped = zipped;
        }

        public static ZippedFileBuilder createZippedFile() {
            return new ZippedFileBuilder(null);
        }

        public static ZippedFileBuilder createZippedFile(Path root, String zipName) {
            return new ZippedFileBuilder(root.resolve(zipName));
        }

        private void addFile(String file, byte[] content, boolean executable) {
            files.put(file, new Tuple(content, executable));
        }

        public ZippedFileBuilder addFile(String file, String content) {
            addFile(file, content.getBytes(Charsets.UTF_8), false);
            return this;
        }

        public ZippedFileBuilder addExecutableFile(String file, String content) {
            addFile(file, content.getBytes(Charsets.UTF_8), true);
            return this;
        }

        public ZippedFileBuilder addFile(String file, byte[] content) {
            addFile(file, content, false);
            return this;
        }

        public ZippedFileBuilder addDir(String dir) {
            files.put(dir, null);
            return this;
        }

        private void writeTo(OutputStream outputStream) throws IOException {
            try (ZipOutputStream zip = new ZipOutputStream(new BufferedOutputStream(outputStream))) {
                for (Map.Entry<String, Tuple<byte[], Boolean>> entry : files.entrySet()) {
                    ZipEntry ze = new ZipEntry(entry.getKey());
                    if ((entry.getValue() != null) && (entry.getValue().v2())) {
                        ze.setUnixMode(0755);
                    }
                    zip.putNextEntry(ze);
                    if (entry.getValue() != null) {
                        zip.write(entry.getValue().v1());
                    }
                    zip.closeEntry();
                }
            }
        }

        public Path toPath() throws IOException {
            try (OutputStream out = Files.newOutputStream(zipped)) {
                writeTo(out);
                return zipped;
            }
        }

        public byte[] toByteArray() throws IOException {
            try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                writeTo(out);
                return out.toByteArray();
            }
        }
    }

    /**
     * Builder class to create the directory layout of an installed plugin.
     */
    public static class PluginStructureBuilder {

        private final String name;
        private final Environment env;
        private final Path plugins;

        private Path bin;
        private Path config;
        private Path site;

        private Set<String> files = Sets.newHashSet();

        public PluginStructureBuilder(String name, Environment env) throws IOException {
            this.name = name;
            this.env = env;
            this.plugins = env.pluginsFile().resolve(name);
            Files.createDirectories(plugins);
        }

        public static PluginStructureBuilder createPluginStructure(String name, Environment env) throws IOException {
            return new PluginStructureBuilder(name, env);
        }

        private Path createDir(Path dir) throws IOException {
            Files.createDirectories(dir);
            files.add(dir.getFileSystem().toString());
            return dir;
        }

        private Path createFile(Path dir, String fileName, String content) throws IOException {
            Streams.copy(content, Files.newBufferedWriter(dir.resolve(fileName), Charsets.UTF_8));
            files.add(dir.resolve(fileName).getFileSystem().toString());
            return dir;
        }

        public PluginStructureBuilder createSiteDir() throws IOException {
            if (site == null) {
                site = createDir(plugins.resolve("_site"));
            }
            return this;
        }

        public PluginStructureBuilder addSiteFile(String siteFile, String content) throws IOException {
            createSiteDir();
            createFile(site, siteFile, content);
            return this;
        }

        public PluginStructureBuilder createBinDir() throws IOException {
            if (bin == null) {
                bin = createDir(env.homeFile().resolve("bin").resolve(name));
            }
            return this;
        }

        public PluginStructureBuilder addBinFile(String binFile, String content) throws IOException {
            createBinDir();
            createFile(bin, binFile, content);
            return this;
        }

        public PluginStructureBuilder createConfigDir() throws IOException {
            if (config == null) {
                config = createDir(env.configFile().resolve(name));
            }
            return this;
        }

        public PluginStructureBuilder addConfigFile(String configFile, String content) throws IOException {
            createConfigDir();
            createFile(config, configFile, content);
            return this;
        }

        public PluginStructureBuilder addPluginsFile(String pluginFile, String content) throws IOException {
            createFile(plugins, pluginFile, content);
            return this;
        }

        public PluginStructureBuilder addPluginsDir(String pluginDir) throws IOException {
            createDir(bin.resolve(pluginDir));
            return this;
        }

        public ZippedFileBuilder createPluginJar(String pluginJarName) throws IOException {
            return new ZippedFileBuilder(plugins.resolve(pluginJarName));
        }
    }
}
