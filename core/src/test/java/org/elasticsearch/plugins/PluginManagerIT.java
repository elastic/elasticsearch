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

import java.nio.charset.StandardCharsets;
import com.google.common.hash.Hashing;

import org.apache.http.impl.client.HttpClients;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.Version;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.cli.CliTool;
import org.elasticsearch.common.cli.CliTool.ExitStatus;
import org.elasticsearch.common.cli.CliToolTestCase.CaptureOutputTerminal;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.junit.annotations.Network;
import org.elasticsearch.test.rest.client.http.HttpRequestBuilder;
import org.elasticsearch.test.rest.client.http.HttpResponse;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.http.*;
import org.jboss.netty.handler.ssl.SslContext;
import org.jboss.netty.handler.ssl.SslHandler;
import org.jboss.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.jboss.netty.handler.ssl.util.SelfSignedCertificate;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.elasticsearch.common.cli.CliTool.ExitStatus.USAGE;
import static org.elasticsearch.common.cli.CliToolTestCase.args;
import static org.elasticsearch.common.io.FileSystemUtilsTests.assertFileContent;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.plugins.PluginInfoTests.writeProperties;
import static org.elasticsearch.test.ESIntegTestCase.Scope;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0, transportClientRatio = 0.0)
@LuceneTestCase.SuppressFileSystems("*") // TODO: clean up this test to allow extra files
// TODO: jimfs is really broken here (throws wrong exception from detection method).
// if its in your classpath, then do not use plugins!!!!!!
public class PluginManagerIT extends ESIntegTestCase {

    private Environment environment;
    private CaptureOutputTerminal terminal = new CaptureOutputTerminal();

    @Before
    public void setup() throws Exception {
        environment = buildInitialSettings();
        System.setProperty("es.default.path.home", environment.settings().get("path.home"));
        Path binDir = environment.binFile();
        if (!Files.exists(binDir)) {
            Files.createDirectories(binDir);
        }
        Path configDir = environment.configFile();
        if (!Files.exists(configDir)) {
            Files.createDirectories(configDir);
        }
    }

    @After
    public void clearPathHome() {
        System.clearProperty("es.default.path.home");
    }

    private void writeSha1(Path file, boolean corrupt) throws IOException {
        String sha1Hex = Hashing.sha1().hashBytes(Files.readAllBytes(file)).toString();
        try (BufferedWriter out = Files.newBufferedWriter(file.resolveSibling(file.getFileName() + ".sha1"), StandardCharsets.UTF_8)) {
            out.write(sha1Hex);
            if (corrupt) {
                out.write("bad");
            }
        }
    }

    private void writeMd5(Path file, boolean corrupt) throws IOException {
        String md5Hex = Hashing.md5().hashBytes(Files.readAllBytes(file)).toString();
        try (BufferedWriter out = Files.newBufferedWriter(file.resolveSibling(file.getFileName() + ".md5"), StandardCharsets.UTF_8)) {
            out.write(md5Hex);
            if (corrupt) {
                out.write("bad");
            }
        }
    }

    /** creates a plugin .zip and returns the url for testing */
    private String createPlugin(final Path structure, String... properties) throws IOException {
        writeProperties(structure, properties);
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
        if (randomBoolean()) {
            writeSha1(zip, false);
        } else if (randomBoolean()) {
            writeMd5(zip, false);
        }
        return zip.toUri().toURL().toString();
    }

    /** creates a plugin .zip and bad checksum file and returns the url for testing */
    private String createPluginWithBadChecksum(final Path structure, String... properties) throws IOException {
        writeProperties(structure, properties);
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
        if (randomBoolean()) {
            writeSha1(zip, true);
        } else {
            writeMd5(zip, true);
        }
        return zip.toUri().toURL().toString();
    }

    public void testThatPluginNameMustBeSupplied() throws IOException {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        String pluginUrl = createPlugin(pluginDir,
            "description", "fake desc",
            "name", "fake-plugin",
            "version", "1.0",
            "elasticsearch.version", Version.CURRENT.toString(),
            "java.version", System.getProperty("java.specification.version"),
            "jvm", "true",
            "classname", "FakePlugin");
        assertStatus("install", USAGE);
    }

    public void testLocalPluginInstallWithBinAndConfig() throws Exception {
        String pluginName = "fake-plugin";
        Path pluginDir = createTempDir().resolve(pluginName);
        // create bin/tool and config/file
        Files.createDirectories(pluginDir.resolve("bin"));
        Files.createFile(pluginDir.resolve("bin").resolve("tool"));
        Files.createDirectories(pluginDir.resolve("config"));
        Files.createFile(pluginDir.resolve("config").resolve("file"));

        String pluginUrl = createPlugin(pluginDir,
            "description", "fake desc",
            "name", pluginName,
            "version", "1.0",
            "elasticsearch.version", Version.CURRENT.toString(),
            "java.version", System.getProperty("java.specification.version"),
            "jvm", "true",
            "classname", "FakePlugin");

        Path binDir = environment.binFile();
        Path pluginBinDir = binDir.resolve(pluginName);

        Path pluginConfigDir = environment.configFile().resolve(pluginName);
        assertStatusOk("install " + pluginUrl + " --verbose");

        terminal.getTerminalOutput().clear();
        assertStatusOk("list");
        assertThat(terminal.getTerminalOutput(), hasItem(containsString(pluginName)));

        assertDirectoryExists(pluginBinDir);
        assertDirectoryExists(pluginConfigDir);
        Path toolFile = pluginBinDir.resolve("tool");
        assertFileExists(toolFile);

        // check that the file is marked executable, without actually checking that we can execute it.
        PosixFileAttributeView view = Files.getFileAttributeView(toolFile, PosixFileAttributeView.class);
        // the view might be null, on e.g. windows, there is nothing to check there!
        if (view != null) {
            PosixFileAttributes attributes = view.readAttributes();
            assertThat(attributes.permissions(), hasItem(PosixFilePermission.OWNER_EXECUTE));
            assertThat(attributes.permissions(), hasItem(PosixFilePermission.OWNER_READ));
        }
    }

    /**
     * Test for #7890
     */
    public void testLocalPluginInstallWithBinAndConfigInAlreadyExistingConfigDir_7890() throws Exception {
        String pluginName = "fake-plugin";
        Path pluginDir = createTempDir().resolve(pluginName);
        // create config/test.txt with contents 'version1'
        Files.createDirectories(pluginDir.resolve("config"));
        Files.write(pluginDir.resolve("config").resolve("test.txt"), "version1".getBytes(StandardCharsets.UTF_8));

        String pluginUrl = createPlugin(pluginDir,
            "description", "fake desc",
            "name", pluginName,
            "version", "1.0",
            "elasticsearch.version", Version.CURRENT.toString(),
            "java.version", System.getProperty("java.specification.version"),
            "jvm", "true",
            "classname", "FakePlugin");

        Path pluginConfigDir = environment.configFile().resolve(pluginName);

        assertStatusOk(String.format(Locale.ROOT, "install %s --verbose", pluginUrl));

        /*
        First time, our plugin contains:
        - config/test.txt (version1)
         */
        assertFileContent(pluginConfigDir, "test.txt", "version1");

        // We now remove the plugin
        assertStatusOk("remove " + pluginName);

        // We should still have test.txt
        assertFileContent(pluginConfigDir, "test.txt", "version1");

        // Installing a new plugin version
        /*
        Second time, our plugin contains:
        - config/test.txt (version2)
        - config/dir/testdir.txt (version1)
        - config/dir/subdir/testsubdir.txt (version1)
         */
        Files.write(pluginDir.resolve("config").resolve("test.txt"), "version2".getBytes(StandardCharsets.UTF_8));
        Files.createDirectories(pluginDir.resolve("config").resolve("dir").resolve("subdir"));
        Files.write(pluginDir.resolve("config").resolve("dir").resolve("testdir.txt"), "version1".getBytes(StandardCharsets.UTF_8));
        Files.write(pluginDir.resolve("config").resolve("dir").resolve("subdir").resolve("testsubdir.txt"), "version1".getBytes(StandardCharsets.UTF_8));
        pluginUrl = createPlugin(pluginDir,
                "description", "fake desc",
                "name", pluginName,
                "version", "2.0",
                "elasticsearch.version", Version.CURRENT.toString(),
                "java.version", System.getProperty("java.specification.version"),
                "jvm", "true",
                "classname", "FakePlugin");

        assertStatusOk(String.format(Locale.ROOT, "install %s --verbose", pluginUrl));

        assertFileContent(pluginConfigDir, "test.txt", "version1");
        assertFileContent(pluginConfigDir, "test.txt.new", "version2");
        assertFileContent(pluginConfigDir, "dir/testdir.txt", "version1");
        assertFileContent(pluginConfigDir, "dir/subdir/testsubdir.txt", "version1");

        // Removing
        assertStatusOk("remove " + pluginName);
        assertFileContent(pluginConfigDir, "test.txt", "version1");
        assertFileContent(pluginConfigDir, "test.txt.new", "version2");
        assertFileContent(pluginConfigDir, "dir/testdir.txt", "version1");
        assertFileContent(pluginConfigDir, "dir/subdir/testsubdir.txt", "version1");

        // Installing a new plugin version
        /*
        Third time, our plugin contains:
        - config/test.txt (version3)
        - config/test2.txt (version1)
        - config/dir/testdir.txt (version2)
        - config/dir/testdir2.txt (version1)
        - config/dir/subdir/testsubdir.txt (version2)
         */
        Files.write(pluginDir.resolve("config").resolve("test.txt"), "version3".getBytes(StandardCharsets.UTF_8));
        Files.write(pluginDir.resolve("config").resolve("test2.txt"), "version1".getBytes(StandardCharsets.UTF_8));
        Files.write(pluginDir.resolve("config").resolve("dir").resolve("testdir.txt"), "version2".getBytes(StandardCharsets.UTF_8));
        Files.write(pluginDir.resolve("config").resolve("dir").resolve("testdir2.txt"), "version1".getBytes(StandardCharsets.UTF_8));
        Files.write(pluginDir.resolve("config").resolve("dir").resolve("subdir").resolve("testsubdir.txt"), "version2".getBytes(StandardCharsets.UTF_8));
        pluginUrl = createPlugin(pluginDir,
                "description", "fake desc",
                "name", pluginName,
                "version", "3.0",
                "elasticsearch.version", Version.CURRENT.toString(),
                "java.version", System.getProperty("java.specification.version"),
                "jvm", "true",
                "classname", "FakePlugin");

        assertStatusOk(String.format(Locale.ROOT, "install %s --verbose", pluginUrl));

        assertFileContent(pluginConfigDir, "test.txt", "version1");
        assertFileContent(pluginConfigDir, "test2.txt", "version1");
        assertFileContent(pluginConfigDir, "test.txt.new", "version3");
        assertFileContent(pluginConfigDir, "dir/testdir.txt", "version1");
        assertFileContent(pluginConfigDir, "dir/testdir.txt.new", "version2");
        assertFileContent(pluginConfigDir, "dir/testdir2.txt", "version1");
        assertFileContent(pluginConfigDir, "dir/subdir/testsubdir.txt", "version1");
        assertFileContent(pluginConfigDir, "dir/subdir/testsubdir.txt.new", "version2");
    }

    // For #7152
    public void testLocalPluginInstallWithBinOnly_7152() throws Exception {
        String pluginName = "fake-plugin";
        Path pluginDir = createTempDir().resolve(pluginName);
        // create bin/tool
        Files.createDirectories(pluginDir.resolve("bin"));
        Files.createFile(pluginDir.resolve("bin").resolve("tool"));;
        String pluginUrl = createPlugin(pluginDir,
            "description", "fake desc",
            "name", "fake-plugin",
            "version", "1.0",
            "elasticsearch.version", Version.CURRENT.toString(),
            "java.version", System.getProperty("java.specification.version"),
            "jvm", "true",
            "classname", "FakePlugin");

        Path binDir = environment.binFile();
        Path pluginBinDir = binDir.resolve(pluginName);

        assertStatusOk(String.format(Locale.ROOT, "install %s --verbose", pluginUrl));
        assertThatPluginIsListed(pluginName);
        assertDirectoryExists(pluginBinDir);
    }

    public void testListInstalledEmpty() throws IOException {
        assertStatusOk("list");
        assertThat(terminal.getTerminalOutput(), hasItem(containsString("No plugin detected")));
    }

    public void testListInstalledEmptyWithExistingPluginDirectory() throws IOException {
        Files.createDirectory(environment.pluginsFile());
        assertStatusOk("list");
        assertThat(terminal.getTerminalOutput(), hasItem(containsString("No plugin detected")));
    }

    public void testInstallPluginVerbose() throws IOException {
        String pluginName = "fake-plugin";
        Path pluginDir = createTempDir().resolve(pluginName);
        String pluginUrl = createPlugin(pluginDir,
            "description", "fake desc",
            "name", pluginName,
            "version", "1.0",
            "elasticsearch.version", Version.CURRENT.toString(),
            "java.version", System.getProperty("java.specification.version"),
            "jvm", "true",
            "classname", "FakePlugin");
        System.err.println("install " + pluginUrl + " --verbose");
        ExitStatus status = new PluginManagerCliParser(terminal).execute(args("install " + pluginUrl + " --verbose"));
        assertThat("Terminal output was: " + terminal.getTerminalOutput(), status, is(ExitStatus.OK));
        assertThat(terminal.getTerminalOutput(), hasItem(containsString("Name: fake-plugin")));
        assertThat(terminal.getTerminalOutput(), hasItem(containsString("Description: fake desc")));
        assertThat(terminal.getTerminalOutput(), hasItem(containsString("Site: false")));
        assertThat(terminal.getTerminalOutput(), hasItem(containsString("Version: 1.0")));
        assertThat(terminal.getTerminalOutput(), hasItem(containsString("JVM: true")));
        assertThatPluginIsListed(pluginName);
    }

    public void testInstallPlugin() throws IOException {
        String pluginName = "fake-plugin";
        Path pluginDir = createTempDir().resolve(pluginName);
        String pluginUrl = createPlugin(pluginDir,
            "description", "fake desc",
            "name", pluginName,
            "version", "1.0",
            "elasticsearch.version", Version.CURRENT.toString(),
            "java.version", System.getProperty("java.specification.version"),
            "jvm", "true",
            "classname", "FakePlugin");
        ExitStatus status = new PluginManagerCliParser(terminal).execute(args("install " + pluginUrl));
        assertThat("Terminal output was: " + terminal.getTerminalOutput(), status, is(ExitStatus.OK));
        assertThat(terminal.getTerminalOutput(), not(hasItem(containsString("Name: fake-plugin"))));
        assertThat(terminal.getTerminalOutput(), not(hasItem(containsString("Description:"))));
        assertThat(terminal.getTerminalOutput(), not(hasItem(containsString("Site:"))));
        assertThat(terminal.getTerminalOutput(), not(hasItem(containsString("Version:"))));
        assertThat(terminal.getTerminalOutput(), not(hasItem(containsString("JVM:"))));
        assertThatPluginIsListed(pluginName);
    }

    public void testInstallSitePluginVerbose() throws IOException {
        String pluginName = "fake-plugin";
        Path pluginDir = createTempDir().resolve(pluginName);
        Files.createDirectories(pluginDir.resolve("_site"));
        Files.createFile(pluginDir.resolve("_site").resolve("somefile"));
        String pluginUrl = createPlugin(pluginDir,
                "description", "fake desc",
                "name", pluginName,
                "version", "1.0",
                "site", "true");
        ExitStatus status = new PluginManagerCliParser(terminal).execute(args("install " + pluginUrl + " --verbose"));
        assertThat("Terminal output was: " + terminal.getTerminalOutput(), status, is(ExitStatus.OK));
        assertThat(terminal.getTerminalOutput(), hasItem(containsString("Name: fake-plugin")));
        assertThat(terminal.getTerminalOutput(), hasItem(containsString("Description: fake desc")));
        assertThat(terminal.getTerminalOutput(), hasItem(containsString("Site: true")));
        assertThat(terminal.getTerminalOutput(), hasItem(containsString("Version: 1.0")));
        assertThat(terminal.getTerminalOutput(), hasItem(containsString("JVM: false")));
        assertThatPluginIsListed(pluginName);
        // We want to check that Plugin Manager moves content to _site
        assertFileExists(environment.pluginsFile().resolve(pluginName).resolve("_site"));
    }

    public void testInstallSitePlugin() throws IOException {
        String pluginName = "fake-plugin";
        Path pluginDir = createTempDir().resolve(pluginName);
        Files.createDirectories(pluginDir.resolve("_site"));
        Files.createFile(pluginDir.resolve("_site").resolve("somefile"));
        String pluginUrl = createPlugin(pluginDir,
            "description", "fake desc",
            "name", pluginName,
            "version", "1.0",
            "site", "true");
        ExitStatus status = new PluginManagerCliParser(terminal).execute(args("install " + pluginUrl));
        assertThat("Terminal output was: " + terminal.getTerminalOutput(), status, is(ExitStatus.OK));
        assertThat(terminal.getTerminalOutput(), not(hasItem(containsString("Name: fake-plugin"))));
        assertThat(terminal.getTerminalOutput(), not(hasItem(containsString("Description:"))));
        assertThat(terminal.getTerminalOutput(), not(hasItem(containsString("Site:"))));
        assertThat(terminal.getTerminalOutput(), not(hasItem(containsString("Version:"))));
        assertThat(terminal.getTerminalOutput(), not(hasItem(containsString("JVM:"))));
        assertThatPluginIsListed(pluginName);
        // We want to check that Plugin Manager moves content to _site
        assertFileExists(environment.pluginsFile().resolve(pluginName).resolve("_site"));
    }

    public void testInstallPluginWithBadChecksum() throws IOException {
        String pluginName = "fake-plugin";
        Path pluginDir = createTempDir().resolve(pluginName);
        Files.createDirectories(pluginDir.resolve("_site"));
        Files.createFile(pluginDir.resolve("_site").resolve("somefile"));
        String pluginUrl = createPluginWithBadChecksum(pluginDir,
                "description", "fake desc",
                "version", "1.0",
                "site", "true");
        assertStatus(String.format(Locale.ROOT, "install %s --verbose", pluginUrl),
                ExitStatus.IO_ERROR);
        assertThatPluginIsNotListed(pluginName);
        assertFileNotExists(environment.pluginsFile().resolve(pluginName).resolve("_site"));
    }

    private void singlePluginInstallAndRemove(String pluginDescriptor, String pluginName, String pluginCoordinates) throws IOException {
        logger.info("--> trying to download and install [{}]", pluginDescriptor);
        if (pluginCoordinates == null) {
            assertStatusOk(String.format(Locale.ROOT, "install %s --verbose", pluginDescriptor));
        } else {
            assertStatusOk(String.format(Locale.ROOT, "install %s --verbose", pluginCoordinates));
        }
        assertThatPluginIsListed(pluginName);

        terminal.getTerminalOutput().clear();
        assertStatusOk("remove " + pluginDescriptor);
        assertThat(terminal.getTerminalOutput(), hasItem(containsString("Removing " + pluginDescriptor)));

        // not listed anymore
        terminal.getTerminalOutput().clear();
        assertStatusOk("list");
        assertThat(terminal.getTerminalOutput(), not(hasItem(containsString(pluginName))));
    }

    /**
     * We are ignoring by default these tests as they require to have an internet access
     * To activate the test, use -Dtests.network=true
     * We test regular form: username/reponame/version
     * It should find it in download.elasticsearch.org service
     */
    @Network
    @AwaitsFix(bugUrl = "fails with jar hell failures - http://build-us-00.elastic.co/job/es_core_master_oracle_6/519/testReport/")
    public void testInstallPluginWithElasticsearchDownloadService() throws IOException {
        assumeTrue("download.elastic.co is accessible", isDownloadServiceWorking("download.elastic.co", 80, "/elasticsearch/ci-test.txt"));
        singlePluginInstallAndRemove("elasticsearch/elasticsearch-transport-thrift/2.4.0", "elasticsearch-transport-thrift", null);
    }

    /**
     * We are ignoring by default these tests as they require to have an internet access
     * To activate the test, use -Dtests.network=true
     * We test regular form: groupId/artifactId/version
     * It should find it in maven central service
     */
    @Network
    @AwaitsFix(bugUrl = "fails with jar hell failures - http://build-us-00.elastic.co/job/es_core_master_oracle_6/519/testReport/")
    public void testInstallPluginWithMavenCentral() throws IOException {
        assumeTrue("search.maven.org is accessible", isDownloadServiceWorking("search.maven.org", 80, "/"));
        assumeTrue("repo1.maven.org is accessible", isDownloadServiceWorking("repo1.maven.org", 443, "/maven2/org/elasticsearch/elasticsearch-transport-thrift/2.4.0/elasticsearch-transport-thrift-2.4.0.pom"));
        singlePluginInstallAndRemove("org.elasticsearch/elasticsearch-transport-thrift/2.4.0", "elasticsearch-transport-thrift", null);
    }

    /**
     * We are ignoring by default these tests as they require to have an internet access
     * To activate the test, use -Dtests.network=true
     * We test site plugins from github: userName/repoName
     * It should find it on github
     */
    @Network @AwaitsFix(bugUrl = "needs to be adapted to 2.0")
    public void testInstallPluginWithGithub() throws IOException {
        assumeTrue("github.com is accessible", isDownloadServiceWorking("github.com", 443, "/"));
        singlePluginInstallAndRemove("elasticsearch/kibana", "kibana", null);
    }

    private boolean isDownloadServiceWorking(String host, int port, String resource) {
        try {
            String protocol = port == 443 ? "https" : "http";
            HttpResponse response = new HttpRequestBuilder(HttpClients.createDefault()).protocol(protocol).host(host).port(port).path(resource).execute();
            if (response.getStatusCode() != 200) {
                logger.warn("[{}{}] download service is not working. Disabling current test.", host, resource);
                return false;
            }
            return true;
        } catch (Throwable t) {
            logger.warn("[{}{}] download service is not working. Disabling current test.", host, resource);
        }
        return false;
    }

    public void testRemovePlugin() throws Exception {
        String pluginName = "plugintest";
        Path pluginDir = createTempDir().resolve(pluginName);
        String pluginUrl = createPlugin(pluginDir,
            "description", "fake desc",
            "name", pluginName,
            "version", "1.0.0",
            "elasticsearch.version", Version.CURRENT.toString(),
            "java.version", System.getProperty("java.specification.version"),
            "jvm", "true",
            "classname", "FakePlugin");

        // We want to remove plugin with plugin short name
        singlePluginInstallAndRemove("plugintest", "plugintest", pluginUrl);

        // We want to remove plugin with groupid/artifactid/version form
        singlePluginInstallAndRemove("groupid/plugintest/1.0.0", "plugintest", pluginUrl);

        // We want to remove plugin with groupid/artifactid form
        singlePluginInstallAndRemove("groupid/plugintest", "plugintest", pluginUrl);
    }

    public void testRemovePlugin_NullName_ThrowsException() throws IOException {
        assertStatus("remove ", USAGE);
    }

    public void testRemovePluginWithURLForm() throws Exception {
        assertStatus("remove file://whatever", USAGE);
        assertThat(terminal.getTerminalOutput(), hasItem(containsString("Illegal plugin name")));
    }

    public void testForbiddenPluginNames() throws IOException {
        assertStatus("remove elasticsearch", USAGE);
        assertStatus("remove elasticsearch.bat", USAGE);
        assertStatus("remove elasticsearch.in.sh", USAGE);
        assertStatus("remove plugin", USAGE);
        assertStatus("remove plugin.bat", USAGE);
        assertStatus("remove service.bat", USAGE);
        assertStatus("remove ELASTICSEARCH", USAGE);
        assertStatus("remove ELASTICSEARCH.IN.SH", USAGE);
    }

    public void testOfficialPluginName_ThrowsException() throws IOException {
        PluginManager.checkForOfficialPlugins("analysis-icu");
        PluginManager.checkForOfficialPlugins("analysis-kuromoji");
        PluginManager.checkForOfficialPlugins("analysis-phonetic");
        PluginManager.checkForOfficialPlugins("analysis-smartcn");
        PluginManager.checkForOfficialPlugins("analysis-stempel");
        PluginManager.checkForOfficialPlugins("cloud-azure");
        PluginManager.checkForOfficialPlugins("cloud-gce");
        PluginManager.checkForOfficialPlugins("delete-by-query");
        PluginManager.checkForOfficialPlugins("lang-javascript");
        PluginManager.checkForOfficialPlugins("lang-python");
        PluginManager.checkForOfficialPlugins("mapper-murmur3");
        PluginManager.checkForOfficialPlugins("mapper-size");
        PluginManager.checkForOfficialPlugins("discovery-multicast");
        PluginManager.checkForOfficialPlugins("discovery-ec2");
        PluginManager.checkForOfficialPlugins("repository-s3");

        try {
            PluginManager.checkForOfficialPlugins("elasticsearch-mapper-attachment");
            fail("elasticsearch-mapper-attachment should not be allowed");
        } catch (IllegalArgumentException e) {
            // We expect that error
        }
    }

    public void testThatBasicAuthIsRejectedOnHttp() throws Exception {
        assertStatus(String.format(Locale.ROOT, "install http://user:pass@localhost:12345/foo.zip --verbose"), CliTool.ExitStatus.IO_ERROR);
        assertThat(terminal.getTerminalOutput(), hasItem(containsString("Basic auth is only supported for HTTPS!")));
    }

    public void testThatBasicAuthIsSupportedWithHttps() throws Exception {
        assumeTrue("test requires security manager to be disabled", System.getSecurityManager() == null);

        SSLSocketFactory defaultSocketFactory = HttpsURLConnection.getDefaultSSLSocketFactory();
        ServerBootstrap serverBootstrap = new ServerBootstrap(new NioServerSocketChannelFactory());
        SelfSignedCertificate ssc = new SelfSignedCertificate("localhost");

        try {

            //  Create a trust manager that does not validate certificate chains:
            SSLContext sc = SSLContext.getInstance("SSL");
            sc.init(null,  InsecureTrustManagerFactory.INSTANCE.getTrustManagers(), null);
            HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());

            final List<HttpRequest> requests = new ArrayList<>();
            final SslContext sslContext = SslContext.newServerContext(ssc.certificate(), ssc.privateKey());

            serverBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
                @Override
                public ChannelPipeline getPipeline() throws Exception {
                    return Channels.pipeline(
                            new SslHandler(sslContext.newEngine()),
                            new HttpRequestDecoder(),
                            new HttpResponseEncoder(),
                            new LoggingServerHandler(requests)
                    );
                }
            });

            Channel channel = serverBootstrap.bind(new InetSocketAddress(InetAddress.getByName("localhost"), 0));
            int port = ((InetSocketAddress) channel.getLocalAddress()).getPort();
            // IO_ERROR because there is no real file delivered...
            assertStatus(String.format(Locale.ROOT, "install https://user:pass@localhost:%s/foo.zip --verbose --timeout 1s", port), ExitStatus.IO_ERROR);

            // ensure that we did not try any other data source like download.elastic.co, in case we specified our own local URL
            assertThat(terminal.getTerminalOutput(), not(hasItem(containsString("download.elastic.co"))));

            assertThat(requests, hasSize(1));
            String msg = String.format(Locale.ROOT, "Request header did not contain Authorization header, terminal output was: %s", terminal.getTerminalOutput());
            assertThat(msg, requests.get(0).headers().contains("Authorization"), is(true));
            assertThat(msg, requests.get(0).headers().get("Authorization"), is("Basic " + Base64.encodeBytes("user:pass".getBytes(StandardCharsets.UTF_8))));
        } finally {
            HttpsURLConnection.setDefaultSSLSocketFactory(defaultSocketFactory);
            serverBootstrap.releaseExternalResources();
            ssc.delete();
        }
    }

    private static class LoggingServerHandler extends SimpleChannelUpstreamHandler {

        private List<HttpRequest> requests;

        public LoggingServerHandler(List<HttpRequest> requests) {
            this.requests = requests;
        }

        @Override
        public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e) throws InterruptedException {
            final HttpRequest request = (HttpRequest) e.getMessage();
            requests.add(request);
            final org.jboss.netty.handler.codec.http.HttpResponse response = new DefaultHttpResponse(HTTP_1_1, HttpResponseStatus.BAD_REQUEST);
            ctx.getChannel().write(response);
        }
    }



    private Environment buildInitialSettings() throws IOException {
        Settings settings = settingsBuilder()
                .put("http.enabled", true)
                .put("path.home", createTempDir()).build();
        return InternalSettingsPreparer.prepareEnvironment(settings, null);
    }

    private void assertStatusOk(String command) {
        assertStatus(command, ExitStatus.OK);
    }

    private void assertStatus(String command, ExitStatus exitStatus) {
        ExitStatus status = new PluginManagerCliParser(terminal).execute(args(command));
        assertThat("Terminal output was: " + terminal.getTerminalOutput(), status, is(exitStatus));
    }

    private void assertThatPluginIsListed(String pluginName) {
        terminal.getTerminalOutput().clear();
        assertStatusOk("list");
        String message = String.format(Locale.ROOT, "Terminal output was: %s", terminal.getTerminalOutput());
        assertThat(message, terminal.getTerminalOutput(), hasItem(containsString(pluginName)));
    }

    private void assertThatPluginIsNotListed(String pluginName) {
        terminal.getTerminalOutput().clear();
        assertStatusOk("list");
        String message = String.format(Locale.ROOT, "Terminal output was: %s", terminal.getTerminalOutput());
        assertFalse(message, terminal.getTerminalOutput().contains(pluginName));
    }
}
