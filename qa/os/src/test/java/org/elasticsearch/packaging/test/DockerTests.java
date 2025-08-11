/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.http.client.fluent.Request;
import org.elasticsearch.packaging.util.Installation;
import org.elasticsearch.packaging.util.Platforms;
import org.elasticsearch.packaging.util.ServerUtils;
import org.elasticsearch.packaging.util.Shell;
import org.elasticsearch.packaging.util.Shell.Result;
import org.elasticsearch.packaging.util.docker.DockerRun;
import org.elasticsearch.packaging.util.docker.DockerShell;
import org.elasticsearch.packaging.util.docker.MockServer;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static java.nio.file.attribute.PosixFilePermissions.fromString;
import static java.util.Arrays.asList;
import static org.elasticsearch.packaging.util.Distribution.Packaging;
import static org.elasticsearch.packaging.util.FileMatcher.p600;
import static org.elasticsearch.packaging.util.FileMatcher.p644;
import static org.elasticsearch.packaging.util.FileMatcher.p660;
import static org.elasticsearch.packaging.util.FileMatcher.p775;
import static org.elasticsearch.packaging.util.FileUtils.append;
import static org.elasticsearch.packaging.util.FileUtils.deleteIfExists;
import static org.elasticsearch.packaging.util.FileUtils.rm;
import static org.elasticsearch.packaging.util.docker.Docker.chownWithPrivilegeEscalation;
import static org.elasticsearch.packaging.util.docker.Docker.copyFromContainer;
import static org.elasticsearch.packaging.util.docker.Docker.existsInContainer;
import static org.elasticsearch.packaging.util.docker.Docker.getContainerLogs;
import static org.elasticsearch.packaging.util.docker.Docker.getImageHealthcheck;
import static org.elasticsearch.packaging.util.docker.Docker.getImageLabels;
import static org.elasticsearch.packaging.util.docker.Docker.getJson;
import static org.elasticsearch.packaging.util.docker.Docker.mkDirWithPrivilegeEscalation;
import static org.elasticsearch.packaging.util.docker.Docker.removeContainer;
import static org.elasticsearch.packaging.util.docker.Docker.restartContainer;
import static org.elasticsearch.packaging.util.docker.Docker.rmDirWithPrivilegeEscalation;
import static org.elasticsearch.packaging.util.docker.Docker.runContainer;
import static org.elasticsearch.packaging.util.docker.Docker.runContainerExpectingFailure;
import static org.elasticsearch.packaging.util.docker.Docker.verifyContainerInstallation;
import static org.elasticsearch.packaging.util.docker.Docker.waitForElasticsearch;
import static org.elasticsearch.packaging.util.docker.DockerFileMatcher.file;
import static org.elasticsearch.packaging.util.docker.DockerRun.builder;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

/**
 * This class tests the Elasticsearch Docker images. We have several:
 * <ul>
 *     <li>The default image</li>
 *     <li>A UBI-based image</li>
 *     <li>Another UBI image for Iron Bank</li>
 * </ul>
 */
public class DockerTests extends PackagingTestCase {
    private Path tempDir;

    private static final String EXAMPLE_PLUGIN_SYSPROP = "tests.example-plugin";
    private static final String EXAMPLE_PLUGIN_PATH = System.getProperty(EXAMPLE_PLUGIN_SYSPROP);

    @BeforeClass
    public static void filterDistros() {
        assumeTrue("only Docker", distribution().isDocker());
    }

    @Before
    public void setupTest() throws IOException {
        installation = runContainer(distribution(), builder());
        tempDir = createTempDir(DockerTests.class.getSimpleName());
    }

    @After
    public void teardownTest() {
        removeContainer();
        rm(tempDir);
    }

    /**
     * Checks that the Docker image can be run, and that it passes various checks.
     */
    public void test010Install() {
        verifyContainerInstallation(installation);
    }

    /**
     * Check that the /_xpack API endpoint responds correctly.
     */
    public void test011PresenceOfXpack() throws Exception {
        waitForElasticsearch(installation);
        final int statusCode = Request.Get("http://localhost:9200/_xpack").execute().returnResponse().getStatusLine().getStatusCode();
        assertThat(statusCode, equalTo(200));
    }

    /**
     * Checks that no plugins are initially active.
     */
    public void test020PluginsListWithNoPlugins() {
        final Installation.Executables bin = installation.executables();
        final Result r = sh.run(bin.pluginTool + " list");

        assertThat("Expected no plugins to be listed", r.stdout, emptyString());
    }

    /**
     * Check that a plugin can be installed without special permissions.
     */
    public void test022InstallPlugin() {
        runContainer(distribution(), builder().volume(Paths.get(EXAMPLE_PLUGIN_PATH), "/analysis-icu.zip"));

        final String plugin = "analysis-icu";
        assertThat("Expected " + plugin + " to not be installed", listPlugins(), not(hasItems(plugin)));

        final Installation.Executables bin = installation.executables();
        sh.run(bin.pluginTool + " install file:///analysis-icu.zip");

        assertThat("Expected installed plugins to be listed", listPlugins(), equalTo(asList("analysis-icu")));
    }

    /**
     * Checks that plugins can be installed by deploying a plugins config file.
     */
    public void test024InstallPluginUsingConfigFile() throws Exception {
        final StringJoiner pluginsDescriptor = new StringJoiner("\n", "", "\n");
        pluginsDescriptor.add("plugins:");
        pluginsDescriptor.add("  - id: analysis-icu");
        pluginsDescriptor.add("    location: file:///analysis-icu.zip");

        final String filename = "elasticsearch-plugins.yml";
        append(tempDir.resolve(filename), pluginsDescriptor.toString());

        // Restart the container. This will sync the plugins automatically. Also
        // stuff the proxy settings with garbage, so any attempt to go out to the internet would fail. The
        // command should instead use the bundled plugin archive.
        runContainer(
            distribution(),
            builder().volume(tempDir.resolve(filename), installation.config.resolve(filename))
                .volume(Paths.get(EXAMPLE_PLUGIN_PATH), "/analysis-icu.zip")
                .envVar(
                    "ES_JAVA_OPTS",
                    "-Dhttp.proxyHost=example.org -Dhttp.proxyPort=9999 -Dhttps.proxyHost=example.org -Dhttps.proxyPort=9999"
                )
        );

        // Since ES is doing the installing, give it a chance to complete
        waitForElasticsearch(installation);

        assertThat("List of installed plugins is incorrect", listPlugins(), hasItems("analysis-icu"));
    }

    /**
     * Check that when using Elasticsearch's plugins sync capability, it will use a proxy when configured to do so.
     * This could either be in the plugins config file, or via the standard Java system properties.
     */
    public void test026SyncPluginsUsingProxy() {
        MockServer.withMockServer(mockServer -> {
            for (boolean useConfigFile : asList(true, false)) {
                mockServer.clearExpectations();

                final StringJoiner config = new StringJoiner("\n", "", "\n");
                config.add("plugins:");
                // This is the new plugin to install. We don't use an official plugin because then Elasticsearch
                // will attempt an SSL connection and that just makes everything more complicated.
                config.add("  - id: my-plugin");
                config.add("    location: http://example.com/my-plugin.zip");

                if (useConfigFile) {
                    config.add("proxy: mockserver:" + mockServer.getPort());
                }

                final String filename = "elasticsearch-plugins.yml";
                final Path pluginsConfigPath = tempDir.resolve(filename);
                deleteIfExists(pluginsConfigPath);
                append(pluginsConfigPath, config.toString());

                final DockerRun builder = builder().volume(pluginsConfigPath, installation.config.resolve(filename))
                    .extraArgs("--link " + mockServer.getContainerId() + ":mockserver");

                if (useConfigFile == false) {
                    builder.envVar("ES_JAVA_OPTS", "-Dhttp.proxyHost=mockserver -Dhttp.proxyPort=" + mockServer.getPort());
                }

                // Restart the container. This will sync plugins automatically, which will fail because
                // ES will be unable to install `my-plugin`
                final Result result = runContainerExpectingFailure(distribution(), builder);

                final List<Map<String, String>> interactions = mockServer.getInteractions();

                assertThat(result.stderr, containsString("FileNotFoundException: http://example.com/my-plugin.zip"));

                // Now check that Elasticsearch did use the proxy server
                assertThat(interactions, hasSize(1));
                final Map<String, String> interaction = interactions.get(0);
                assertThat(interaction, hasEntry("httpRequest.headers.Host[0]", "example.com"));
                assertThat(interaction, hasEntry("httpRequest.headers.User-Agent[0]", "elasticsearch-plugin-installer"));
                assertThat(interaction, hasEntry("httpRequest.method", "GET"));
                assertThat(interaction, hasEntry("httpRequest.path", "/my-plugin.zip"));
            }
        });
    }

    /**
     * Check that the JDK's `cacerts` file is a symlink to the copy provided by the operating system.
     */
    public void test040JavaUsesTheOsProvidedKeystore() {
        final String path = sh.run("realpath jdk/lib/security/cacerts").stdout;

        if (distribution.packaging == Packaging.DOCKER_UBI || distribution.packaging == Packaging.DOCKER_IRON_BANK) {
            // In these images, the `cacerts` file ought to be a symlink here
            assertThat(path, equalTo("/etc/pki/ca-trust/extracted/java/cacerts"));
        } else {
            // Whereas on other images, it's a real file so the real path is the same
            assertThat(path, equalTo("/usr/share/elasticsearch/jdk/lib/security/cacerts"));
        }
    }

    /**
     * Checks that there are Amazon trusted certificates in the cacaerts keystore.
     */
    public void test041AmazonCaCertsAreInTheKeystore() {
        final boolean matches = Arrays.stream(
            sh.run("jdk/bin/keytool -cacerts -storepass changeit -list | grep trustedCertEntry").stdout.split("\n")
        ).anyMatch(line -> line.contains("amazonrootca"));

        assertTrue("Expected Amazon trusted cert in cacerts", matches);
    }

    /**
     * Check that when the keystore is created on startup, it is created with the correct permissions.
     */
    public void test042KeystorePermissionsAreCorrect() throws Exception {
        waitForElasticsearch(installation);

        assertThat(installation.config("elasticsearch.keystore"), file(p660));
    }

    /**
     * Send some basic index, count and delete requests, in order to check that the installation
     * is minimally functional.
     */
    public void test050BasicApiTests() throws Exception {
        waitForElasticsearch(installation);

        assertTrue(existsInContainer(installation.logs.resolve("gc.log")));

        ServerUtils.runElasticsearchTests();
    }

    /**
     * Check that the default config can be overridden using a bind mount, and that env vars are respected
     */
    public void test070BindMountCustomPathConfAndJvmOptions() throws Exception {
        copyFromContainer(installation.config("elasticsearch.yml"), tempDir.resolve("elasticsearch.yml"));
        copyFromContainer(installation.config("log4j2.properties"), tempDir.resolve("log4j2.properties"));

        // we have to disable Log4j from using JMX lest it will hit a security
        // manager exception before we have configured logging; this will fail
        // startup since we detect usages of logging before it is configured
        final String jvmOptions = "-Xms512m\n-Xmx512m\n-Dlog4j2.disable.jmx=true\n";
        append(tempDir.resolve("jvm.options"), jvmOptions);

        // Make the temp directory and contents accessible when bind-mounted.
        Files.setPosixFilePermissions(tempDir, fromString("rwxrwxrwx"));
        // These permissions are necessary to run the tests under Vagrant
        Files.setPosixFilePermissions(tempDir.resolve("elasticsearch.yml"), p644);
        Files.setPosixFilePermissions(tempDir.resolve("log4j2.properties"), p644);

        // Restart the container
        runContainer(
            distribution(),
            builder().volume(tempDir, "/usr/share/elasticsearch/config").envVar("ES_JAVA_OPTS", "-XX:-UseCompressedOops")
        );

        waitForElasticsearch(installation);

        final JsonNode nodes = getJson("/_nodes").get("nodes");
        final String nodeId = nodes.fieldNames().next();

        final int heapSize = nodes.at("/" + nodeId + "/jvm/mem/heap_init_in_bytes").intValue();
        final boolean usingCompressedPointers = nodes.at("/" + nodeId + "/jvm/using_compressed_ordinary_object_pointers").asBoolean();

        logger.warn(nodes.at("/" + nodeId + "/jvm/mem/heap_init_in_bytes"));

        assertThat("heap_init_in_bytes", heapSize, equalTo(536870912));
        assertThat("using_compressed_ordinary_object_pointers", usingCompressedPointers, equalTo(false));
    }

    /**
     * Check that the default config can be overridden using a bind mount, and that env vars are respected.
     */
    public void test071BindMountCustomPathWithDifferentUID() throws Exception {
        Platforms.onLinux(() -> {
            final Path tempEsDataDir = tempDir.resolve("esDataDir");
            // Make the local directory and contents accessible when bind-mounted
            mkDirWithPrivilegeEscalation(tempEsDataDir, 1500, 0);

            // Restart the container
            runContainer(distribution(), builder().volume(tempEsDataDir.toAbsolutePath(), installation.data));

            waitForElasticsearch(installation);

            final JsonNode nodes = getJson("/_nodes");

            assertThat(nodes.at("/_nodes/total").intValue(), equalTo(1));
            assertThat(nodes.at("/_nodes/successful").intValue(), equalTo(1));
            assertThat(nodes.at("/_nodes/failed").intValue(), equalTo(0));

            // Ensure container is stopped before we remove tempEsDataDir, so nothing
            // is using the directory.
            removeContainer();

            rmDirWithPrivilegeEscalation(tempEsDataDir);
        });
    }

    /**
     * Check that it is possible to run Elasticsearch under a different user and group to the default.
     * Note that while the default configuration files are world-readable, when we execute Elasticsearch
     * it will attempt to create a keystore under the `config` directory. This will fail unless
     * we also bind-mount the config dir.
     */
    public void test072RunEsAsDifferentUserAndGroup() throws Exception {
        assumeFalse(Platforms.WINDOWS);

        final Path tempEsDataDir = tempDir.resolve("esDataDir");
        final Path tempEsConfigDir = tempDir.resolve("esConfDir");
        final Path tempEsLogsDir = tempDir.resolve("esLogsDir");

        Files.createDirectory(tempEsConfigDir);
        Files.createDirectory(tempEsConfigDir.resolve("jvm.options.d"));
        Files.createDirectory(tempEsDataDir);
        Files.createDirectory(tempEsLogsDir);

        copyFromContainer(installation.config("elasticsearch.yml"), tempEsConfigDir);
        copyFromContainer(installation.config("jvm.options"), tempEsConfigDir);
        copyFromContainer(installation.config("log4j2.properties"), tempEsConfigDir);

        chownWithPrivilegeEscalation(tempEsConfigDir, "501:501");
        chownWithPrivilegeEscalation(tempEsDataDir, "501:501");
        chownWithPrivilegeEscalation(tempEsLogsDir, "501:501");

        try {
            // Restart the container
            runContainer(
                distribution(),
                builder().uid(501, 501)
                    .volume(tempEsDataDir.toAbsolutePath(), installation.data)
                    .volume(tempEsConfigDir.toAbsolutePath(), installation.config)
                    .volume(tempEsLogsDir.toAbsolutePath(), installation.logs)
            );

            waitForElasticsearch(installation);
            removeContainer();
        } finally {
            rmDirWithPrivilegeEscalation(tempEsConfigDir);
            rmDirWithPrivilegeEscalation(tempEsDataDir);
            rmDirWithPrivilegeEscalation(tempEsLogsDir);
        }
    }

    /**
     * Check that it is possible to run Elasticsearch under a different user and group to the default,
     * without bind-mounting any directories, provided the container user is added to the `root` group.
     */
    public void test073RunEsAsDifferentUserAndGroupWithoutBindMounting() throws Exception {
        // Restart the container
        runContainer(distribution(), builder().extraArgs("--group-add 0").uid(501, 501));

        waitForElasticsearch(installation);
    }

    /**
     * Check that the elastic user's password can be configured via a file and the ELASTIC_PASSWORD_FILE environment variable.
     */
    public void test080ConfigurePasswordThroughEnvironmentVariableFile() throws Exception {
        final String xpackPassword = "hunter2";
        final String passwordFilename = "password.txt";

        append(tempDir.resolve(passwordFilename), xpackPassword + "\n");

        // File permissions need to be secured in order for the ES wrapper to accept
        // them for populating env var values
        Files.setPosixFilePermissions(tempDir.resolve(passwordFilename), p600);
        // But when running in Vagrant, also ensure ES can actually access the file
        chownWithPrivilegeEscalation(tempDir.resolve(passwordFilename), "1000:0");

        // Restart the container
        runContainer(
            distribution(),
            builder().volume(tempDir, "/run/secrets")
                .envVar("ELASTIC_PASSWORD_FILE", "/run/secrets/" + passwordFilename)
                // Enable security so that we can test that the password has been used
                .envVar("xpack.security.enabled", "true")
        );

        // If we configured security correctly, then this call will only work if we specify the correct credentials.
        try {
            waitForElasticsearch("green", null, installation, "elastic", "hunter2");
        } catch (Exception e) {
            throw new AssertionError(
                "Failed to check whether Elasticsearch had started. This could be because "
                    + "authentication isn't working properly. Check the container logs",
                e
            );
        }

        // Also check that an unauthenticated call fails
        final int statusCode = Request.Get("http://localhost:9200/_nodes").execute().returnResponse().getStatusLine().getStatusCode();
        assertThat("Expected server to require authentication", statusCode, equalTo(401));
    }

    /**
     * Check that when verifying the file permissions of _FILE environment variables, symlinks
     * are followed.
     */
    public void test081SymlinksAreFollowedWithEnvironmentVariableFiles() throws Exception {
        // Test relies on symlinks
        assumeFalse(Platforms.WINDOWS);

        final String xpackPassword = "hunter2";
        final String passwordFilename = "password.txt";
        final String symlinkFilename = "password_symlink";

        // ELASTIC_PASSWORD_FILE
        Files.write(tempDir.resolve(passwordFilename), (xpackPassword + "\n").getBytes(StandardCharsets.UTF_8));

        // Link to the password file. We can't use an absolute path for the target, because
        // it won't resolve inside the container.
        Files.createSymbolicLink(tempDir.resolve(symlinkFilename), Paths.get(passwordFilename));

        // File permissions need to be secured in order for the ES wrapper to accept
        // them for populating env var values. The wrapper will resolve the symlink
        // and check the target's permissions.
        Files.setPosixFilePermissions(tempDir.resolve(passwordFilename), p600);

        // Restart the container - this will check that Elasticsearch started correctly,
        // and didn't fail to follow the symlink and check the file permissions
        runContainer(
            distribution(),
            builder().volume(tempDir, "/run/secrets")
                .envVar("ELASTIC_PASSWORD_FILE", "/run/secrets/" + symlinkFilename)
                // Enable security so that we can test that the password has been used
                .envVar("xpack.security.enabled", "true")
        );
    }

    /**
     * Check that environment variables cannot be used with _FILE environment variables.
     */
    public void test082CannotUseEnvVarsAndFiles() throws Exception {
        final String passwordFilename = "password.txt";

        Files.write(tempDir.resolve(passwordFilename), "other_hunter2\n".getBytes(StandardCharsets.UTF_8));

        // File permissions need to be secured in order for the ES wrapper to accept
        // them for populating env var values
        Files.setPosixFilePermissions(tempDir.resolve(passwordFilename), p600);

        final Result dockerLogs = runContainerExpectingFailure(
            distribution,
            builder().volume(tempDir, "/run/secrets")
                .envVar("ELASTIC_PASSWORD", "hunter2")
                .envVar("ELASTIC_PASSWORD_FILE", "/run/secrets/" + passwordFilename)
        );

        assertThat(
            dockerLogs.stderr,
            containsString("ERROR: Both ELASTIC_PASSWORD_FILE and ELASTIC_PASSWORD are set. These are mutually exclusive.")
        );
    }

    /**
     * Check that when populating environment variables by setting variables with the suffix "_FILE",
     * the files' permissions are checked.
     */
    public void test083EnvironmentVariablesUsingFilesHaveCorrectPermissions() throws Exception {
        final String passwordFilename = "password.txt";

        Files.write(tempDir.resolve(passwordFilename), "hunter2\n".getBytes(StandardCharsets.UTF_8));

        // Set invalid file permissions
        Files.setPosixFilePermissions(tempDir.resolve(passwordFilename), p660);

        // Restart the container
        final Result dockerLogs = runContainerExpectingFailure(
            distribution(),
            builder().volume(tempDir, "/run/secrets").envVar("ELASTIC_PASSWORD_FILE", "/run/secrets/" + passwordFilename)
        );

        assertThat(
            dockerLogs.stderr,
            containsString(
                "ERROR: File /run/secrets/" + passwordFilename + " from ELASTIC_PASSWORD_FILE must have file permissions 400 or 600"
            )
        );
    }

    /**
     * Check that when verifying the file permissions of _FILE environment variables, symlinks
     * are followed, and that invalid target permissions are detected.
     */
    public void test084SymlinkToFileWithInvalidPermissionsIsRejected() throws Exception {
        // Test relies on symlinks
        assumeFalse(Platforms.WINDOWS);

        final String xpackPassword = "hunter2";
        final String passwordFilename = "password.txt";
        final String symlinkFilename = "password_symlink";

        // ELASTIC_PASSWORD_FILE
        Files.write(tempDir.resolve(passwordFilename), (xpackPassword + "\n").getBytes(StandardCharsets.UTF_8));

        // Link to the password file. We can't use an absolute path for the target, because
        // it won't resolve inside the container.
        Files.createSymbolicLink(tempDir.resolve(symlinkFilename), Paths.get(passwordFilename));

        // Set invalid permissions on the file that the symlink targets
        Files.setPosixFilePermissions(tempDir.resolve(passwordFilename), p775);

        // Restart the container
        final Result dockerLogs = runContainerExpectingFailure(
            distribution(),
            builder().volume(tempDir, "/run/secrets")
                .envVar("ELASTIC_PASSWORD_FILE", "/run/secrets/" + symlinkFilename)
                // Enable security so that we can test that the password has been used
                .envVar("xpack.security.enabled", "true")
        );

        assertThat(
            dockerLogs.stderr,
            containsString(
                "ERROR: File "
                    + passwordFilename
                    + " (target of symlink /run/secrets/"
                    + symlinkFilename
                    + " from ELASTIC_PASSWORD_FILE) must have file permissions 400 or 600, but actually has: 775"
            )
        );
    }

    /**
     * Check that environment variables are translated to -E options even for commands invoked under
     * `docker exec`, where the Docker image's entrypoint is not executed.
     */
    public void test085EnvironmentVariablesAreRespectedUnderDockerExec() throws Exception {
        installation = runContainer(
            distribution(),
            builder().envVar("ELASTIC_PASSWORD", "hunter2").envVar("xpack.security.enabled", "true")
        );

        // The tool below requires a keystore, so ensure that ES is fully initialised before proceeding.
        waitForElasticsearch("green", null, installation, "elastic", "hunter2");

        sh.getEnv().put("http.host", "this.is.not.valid");

        // This will fail because of the extra env var
        final Result result = sh.runIgnoreExitCode("bash -c 'echo y | elasticsearch-setup-passwords auto'");

        assertFalse("elasticsearch-setup-passwords command should have failed", result.isSuccess());
        assertThat(result.stdout, containsString("java.net.UnknownHostException: this.is.not.valid: Name or service not known"));
    }

    /**
     * Check that settings are applied when they are supplied as environment variables with names that are:
     * <ul>
     *     <li>Prefixed with {@code ES_SETTING_}</li>
     *     <li>All uppercase</li>
     *     <li>Dots (periods) are converted to underscores</li>
     *     <li>Underscores in setting names are escaped by doubling them</li>
     * </ul>
     */
    public void test086EnvironmentVariablesInSnakeCaseAreTranslated() {
        // Note the double-underscore in the var name here, which retains the underscore in translation
        installation = runContainer(distribution(), builder().envVar("ES_SETTING_XPACK_SECURITY_FIPS__MODE_ENABLED", "false"));

        final Optional<String> commandLine = Arrays.stream(sh.run("bash -c 'COLUMNS=2000 ps ax'").stdout.split("\n"))
            .filter(line -> line.contains("org.elasticsearch.bootstrap.Elasticsearch"))
            .findFirst();

        assertThat(commandLine.isPresent(), equalTo(true));

        assertThat(commandLine.get(), containsString("-Expack.security.fips_mode.enabled=false"));
    }

    /**
     * Check that environment variables that do not match the criteria for translation to settings are ignored.
     */
    public void test087EnvironmentVariablesInIncorrectFormatAreIgnored() {
        installation = runContainer(
            distribution(),
            builder()
                // No ES_SETTING_ prefix
                .envVar("XPACK_SECURITY_FIPS__MODE_ENABLED", "false")
                // Incomplete prefix
                .envVar("ES_XPACK_SECURITY_FIPS__MODE_ENABLED", "false")
                // Not underscore-separated
                .envVar("ES.SETTING.XPACK.SECURITY.FIPS_MODE.ENABLED", "false")
                // Not uppercase
                .envVar("es_setting_xpack_security_fips__mode_enabled", "false")
        );

        final Optional<String> commandLine = Arrays.stream(sh.run("bash -c 'COLUMNS=2000 ps ax'").stdout.split("\n"))
            .filter(line -> line.contains("org.elasticsearch.bootstrap.Elasticsearch"))
            .findFirst();

        assertThat(commandLine.isPresent(), equalTo(true));

        assertThat(commandLine.get(), not(containsString("-Expack.security.fips_mode.enabled=false")));
    }

    /**
     * Check that settings are applied when they are supplied as environment variables with names that:
     * <ul>
     *     <li>Consist only of lowercase letters, numbers, underscores and hyphens</li>
     *     <li>Separated by periods</li>
     * </ul>
     */
    public void test088EnvironmentVariablesInDottedFormatArePassedThrough() {
        // Note the double-underscore in the var name here, which retains the underscore in translation
        installation = runContainer(
            distribution(),
            builder().envVar("xpack.security.fips_mode.enabled", "false").envVar("http.cors.allow-methods", "GET")
        );

        final Optional<String> commandLine = Arrays.stream(sh.run("bash -c 'COLUMNS=2000 ps ax'").stdout.split("\n"))
            .filter(line -> line.contains("org.elasticsearch.bootstrap.Elasticsearch"))
            .findFirst();

        assertThat(commandLine.isPresent(), equalTo(true));

        assertThat(
            commandLine.get(),
            allOf(containsString("-Expack.security.fips_mode.enabled=false"), containsString("-Ehttp.cors.allow-methods=GET"))
        );
    }

    /**
     * Check whether the elasticsearch-certutil tool has been shipped correctly,
     * and if present then it can execute.
     */
    public void test090SecurityCliPackaging() {
        final Installation.Executables bin = installation.executables();

        final Path securityCli = installation.lib.resolve("tools").resolve("security-cli");

        assertTrue(existsInContainer(securityCli));

        Result result = sh.run(bin.certutilTool + " --help");
        assertThat(result.stdout, containsString("Simplifies certificate creation for use with the Elastic Stack"));

        // Ensure that the exit code from the java command is passed back up through the shell script
        result = sh.runIgnoreExitCode(bin.certutilTool + " invalid-command");
        assertThat(result.isSuccess(), is(false));
        assertThat(result.stdout, containsString("Unknown command [invalid-command]"));
    }

    /**
     * Check that the elasticsearch-shard tool is shipped in the Docker image and is executable.
     */
    public void test091ElasticsearchShardCliPackaging() {
        final Installation.Executables bin = installation.executables();

        final Result result = sh.run(bin.shardTool + " -h");
        assertThat(result.stdout, containsString("A CLI tool to remove corrupted parts of unrecoverable shards"));
    }

    /**
     * Check that the elasticsearch-node tool is shipped in the Docker image and is executable.
     */
    public void test092ElasticsearchNodeCliPackaging() {
        final Installation.Executables bin = installation.executables();

        final Result result = sh.run(bin.nodeTool + " -h");
        assertThat(
            "Failed to find expected message about the elasticsearch-node CLI tool",
            result.stdout,
            containsString("A CLI tool to " + "do unsafe cluster and index manipulations on current node")
        );
    }

    /**
     * Check that no core dumps have been accidentally included in the Docker image.
     */
    public void test100NoCoreFilesInImage() {
        assertFalse("Unexpected core dump found in Docker image", existsInContainer("/core*"));
    }

    /**
     * Check that there are no files with a GID other than 0.
     */
    public void test101AllFilesAreGroupZero() {
        // Run a `find` command in a new container without Elasticsearch running, so
        // that the results aren't subject to sporadic failures from files appearing /
        // disappearing while `find` is traversing the filesystem.
        //
        // We also create a file under `data/` to ensure that files are created with the
        // expected group.
        final Shell localSh = new Shell();
        final String findResults = localSh.run(
            "docker run --rm --tty " + DockerRun.getImageName(distribution) + " bash -c ' touch data/test && find . -not -gid 0 ' "
        ).stdout;

        assertThat("Found some files whose GID != 0", findResults, is(emptyString()));
    }

    /**
     * Check that the Docker image has the expected "Label Schema" labels.
     * @see <a href="http://label-schema.org/">Label Schema website</a>
     */
    public void test110OrgLabelSchemaLabels() throws Exception {
        assumeTrue(distribution.packaging != Packaging.DOCKER_IRON_BANK);

        final Map<String, String> labels = getImageLabels(distribution);

        final Map<String, String> staticLabels = new HashMap<>();
        staticLabels.put("name", "Elasticsearch");
        staticLabels.put("schema-version", "1.0");
        staticLabels.put("url", "https://www.elastic.co/products/elasticsearch");
        staticLabels.put("usage", "https://www.elastic.co/guide/en/elasticsearch/reference/index.html");
        staticLabels.put("vcs-url", "https://github.com/elastic/elasticsearch");
        staticLabels.put("vendor", "Elastic");
        staticLabels.put("license", "Elastic-License-2.0");

        // TODO: we should check the actual version value
        final Set<String> dynamicLabels = new HashSet<>();
        dynamicLabels.add("build-date");
        dynamicLabels.add("vcs-ref");
        dynamicLabels.add("version");

        final String prefix = "org.label-schema";

        staticLabels.forEach((suffix, value) -> {
            String key = prefix + "." + suffix;
            assertThat(labels, hasKey(key));
            assertThat(labels.get(key), equalTo(value));
        });

        dynamicLabels.forEach(label -> {
            String key = prefix + "." + label;
            assertThat(labels, hasKey(key));
        });
    }

    /**
     * Check that the Docker image has the expected "Open Containers Annotations" labels.
     * @see <a href="https://github.com/opencontainers/image-spec/blob/master/annotations.md">Open Containers Annotations</a>
     */
    public void test110OrgOpencontainersLabels() throws Exception {
        assumeTrue(distribution.packaging != Packaging.DOCKER_IRON_BANK);

        final Map<String, String> labels = getImageLabels(distribution);

        final Map<String, String> staticLabels = new HashMap<>();
        staticLabels.put("title", "Elasticsearch");
        staticLabels.put("url", "https://www.elastic.co/products/elasticsearch");
        staticLabels.put("documentation", "https://www.elastic.co/guide/en/elasticsearch/reference/index.html");
        staticLabels.put("source", "https://github.com/elastic/elasticsearch");
        staticLabels.put("vendor", "Elastic");
        staticLabels.put("licenses", "Elastic-License-2.0");

        // TODO: we should check the actual version value
        final Set<String> dynamicLabels = new HashSet<>();
        dynamicLabels.add("created");
        dynamicLabels.add("revision");
        dynamicLabels.add("version");

        final String prefix = "org.opencontainers.image";

        staticLabels.forEach((suffix, value) -> {
            String key = prefix + "." + suffix;
            assertThat(labels, hasKey(key));
            assertThat(labels.get(key), equalTo(value));
        });

        dynamicLabels.forEach(label -> {
            String key = prefix + "." + label;
            assertThat(labels, hasKey(key));
        });
    }

    /**
     * Check that the container logs contain the expected content for Elasticsearch itself.
     */
    public void test120DockerLogsIncludeElasticsearchLogs() throws Exception {
        waitForElasticsearch(installation);
        final Result containerLogs = getContainerLogs();

        assertThat("Container logs don't contain abbreviated class names", containerLogs.stdout, containsString("o.e.n.Node"));
        assertThat("Container logs don't contain INFO level messages", containerLogs.stdout, containsString("INFO"));
    }

    /**
     * Check that it is possible to write logs to disk
     */
    public void test121CanUseStackLoggingConfig() throws Exception {
        runContainer(distribution(), builder().envVar("ES_LOG_STYLE", "file"));

        waitForElasticsearch(installation);

        final Result containerLogs = getContainerLogs();
        final String[] stdout = containerLogs.stdout.split("\n");

        assertThat(
            "Container logs should be formatted using the stack config",
            stdout[stdout.length - 1],
            matchesPattern("^\\[\\d\\d\\d\\d-.*")
        );
        assertThat("[logs/docker-cluster.log] should exist but it doesn't", existsInContainer("logs/docker-cluster.log"), is(true));
    }

    /**
     * Check that the default logging config can be explicitly selected.
     */
    public void test122CanUseDockerLoggingConfig() throws Exception {
        runContainer(distribution(), builder().envVar("ES_LOG_STYLE", "console"));

        waitForElasticsearch(installation);

        final Result containerLogs = getContainerLogs();
        final String[] stdout = containerLogs.stdout.split("\n");

        assertThat("Container logs should be formatted using the docker config", stdout[stdout.length - 1], startsWith("{\""));
        assertThat("[logs/docker-cluster.log] shouldn't exist but it does", existsInContainer("logs/docker-cluster.log"), is(false));
    }

    /**
     * Check that an unknown logging config is rejected
     */
    public void test123CannotUseUnknownLoggingConfig() {
        final Result result = runContainerExpectingFailure(distribution(), builder().envVar("ES_LOG_STYLE", "unknown"));

        assertThat(result.stderr, containsString("ERROR: ES_LOG_STYLE set to [unknown]. Expected [console] or [file]"));
    }

    /**
     * Check that it when configuring logging to write to disk, the container can be restarted.
     */
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/73126")
    public void test124CanRestartContainerWithStackLoggingConfig() throws Exception {
        runContainer(distribution(), builder().envVar("ES_LOG_STYLE", "file"));

        waitForElasticsearch(installation);

        restartContainer();

        // If something went wrong running Elasticsearch the second time, this will fail.
        waitForElasticsearch(installation);
    }

    /**
     * Check that the Java process running inside the container has the expected UID, GID and username.
     */
    public void test130JavaHasCorrectOwnership() {
        final List<String> processes = Arrays.stream(sh.run("ps -o uid,gid,user -C java").stdout.split("\n"))
            .skip(1)
            .collect(Collectors.toList());

        assertThat("Expected a single java process", processes, hasSize(1));

        final String[] fields = processes.get(0).trim().split("\\s+");

        assertThat(fields, arrayWithSize(3));
        assertThat("Incorrect UID", fields[0], equalTo("1000"));
        assertThat("Incorrect GID", fields[1], equalTo("0"));
        assertThat("Incorrect username", fields[2], equalTo("elasticsearch"));
    }

    /**
     * Check that the init process running inside the container has the expected PID, UID, GID and user.
     * The PID is particularly important because PID 1 handles signal forwarding and child reaping.
     */
    public void test131InitProcessHasCorrectPID() {
        final List<String> processes = Arrays.stream(sh.run("ps -o pid,uid,gid,command -p 1").stdout.split("\n"))
            .skip(1)
            .collect(Collectors.toList());

        assertThat("Expected a single process", processes, hasSize(1));

        final String[] fields = processes.get(0).trim().split("\\s+", 4);

        String expectedUid;
        switch (distribution.packaging) {
            case DOCKER_IRON_BANK:
            case DOCKER_UBI:
                expectedUid = "1000";
                break;
            default:
                expectedUid = "0";
                break;
        }

        assertThat(fields, arrayWithSize(4));
        assertThat("Incorrect PID", fields[0], equalTo("1"));
        assertThat("Incorrect UID", fields[1], equalTo(expectedUid));
        assertThat("Incorrect GID", fields[2], equalTo("0"));
        assertThat("Incorrect init command", fields[3], startsWith("/bin/tini"));
    }

    /**
     * Check that Elasticsearch reports per-node cgroup information.
     */
    public void test140CgroupOsStatsAreAvailable() throws Exception {
        waitForElasticsearch(installation);

        final JsonNode nodes = getJson("/_nodes/stats/os").get("nodes");

        final String nodeId = nodes.fieldNames().next();

        final JsonNode cgroupStats = nodes.at("/" + nodeId + "/os/cgroup");
        assertFalse("Couldn't find /nodes/{nodeId}/os/cgroup in API response", cgroupStats.isMissingNode());

        assertThat("Failed to find [cpu] in node OS cgroup stats", cgroupStats.get("cpu"), not(nullValue()));
        assertThat("Failed to find [cpuacct] in node OS cgroup stats", cgroupStats.get("cpuacct"), not(nullValue()));
    }

    /**
     * Check that when available system memory is constrained by Docker, the machine-dependant heap sizing
     * logic sets the correct heap size, based on the container limits.
     */
    public void test150MachineDependentHeap() throws Exception {
        // Start by ensuring `jvm.options` doesn't define any heap options
        final Path jvmOptionsPath = tempDir.resolve("jvm.options");
        final Path containerJvmOptionsPath = installation.config("jvm.options");
        copyFromContainer(containerJvmOptionsPath, jvmOptionsPath);

        final List<String> jvmOptions = Files.readAllLines(jvmOptionsPath)
            .stream()
            .filter(line -> (line.startsWith("-Xms") || line.startsWith("-Xmx")) == false)
            .collect(Collectors.toList());

        Files.write(jvmOptionsPath, jvmOptions);

        // Now run the container, being explicit about the available memory
        runContainer(distribution(), builder().memory("942m").volume(jvmOptionsPath, containerJvmOptionsPath));
        waitForElasticsearch(installation);

        // Grab the container output and find the line where it print the JVM arguments. This will
        // let us see what the automatic heap sizing calculated.
        final Optional<String> jvmArgumentsLine = Arrays.stream(getContainerLogs().stdout.split(System.lineSeparator()))
            .filter(line -> line.contains("JVM arguments"))
            .findFirst();
        assertThat("Failed to find jvmArguments in container logs", jvmArgumentsLine.isPresent(), is(true));

        final JsonNode jsonNode = new ObjectMapper().readTree(jvmArgumentsLine.get());

        final String argsStr = jsonNode.get("message").textValue();
        final List<String> xArgs = Arrays.stream(argsStr.substring(1, argsStr.length() - 1).split(",\\s*"))
            .filter(arg -> arg.startsWith("-X"))
            .collect(Collectors.toList());

        // This is roughly 0.4 * 942
        assertThat(xArgs, hasItems("-Xms376m", "-Xmx376m"));
    }

    /**
     * Checks that the image has an appropriate <code>HEALTHCHECK</code> definition for the current distribution.
     */
    public void test160CheckImageHealthcheckDefinition() throws Exception {
        final List<String> imageHealthcheck = getImageHealthcheck(distribution);

        if (distribution.packaging == Packaging.DOCKER_IRON_BANK) {
            assertThat(imageHealthcheck, contains("CMD-SHELL", "curl -I -f --max-time 5 http://localhost:9200 || exit 1"));
        } else {
            assertThat(imageHealthcheck, nullValue());
        }
    }

    /**
     * Ensure that the default shell in the image is {@code bash}, since some alternatives e.g. {@code dash}
     * are stricter about environment variable names.
     */
    public void test170DefaultShellIsBash() {
        final Result result = DockerShell.executeCommand("/bin/sh", "-c", "echo $SHELL");
        if (result.isSuccess()) {
            assertThat(result.stdout, equalTo("/bin/bash"));
        } else {
            throw new RuntimeException("Command failed: " + result.stderr);
        }
    }

    /**
     * Check that the UBI images has the correct license information in the correct place.
     */
    public void test200UbiImagesHaveLicenseDirectory() {
        assumeTrue(distribution.packaging == Packaging.DOCKER_UBI);

        final String[] files = sh.run("find /licenses -type f").stdout.split("\n");
        assertThat(files, arrayContaining("/licenses/LICENSE"));

        // UBI image doesn't contain `diff`
        final String ubiLicense = sh.run("cat /licenses/LICENSE").stdout;
        final String distroLicense = sh.run("cat /usr/share/elasticsearch/LICENSE.txt").stdout;
        assertThat(ubiLicense, equalTo(distroLicense));
    }

    /**
     * Check that the UBI image has the expected labels
     */
    public void test210UbiLabels() throws Exception {
        assumeTrue(distribution.packaging == Packaging.DOCKER_UBI);

        final Map<String, String> labels = getImageLabels(distribution);

        final Map<String, String> staticLabels = new HashMap<>();
        staticLabels.put("name", "Elasticsearch");
        staticLabels.put("maintainer", "infra@elastic.co");
        staticLabels.put("vendor", "Elastic");
        staticLabels.put("summary", "Elasticsearch");
        staticLabels.put("description", "You know, for search.");

        final Set<String> dynamicLabels = new HashSet<>();
        dynamicLabels.add("release");
        dynamicLabels.add("version");

        staticLabels.forEach((key, value) -> {
            assertThat(labels, hasKey(key));
            assertThat(labels.get(key), equalTo(value));
        });

        dynamicLabels.forEach(key -> assertThat(labels, hasKey(key)));
    }

    /**
     * Check that the Iron Bank image has the correct license information in the correct place.
     */
    public void test300IronBankImagesHaveLicenseDirectory() {
        assumeTrue(distribution.packaging == Packaging.DOCKER_IRON_BANK);

        final String[] files = sh.run("find /licenses -type f").stdout.split("\n");
        assertThat(files, arrayContaining("/licenses/LICENSE", "/licenses/LICENSE.addendum"));

        // Image doesn't contain `diff`
        final String ubiLicense = sh.run("cat /licenses/LICENSE").stdout;
        final String distroLicense = sh.run("cat /usr/share/elasticsearch/LICENSE.txt").stdout;
        assertThat(ubiLicense, equalTo(distroLicense));
    }

    /**
     * Check that the Iron Bank image doesn't define extra labels
     */
    public void test310IronBankImageHasNoAdditionalLabels() throws Exception {
        assumeTrue(distribution.packaging == Packaging.DOCKER_IRON_BANK);

        final Map<String, String> labels = getImageLabels(distribution);

        final Set<String> labelKeys = labels.keySet();

        // We can't just assert that the labels map is empty, because it can inherit labels from its base.
        // This is certainly the case when we build the Iron Bank image using a UBI base. It is unknown
        // if that is true for genuine Iron Bank builds.
        assertFalse(labelKeys.stream().anyMatch(l -> l.startsWith("org.label-schema.")));
        assertFalse(labelKeys.stream().anyMatch(l -> l.startsWith("org.opencontainers.")));
    }

    private List<String> listPlugins() {
        final Installation.Executables bin = installation.executables();
        return Arrays.stream(sh.run(bin.pluginTool + " list").stdout.split("\n")).collect(Collectors.toList());
    }

    public void test600Interrupt() throws Exception {
        waitForElasticsearch(installation);
        final Result containerLogs = getContainerLogs();

        assertThat("Container logs should contain starting ...", containerLogs.stdout, containsString("starting ..."));

        final List<String> processes = Arrays.stream(sh.run("pgrep java").stdout.split(System.lineSeparator()))
            .collect(Collectors.toList());
        int maxPid = processes.stream().map(i -> Integer.parseInt(i.trim())).max(Integer::compareTo).get();

        sh.run("bash -c 'kill -int " + maxPid + "'"); // send ctrl+c to all java processes

        assertBusy(() -> {
            final Result containerLogsAfter = getContainerLogs();
            assertThat("Container logs should contain stopping ...", containerLogsAfter.stdout, containsString("stopping ..."));
            assertThat("No errors stdout", containerLogsAfter.stdout, not(containsString("java.security.AccessControlException:")));
            assertThat("No errors stderr", containerLogsAfter.stderr, not(containsString("java.security.AccessControlException:")));
        });
    }
}
