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

package org.elasticsearch.packaging.test;

import org.apache.http.client.fluent.Request;
import org.elasticsearch.packaging.util.Distribution;
import org.elasticsearch.packaging.util.Docker.DockerShell;
import org.elasticsearch.packaging.util.Installation;
import org.elasticsearch.packaging.util.ServerUtils;
import org.elasticsearch.packaging.util.Shell.Result;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.nio.file.attribute.PosixFilePermissions.fromString;
import static org.elasticsearch.packaging.util.Docker.assertPermissionsAndOwnership;
import static org.elasticsearch.packaging.util.Docker.copyFromContainer;
import static org.elasticsearch.packaging.util.Docker.ensureImageIsLoaded;
import static org.elasticsearch.packaging.util.Docker.existsInContainer;
import static org.elasticsearch.packaging.util.Docker.getEnabledPlugins;
import static org.elasticsearch.packaging.util.Docker.getImageLabels;
import static org.elasticsearch.packaging.util.Docker.removeContainer;
import static org.elasticsearch.packaging.util.Docker.runContainer;
import static org.elasticsearch.packaging.util.Docker.runContainerExpectingFailure;
import static org.elasticsearch.packaging.util.Docker.verifyContainerInstallation;
import static org.elasticsearch.packaging.util.Docker.waitForElasticsearch;
import static org.elasticsearch.packaging.util.Docker.waitForPathToExist;
import static org.elasticsearch.packaging.util.FileMatcher.p600;
import static org.elasticsearch.packaging.util.FileMatcher.p660;
import static org.elasticsearch.packaging.util.FileUtils.append;
import static org.elasticsearch.packaging.util.FileUtils.getTempDir;
import static org.elasticsearch.packaging.util.FileUtils.rm;
import static org.elasticsearch.packaging.util.ServerUtils.makeRequest;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.Assume.assumeTrue;

public class DockerTests extends PackagingTestCase {
    protected DockerShell sh;
    private Path tempDir;

    @BeforeClass
    public static void filterDistros() {
        assumeTrue("only Docker", distribution.packaging == Distribution.Packaging.DOCKER);

        ensureImageIsLoaded(distribution);
    }

    @AfterClass
    public static void cleanup() {
        // runContainer also calls this, so we don't need this method to be annotated as `@After`
        removeContainer();
    }

    @Before
    public void setupTest() throws IOException {
        sh = new DockerShell();
        installation = runContainer(distribution());
        tempDir = Files.createTempDirectory(getTempDir(), DockerTests.class.getSimpleName());
    }

    @After
    public void teardownTest() {
        rm(tempDir);
    }

    /**
     * Checks that the Docker image can be run, and that it passes various checks.
     */
    public void test10Install() {
        verifyContainerInstallation(installation, distribution());
    }

    /**
     * Checks that no plugins are initially active.
     */
    public void test20PluginsListWithNoPlugins() {
        final Installation.Executables bin = installation.executables();
        final Result r = sh.run(bin.elasticsearchPlugin + " list");

        assertThat("Expected no plugins to be listed", r.stdout, emptyString());
    }

    /**
     * Check that the IngestUserAgentPlugin is enabled.
     */
    public void test21IngestUserAgentPluginIsInstalled() throws Exception {
        assertThat(getEnabledPlugins(installation), hasItem("org.elasticsearch.ingest.useragent.IngestUserAgentPlugin"));
    }

    /**
     * Check that the IngestGeoIpPlugin is enabled.
     */
    public void test22IngestGeoIpPluginIsInstalled() throws Exception {
        assertThat(getEnabledPlugins(installation), hasItem("org.elasticsearch.ingest.geoip.IngestGeoIpPlugin"));
    }

    /**
     * Check that a keystore can be manually created using the provided CLI tool.
     */
    public void test40CreateKeystoreManually() throws InterruptedException {
        final Installation.Executables bin = installation.executables();

        final Path keystorePath = installation.config("elasticsearch.keystore");

        waitForPathToExist(keystorePath);

        // Move the auto-created one out of the way, or else the CLI prompts asks us to confirm
        sh.run("mv " + keystorePath + " " + keystorePath + ".bak");

        sh.run(bin.elasticsearchKeystore + " create");

        final Result r = sh.run(bin.elasticsearchKeystore + " list");
        assertThat(r.stdout, containsString("keystore.seed"));
    }

    /**
     * Check that the default keystore is automatically created
     */
    public void test41AutoCreateKeystore() throws Exception {
        final Path keystorePath = installation.config("elasticsearch.keystore");

        waitForPathToExist(keystorePath);

        assertPermissionsAndOwnership(keystorePath, p660);

        final Installation.Executables bin = installation.executables();
        final Result result = sh.run(bin.elasticsearchKeystore + " list");
        assertThat(result.stdout, containsString("keystore.seed"));
    }

    /**
     * Check that the JDK's cacerts file is a symlink to the copy provided by the operating system.
     */
    public void test42JavaUsesTheOsProvidedKeystore() {
        final String path = sh.run("realpath jdk/lib/security/cacerts").stdout;

        assertThat(path, equalTo("/etc/pki/ca-trust/extracted/java/cacerts"));
    }

    /**
     * Checks that there are Amazon trusted certificates in the cacaerts keystore.
     */
    public void test43AmazonCaCertsAreInTheKeystore() {
        final boolean matches = sh.run("jdk/bin/keytool -cacerts -storepass changeit -list | grep trustedCertEntry").stdout
            .lines()
            .anyMatch(line -> line.contains("amazonrootca"));

        assertTrue("Expected Amazon trusted cert in cacerts", matches);
    }

    /**
     * Send some basic index, count and delete requests, in order to check that the installation
     * is minimally functional.
     */
    public void test50BasicApiTests() throws Exception {
        waitForElasticsearch(installation);

        assertTrue(existsInContainer(installation.logs.resolve("gc.log")));

        ServerUtils.runElasticsearchTests();
    }

    /**
     * Check that the default config can be overridden using a bind mount, and that env vars are respected
     */
    public void test70BindMountCustomPathConfAndJvmOptions() throws Exception {
        copyFromContainer(installation.config("elasticsearch.yml"), tempDir.resolve("elasticsearch.yml"));
        copyFromContainer(installation.config("log4j2.properties"), tempDir.resolve("log4j2.properties"));

        // we have to disable Log4j from using JMX lest it will hit a security
        // manager exception before we have configured logging; this will fail
        // startup since we detect usages of logging before it is configured
        final String jvmOptions = "-Xms512m\n-Xmx512m\n-Dlog4j2.disable.jmx=true\n";
        append(tempDir.resolve("jvm.options"), jvmOptions);

        // Make the temp directory and contents accessible when bind-mounted
        Files.setPosixFilePermissions(tempDir, fromString("rwxrwxrwx"));

        // Restart the container
        final Map<Path, Path> volumes = Map.of(tempDir, Path.of("/usr/share/elasticsearch/config"));
        runContainer(distribution(), volumes, Map.of("ES_JAVA_OPTS", "-XX:-UseCompressedOops"));

        waitForElasticsearch(installation);

        final String nodesResponse = makeRequest(Request.Get("http://localhost:9200/_nodes"));
        assertThat(nodesResponse, containsString("\"heap_init_in_bytes\":536870912"));
        assertThat(nodesResponse, containsString("\"using_compressed_ordinary_object_pointers\":\"false\""));
    }

    /**
     * Check that environment variables can be populated by setting variables with the suffix "_FILE",
     * which point to files that hold the required values.
     */
    public void test80SetEnvironmentVariablesUsingFiles() throws Exception {
        final String optionsFilename = "esJavaOpts.txt";

        // ES_JAVA_OPTS_FILE
        Files.writeString(tempDir.resolve(optionsFilename), "-XX:-UseCompressedOops\n");

        Map<String, String> envVars = Map.of("ES_JAVA_OPTS_FILE", "/run/secrets/" + optionsFilename);

        // File permissions need to be secured in order for the ES wrapper to accept
        // them for populating env var values
        Files.setPosixFilePermissions(tempDir.resolve(optionsFilename), p600);

        final Map<Path, Path> volumes = Map.of(tempDir, Path.of("/run/secrets"));

        // Restart the container
        runContainer(distribution(), volumes, envVars);

        waitForElasticsearch(installation);

        final String nodesResponse = makeRequest(Request.Get("http://localhost:9200/_nodes"));

        assertThat(nodesResponse, containsString("\"using_compressed_ordinary_object_pointers\":\"false\""));
    }

    /**
     * Check that the elastic user's password can be configured via a file and the ELASTIC_PASSWORD_FILE environment variable.
     */
    public void test81ConfigurePasswordThroughEnvironmentVariableFile() throws Exception {
        // Test relies on configuring security
        assumeTrue(distribution.isDefault());

        final String xpackPassword = "hunter2";
        final String passwordFilename = "password.txt";

        // ELASTIC_PASSWORD_FILE
        Files.writeString(tempDir.resolve(passwordFilename), xpackPassword + "\n");

        Map<String, String> envVars = Map
            .of(
                "ELASTIC_PASSWORD_FILE",
                "/run/secrets/" + passwordFilename,
                // Enable security so that we can test that the password has been used
                "xpack.security.enabled",
                "true"
            );

        // File permissions need to be secured in order for the ES wrapper to accept
        // them for populating env var values
        Files.setPosixFilePermissions(tempDir.resolve(passwordFilename), p600);

        final Map<Path, Path> volumes = Map.of(tempDir, Path.of("/run/secrets"));

        // Restart the container
        runContainer(distribution(), volumes, envVars);

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
     * Check that environment variables cannot be used with _FILE environment variables.
     */
    public void test81CannotUseEnvVarsAndFiles() throws Exception {
        final String optionsFilename = "esJavaOpts.txt";

        // ES_JAVA_OPTS_FILE
        Files.writeString(tempDir.resolve(optionsFilename), "-XX:-UseCompressedOops\n");

        Map<String, String> envVars = Map.of(
            "ES_JAVA_OPTS", "-XX:+UseCompressedOops",
            "ES_JAVA_OPTS_FILE", "/run/secrets/" + optionsFilename
        );

        // File permissions need to be secured in order for the ES wrapper to accept
        // them for populating env var values
        Files.setPosixFilePermissions(tempDir.resolve(optionsFilename), p600);

        final Map<Path, Path> volumes = Map.of(tempDir, Path.of("/run/secrets"));

        final Result dockerLogs = runContainerExpectingFailure(distribution, volumes, envVars);

        assertThat(
            dockerLogs.stderr,
            containsString("ERROR: Both ES_JAVA_OPTS_FILE and ES_JAVA_OPTS are set. These are mutually exclusive.")
        );
    }

    /**
     * Check that when populating environment variables by setting variables with the suffix "_FILE",
     * the files' permissions are checked.
     */
    public void test82EnvironmentVariablesUsingFilesHaveCorrectPermissions() throws Exception {
        final String optionsFilename = "esJavaOpts.txt";

        // ES_JAVA_OPTS_FILE
        Files.writeString(tempDir.resolve(optionsFilename), "-XX:-UseCompressedOops\n");

        Map<String, String> envVars = Map.of("ES_JAVA_OPTS_FILE", "/run/secrets/" + optionsFilename);

        // Set invalid file permissions
        Files.setPosixFilePermissions(tempDir.resolve(optionsFilename), p660);

        final Map<Path, Path> volumes = Map.of(tempDir, Path.of("/run/secrets"));

        // Restart the container
        final Result dockerLogs = runContainerExpectingFailure(distribution(), volumes, envVars);

        assertThat(
            dockerLogs.stderr,
            containsString("ERROR: File /run/secrets/" + optionsFilename + " from ES_JAVA_OPTS_FILE must have file permissions 400 or 600")
        );
    }

    /**
     * Check whether the elasticsearch-certutil tool has been shipped correctly,
     * and if present then it can execute.
     */
    public void test90SecurityCliPackaging() {
        final Installation.Executables bin = installation.executables();

        final Path securityCli = installation.lib.resolve("tools").resolve("security-cli");

        if (distribution().isDefault()) {
            assertTrue(existsInContainer(securityCli));

            Result result = sh.run(bin.elasticsearchCertutil + " --help");
            assertThat(result.stdout, containsString("Simplifies certificate creation for use with the Elastic Stack"));

            // Ensure that the exit code from the java command is passed back up through the shell script
            result = sh.runIgnoreExitCode(bin.elasticsearchCertutil + " invalid-command");
            assertThat(result.isSuccess(), is(false));
            assertThat(result.stdout, containsString("Unknown command [invalid-command]"));
        } else {
            assertFalse(existsInContainer(securityCli));
        }
    }

    /**
     * Check that the elasticsearch-shard tool is shipped in the Docker image and is executable.
     */
    public void test91ElasticsearchShardCliPackaging() {
        final Installation.Executables bin = installation.executables();

        final Result result = sh.run(bin.elasticsearchShard + " -h");
        assertThat(result.stdout, containsString("A CLI tool to remove corrupted parts of unrecoverable shards"));
    }

    /**
     * Check that the elasticsearch-node tool is shipped in the Docker image and is executable.
     */
    public void test92ElasticsearchNodeCliPackaging() {
        final Installation.Executables bin = installation.executables();

        final Result result = sh.run(bin.elasticsearchNode + " -h");
        assertThat(
            "Failed to find expected message about the elasticsearch-node CLI tool",
            result.stdout,
            containsString("A CLI tool to do unsafe cluster and index manipulations on current node")
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
        final String findResults = sh.run("find . -not -gid 0").stdout;

        assertThat("Found some files whose GID != 0", findResults, is(emptyString()));
    }

    public void test110OrgLabelSchemaLabels() throws Exception {
        final Map<String, String> labels = getImageLabels(distribution);

        final Map<String, String> staticLabels = new HashMap<>();
        staticLabels.put("name", "Elasticsearch");
        staticLabels.put("schema-version", "1.0");
        staticLabels.put("url", "https://www.elastic.co/products/elasticsearch");
        staticLabels.put("usage", "https://www.elastic.co/guide/en/elasticsearch/reference/index.html");
        staticLabels.put("vcs-url", "https://github.com/elastic/elasticsearch");
        staticLabels.put("vendor", "Elastic");

        if (distribution.isOSS()) {
            staticLabels.put("license", "Apache-2.0");
        } else {
            staticLabels.put("license", "Elastic-License");
        }

        // TODO: we should check the actual version value
        final Set<String> dynamicLabels = Set.of("build-date", "vcs-ref", "version");

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

    public void test110OrgOpencontainersLabels() throws Exception {
        final Map<String, String> labels = getImageLabels(distribution);

        final Map<String, String> staticLabels = new HashMap<>();
        staticLabels.put("title", "Elasticsearch");
        staticLabels.put("url", "https://www.elastic.co/products/elasticsearch");
        staticLabels.put("documentation", "https://www.elastic.co/guide/en/elasticsearch/reference/index.html");
        staticLabels.put("source", "https://github.com/elastic/elasticsearch");
        staticLabels.put("vendor", "Elastic");

        if (distribution.isOSS()) {
            staticLabels.put("licenses", "Apache-2.0");
        } else {
            staticLabels.put("licenses", "Elastic-License");
        }

        // TODO: we should check the actual version value
        final Set<String> dynamicLabels = Set.of("created", "revision", "version");

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
}
