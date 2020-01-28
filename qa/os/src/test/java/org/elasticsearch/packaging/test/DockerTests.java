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

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.http.client.fluent.Request;
import org.elasticsearch.packaging.util.Installation;
import org.elasticsearch.packaging.util.Platforms;
import org.elasticsearch.packaging.util.ServerUtils;
import org.elasticsearch.packaging.util.Shell.Result;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.nio.file.attribute.PosixFilePermissions.fromString;
import static org.elasticsearch.packaging.util.Docker.copyFromContainer;
import static org.elasticsearch.packaging.util.Docker.existsInContainer;
import static org.elasticsearch.packaging.util.Docker.getContainerLogs;
import static org.elasticsearch.packaging.util.Docker.getImageLabels;
import static org.elasticsearch.packaging.util.Docker.getJson;
import static org.elasticsearch.packaging.util.Docker.mkDirWithPrivilegeEscalation;
import static org.elasticsearch.packaging.util.Docker.rmDirWithPrivilegeEscalation;
import static org.elasticsearch.packaging.util.Docker.runContainer;
import static org.elasticsearch.packaging.util.Docker.runContainerExpectingFailure;
import static org.elasticsearch.packaging.util.Docker.verifyContainerInstallation;
import static org.elasticsearch.packaging.util.Docker.waitForElasticsearch;
import static org.elasticsearch.packaging.util.FileMatcher.p600;
import static org.elasticsearch.packaging.util.FileMatcher.p660;
import static org.elasticsearch.packaging.util.FileMatcher.p775;
import static org.elasticsearch.packaging.util.FileUtils.append;
import static org.elasticsearch.packaging.util.FileUtils.getTempDir;
import static org.elasticsearch.packaging.util.FileUtils.rm;
import static org.elasticsearch.packaging.util.ServerUtils.makeRequest;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

public class DockerTests extends PackagingTestCase {
    private Path tempDir;

    @BeforeClass
    public static void filterDistros() {
        assumeTrue("only Docker", distribution().isDocker());
    }

    @Before
    public void setupTest() throws IOException {
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
    public void test010Install() {
        verifyContainerInstallation(installation, distribution());
    }

    /**
     * Check that the /_xpack API endpoint's presence is correct for the type of distribution being tested.
     */
    public void test011PresenceOfXpack() throws Exception {
        waitForElasticsearch(installation);
        final int statusCode = Request.Get("http://localhost:9200/_xpack").execute().returnResponse().getStatusLine().getStatusCode();

        if (distribution.isOSS()) {
            assertThat(statusCode, greaterThanOrEqualTo(400));
        } else {
            assertThat(statusCode, equalTo(200));
        }
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
     * Check that the JDK's cacerts file is a symlink to the copy provided by the operating system.
     */
    public void test040JavaUsesTheOsProvidedKeystore() {
        final String path = sh.run("realpath jdk/lib/security/cacerts").stdout;

        assertThat(path, equalTo("/etc/pki/ca-trust/extracted/java/cacerts"));
    }

    /**
     * Checks that there are Amazon trusted certificates in the cacaerts keystore.
     */
    public void test041AmazonCaCertsAreInTheKeystore() {
        final boolean matches = sh.run("jdk/bin/keytool -cacerts -storepass changeit -list | grep trustedCertEntry").stdout.lines()
            .anyMatch(line -> line.contains("amazonrootca"));

        assertTrue("Expected Amazon trusted cert in cacerts", matches);
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

        // Make the temp directory and contents accessible when bind-mounted
        Files.setPosixFilePermissions(tempDir, fromString("rwxrwxrwx"));

        // Restart the container
        final Map<Path, Path> volumes = Map.of(tempDir, Path.of("/usr/share/elasticsearch/config"));
        runContainer(distribution(), volumes, Map.of("ES_JAVA_OPTS", "-XX:-UseCompressedOops"));

        waitForElasticsearch(installation);

        final JsonNode nodes = getJson("_nodes").get("nodes");
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
            final Map<Path, Path> volumes = Map.of(tempEsDataDir.toAbsolutePath(), installation.data);

            runContainer(distribution(), volumes, null);

            waitForElasticsearch(installation);

            final JsonNode nodes = getJson("_nodes");

            assertThat(nodes.at("/_nodes/total").intValue(), equalTo(1));
            assertThat(nodes.at("/_nodes/successful").intValue(), equalTo(1));
            assertThat(nodes.at("/_nodes/failed").intValue(), equalTo(0));

            rmDirWithPrivilegeEscalation(tempEsDataDir);
        });
    }

    /**
     * Check that environment variables can be populated by setting variables with the suffix "_FILE",
     * which point to files that hold the required values.
     */
    public void test080SetEnvironmentVariablesUsingFiles() throws Exception {
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
    public void test081ConfigurePasswordThroughEnvironmentVariableFile() throws Exception {
        // Test relies on configuring security
        assumeTrue(distribution.isDefault());

        final String xpackPassword = "hunter2";
        final String passwordFilename = "password.txt";

        // ELASTIC_PASSWORD_FILE
        Files.writeString(tempDir.resolve(passwordFilename), xpackPassword + "\n");

        Map<String, String> envVars = Map.of(
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
     * Check that when verifying the file permissions of _FILE environment variables, symlinks
     * are followed.
     */
    public void test082SymlinksAreFollowedWithEnvironmentVariableFiles() throws Exception {
        // Test relies on configuring security
        assumeTrue(distribution.isDefault());
        // Test relies on symlinks
        assumeFalse(Platforms.WINDOWS);

        final String xpackPassword = "hunter2";
        final String passwordFilename = "password.txt";
        final String symlinkFilename = "password_symlink";

        // ELASTIC_PASSWORD_FILE
        Files.writeString(tempDir.resolve(passwordFilename), xpackPassword + "\n");

        // Link to the password file. We can't use an absolute path for the target, because
        // it won't resolve inside the container.
        Files.createSymbolicLink(tempDir.resolve(symlinkFilename), Path.of(passwordFilename));

        Map<String, String> envVars = Map.of(
            "ELASTIC_PASSWORD_FILE",
            "/run/secrets/" + symlinkFilename,
            // Enable security so that we can test that the password has been used
            "xpack.security.enabled",
            "true"
        );

        // File permissions need to be secured in order for the ES wrapper to accept
        // them for populating env var values. The wrapper will resolve the symlink
        // and check the target's permissions.
        Files.setPosixFilePermissions(tempDir.resolve(passwordFilename), p600);

        final Map<Path, Path> volumes = Map.of(tempDir, Path.of("/run/secrets"));

        // Restart the container - this will check that Elasticsearch started correctly,
        // and didn't fail to follow the symlink and check the file permissions
        runContainer(distribution(), volumes, envVars);
    }

    /**
     * Check that environment variables cannot be used with _FILE environment variables.
     */
    public void test083CannotUseEnvVarsAndFiles() throws Exception {
        final String optionsFilename = "esJavaOpts.txt";

        // ES_JAVA_OPTS_FILE
        Files.writeString(tempDir.resolve(optionsFilename), "-XX:-UseCompressedOops\n");

        Map<String, String> envVars = Map.of(
            "ES_JAVA_OPTS",
            "-XX:+UseCompressedOops",
            "ES_JAVA_OPTS_FILE",
            "/run/secrets/" + optionsFilename
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
    public void test084EnvironmentVariablesUsingFilesHaveCorrectPermissions() throws Exception {
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
     * Check that when verifying the file permissions of _FILE environment variables, symlinks
     * are followed, and that invalid target permissions are detected.
     */
    public void test085SymlinkToFileWithInvalidPermissionsIsRejected() throws Exception {
        // Test relies on configuring security
        assumeTrue(distribution.isDefault());
        // Test relies on symlinks
        assumeFalse(Platforms.WINDOWS);

        final String xpackPassword = "hunter2";
        final String passwordFilename = "password.txt";
        final String symlinkFilename = "password_symlink";

        // ELASTIC_PASSWORD_FILE
        Files.writeString(tempDir.resolve(passwordFilename), xpackPassword + "\n");

        // Link to the password file. We can't use an absolute path for the target, because
        // it won't resolve inside the container.
        Files.createSymbolicLink(tempDir.resolve(symlinkFilename), Path.of(passwordFilename));

        Map<String, String> envVars = Map.of(
            "ELASTIC_PASSWORD_FILE",
            "/run/secrets/" + symlinkFilename,
            // Enable security so that we can test that the password has been used
            "xpack.security.enabled",
            "true"
        );

        // Set invalid permissions on the file that the symlink targets
        Files.setPosixFilePermissions(tempDir.resolve(passwordFilename), p775);

        final Map<Path, Path> volumes = Map.of(tempDir, Path.of("/run/secrets"));

        // Restart the container
        final Result dockerLogs = runContainerExpectingFailure(distribution(), volumes, envVars);

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
    public void test086EnvironmentVariablesAreRespectedUnderDockerExec() {
        // This test relies on a CLI tool attempting to connect to Elasticsearch, and the
        // tool in question is only in the default distribution.
        assumeTrue(distribution.isDefault());

        runContainer(distribution(), null, Map.of("http.host", "this.is.not.valid"));

        // This will fail if the env var above is passed as a -E argument
        final Result result = sh.runIgnoreExitCode("elasticsearch-setup-passwords auto");

        assertFalse("elasticsearch-setup-passwords command should have failed", result.isSuccess());
        assertThat(result.stdout, containsString("java.net.UnknownHostException: this.is.not.valid: Name or service not known"));
    }

    /**
     * Check whether the elasticsearch-certutil tool has been shipped correctly,
     * and if present then it can execute.
     */
    public void test090SecurityCliPackaging() {
        final Installation.Executables bin = installation.executables();

        final Path securityCli = installation.lib.resolve("tools").resolve("security-cli");

        if (distribution().isDefault()) {
            assertTrue(existsInContainer(securityCli));

            Result result = sh.run(bin.certutilTool + " --help");
            assertThat(result.stdout, containsString("Simplifies certificate creation for use with the Elastic Stack"));

            // Ensure that the exit code from the java command is passed back up through the shell script
            result = sh.runIgnoreExitCode(bin.certutilTool + " invalid-command");
            assertThat(result.isSuccess(), is(false));
            assertThat(result.stdout, containsString("Unknown command [invalid-command]"));
        } else {
            assertFalse(existsInContainer(securityCli));
        }
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

    /**
     * Check that the Docker image has the expected "Label Schema" labels.
     * @see <a href="http://label-schema.org/">Label Schema website</a>
     */
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

    /**
     * Check that the Docker image has the expected "Open Containers Annotations" labels.
     * @see <a href="https://github.com/opencontainers/image-spec/blob/master/annotations.md">Open Containers Annotations</a>
     */
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
     * Check that the Java process running inside the container has the expected UID, GID and username.
     */
    public void test130JavaHasCorrectOwnership() {
        final List<String> processes = sh.run("ps -o uid,gid,user -C java").stdout.lines().skip(1).collect(Collectors.toList());

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
        final List<String> processes = sh.run("ps -o pid,uid,gid,user -p 1").stdout.lines().skip(1).collect(Collectors.toList());

        assertThat("Expected a single process", processes, hasSize(1));

        final String[] fields = processes.get(0).trim().split("\\s+");

        assertThat(fields, arrayWithSize(4));
        assertThat("Incorrect PID", fields[0], equalTo("1"));
        assertThat("Incorrect UID", fields[1], equalTo("1000"));
        assertThat("Incorrect GID", fields[2], equalTo("0"));
        assertThat("Incorrect username", fields[3], equalTo("elasticsearch"));
    }

    /**
     * Check that Elasticsearch reports per-node cgroup information.
     */
    public void test140CgroupOsStatsAreAvailable() throws Exception {
        waitForElasticsearch(installation);

        final JsonNode nodes = getJson("_nodes/stats/os").get("nodes");

        final String nodeId = nodes.fieldNames().next();

        final JsonNode cgroupStats = nodes.at("/" + nodeId + "/os/cgroup");
        assertFalse("Couldn't find /nodes/{nodeId}/os/cgroup in API response", cgroupStats.isMissingNode());

        assertThat("Failed to find [cpu] in node OS cgroup stats", cgroupStats.get("cpu"), not(nullValue()));
        assertThat("Failed to find [cpuacct] in node OS cgroup stats", cgroupStats.get("cpuacct"), not(nullValue()));
    }
}
