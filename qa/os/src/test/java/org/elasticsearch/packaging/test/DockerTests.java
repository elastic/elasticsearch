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
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static java.nio.file.attribute.PosixFilePermissions.fromString;
import static org.elasticsearch.packaging.util.Docker.assertPermissionsAndOwnership;
import static org.elasticsearch.packaging.util.Docker.copyFromContainer;
import static org.elasticsearch.packaging.util.Docker.ensureImageIsLoaded;
import static org.elasticsearch.packaging.util.Docker.existsInContainer;
import static org.elasticsearch.packaging.util.Docker.removeContainer;
import static org.elasticsearch.packaging.util.Docker.runContainer;
import static org.elasticsearch.packaging.util.Docker.verifyContainerInstallation;
import static org.elasticsearch.packaging.util.Docker.waitForPathToExist;
import static org.elasticsearch.packaging.util.FileMatcher.p660;
import static org.elasticsearch.packaging.util.FileUtils.append;
import static org.elasticsearch.packaging.util.FileUtils.getTempDir;
import static org.elasticsearch.packaging.util.FileUtils.mkdir;
import static org.elasticsearch.packaging.util.FileUtils.rm;
import static org.elasticsearch.packaging.util.ServerUtils.makeRequest;
import static org.elasticsearch.packaging.util.ServerUtils.waitForElasticsearch;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.emptyString;
import static org.junit.Assume.assumeTrue;

@Ignore("https://github.com/elastic/elasticsearch/issues/47639")
public class DockerTests extends PackagingTestCase {
    protected DockerShell sh;

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
    public void setupTest() throws Exception {
        sh = new DockerShell();
        installation = runContainer(distribution());
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
     * Send some basic index, count and delete requests, in order to check that the installation
     * is minimally functional.
     */
    public void test50BasicApiTests() throws Exception {
        waitForElasticsearch(installation);

        assertTrue(existsInContainer(installation.logs.resolve("gc.log")));

        ServerUtils.runElasticsearchTests();
    }

    /**
     * Check that the default keystore is automatically created
     */
    public void test60AutoCreateKeystore() throws Exception {
        final Path keystorePath = installation.config("elasticsearch.keystore");

        waitForPathToExist(keystorePath);

        assertPermissionsAndOwnership(keystorePath, p660);

        final Installation.Executables bin = installation.executables();
        final Result result = sh.run(bin.elasticsearchKeystore + " list");
        assertThat(result.stdout, containsString("keystore.seed"));
    }

    /**
     * Check that the default config can be overridden using a bind mount, and that env vars are respected
     */
    public void test70BindMountCustomPathConfAndJvmOptions() throws Exception {
        final Path tempConf = getTempDir().resolve("esconf-alternate");

        try {
            mkdir(tempConf);
            copyFromContainer(installation.config("elasticsearch.yml"), tempConf.resolve("elasticsearch.yml"));
            copyFromContainer(installation.config("log4j2.properties"), tempConf.resolve("log4j2.properties"));

            // we have to disable Log4j from using JMX lest it will hit a security
            // manager exception before we have configured logging; this will fail
            // startup since we detect usages of logging before it is configured
            final String jvmOptions =
                "-Xms512m\n" +
                "-Xmx512m\n" +
                "-Dlog4j2.disable.jmx=true\n";
            append(tempConf.resolve("jvm.options"), jvmOptions);

            // Make the temp directory and contents accessible when bind-mounted
            Files.setPosixFilePermissions(tempConf, fromString("rwxrwxrwx"));

            // Restart the container
            removeContainer();
            runContainer(distribution(), tempConf, Map.of(
                "ES_JAVA_OPTS", "-XX:-UseCompressedOops"
            ));

            waitForElasticsearch(installation);

            final String nodesResponse = makeRequest(Request.Get("http://localhost:9200/_nodes"));
            assertThat(nodesResponse, containsString("\"heap_init_in_bytes\":536870912"));
            assertThat(nodesResponse, containsString("\"using_compressed_ordinary_object_pointers\":\"false\""));
        } finally {
            rm(tempConf);
        }
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
        assertThat(result.stdout,
            containsString("A CLI tool to do unsafe cluster and index manipulations on current node"));
    }
}
