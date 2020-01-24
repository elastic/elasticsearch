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

import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import org.apache.http.client.fluent.Request;
import org.elasticsearch.packaging.util.FileUtils;
import org.elasticsearch.packaging.util.Packages;
import org.elasticsearch.packaging.util.Shell.Result;
import org.junit.BeforeClass;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.carrotsearch.randomizedtesting.RandomizedTest.getRandom;
import static org.elasticsearch.packaging.util.FileUtils.append;
import static org.elasticsearch.packaging.util.FileUtils.assertPathsDontExist;
import static org.elasticsearch.packaging.util.FileUtils.assertPathsExist;
import static org.elasticsearch.packaging.util.FileUtils.cp;
import static org.elasticsearch.packaging.util.FileUtils.fileWithGlobExist;
import static org.elasticsearch.packaging.util.FileUtils.mkdir;
import static org.elasticsearch.packaging.util.FileUtils.mv;
import static org.elasticsearch.packaging.util.FileUtils.rm;
import static org.elasticsearch.packaging.util.FileUtils.slurp;
import static org.elasticsearch.packaging.util.Packages.SYSTEMD_SERVICE;
import static org.elasticsearch.packaging.util.Packages.assertInstalled;
import static org.elasticsearch.packaging.util.Packages.assertRemoved;
import static org.elasticsearch.packaging.util.Packages.installPackage;
import static org.elasticsearch.packaging.util.Packages.remove;
import static org.elasticsearch.packaging.util.Packages.restartElasticsearch;
import static org.elasticsearch.packaging.util.Packages.verifyPackageInstallation;
import static org.elasticsearch.packaging.util.Platforms.getOsRelease;
import static org.elasticsearch.packaging.util.Platforms.isSystemd;
import static org.elasticsearch.packaging.util.ServerUtils.makeRequest;
import static org.elasticsearch.packaging.util.ServerUtils.runElasticsearchTests;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.core.Is.is;
import static org.junit.Assume.assumeThat;
import static org.junit.Assume.assumeTrue;

public class PackageTests extends PackagingTestCase {

    @BeforeClass
    public static void filterDistros() {
        assumeTrue("rpm or deb", distribution.isPackage());
    }

    public void test10InstallPackage() throws Exception {
        assertRemoved(distribution());
        installation = installPackage(sh, distribution());
        assertInstalled(distribution());
        verifyPackageInstallation(installation, distribution(), sh);
    }

    public void test20PluginsCommandWhenNoPlugins() {
        assertThat(sh.run(installation.bin("elasticsearch-plugin") + " list").stdout, is(emptyString()));
    }

    public void test30DaemonIsNotEnabledOnRestart() {
        if (isSystemd()) {
            sh.run("systemctl daemon-reload");
            String isEnabledOutput = sh.runIgnoreExitCode("systemctl is-enabled elasticsearch.service").stdout.trim();
            assertThat(isEnabledOutput, equalTo("disabled"));
        }
    }

    public void test31InstallDoesNotStartServer() {
        assertThat(sh.run("ps aux").stdout, not(containsString("org.elasticsearch.bootstrap.Elasticsearch")));
    }

    private void assertRunsWithJavaHome() throws Exception {
        byte[] originalEnvFile = Files.readAllBytes(installation.envFile);
        try {
            Files.write(installation.envFile, ("JAVA_HOME=" + systemJavaHome + "\n").getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.APPEND);
            startElasticsearch();
            runElasticsearchTests();
            stopElasticsearch();
        } finally {
            Files.write(installation.envFile, originalEnvFile);
        }

        assertThat(FileUtils.slurpAllLogs(installation.logs, "elasticsearch.log", "elasticsearch*.log.gz"),
            containsString(systemJavaHome));
    }

    public void test32JavaHomeOverride() throws Exception {
        // we always run with java home when no bundled jdk is included, so this test would be repetitive
        assumeThat(distribution().hasJdk, is(true));

        assertRunsWithJavaHome();
    }

    public void test33RunsIfJavaNotOnPath() throws Exception {
        assumeThat(distribution().hasJdk, is(true));

        // we don't require java be installed but some images have it
        String backupPath = "/usr/bin/java." + getClass().getSimpleName() + ".bak";
        if (Files.exists(Paths.get("/usr/bin/java"))) {
            sh.run("sudo mv /usr/bin/java " + backupPath);
        }

        try {
            startElasticsearch();
            runElasticsearchTests();
            stopElasticsearch();
        } finally {
            if (Files.exists(Paths.get(backupPath))) {
                sh.run("sudo mv " + backupPath + " /usr/bin/java");
            }
        }
    }

    public void test42BundledJdkRemoved() throws Exception {
        assumeThat(distribution().hasJdk, is(true));

        Path relocatedJdk = installation.bundledJdk.getParent().resolve("jdk.relocated");
        try {
            mv(installation.bundledJdk, relocatedJdk);
            assertRunsWithJavaHome();
        } finally {
            mv(relocatedJdk, installation.bundledJdk);
        }
    }

    public void test40StartServer() throws Exception {
        String start = sh.runIgnoreExitCode("date ").stdout.trim();
        startElasticsearch();

        String journalEntries = sh.runIgnoreExitCode("journalctl _SYSTEMD_UNIT=elasticsearch.service " +
            "--since \"" + start + "\" --output cat | wc -l").stdout.trim();
        assertThat(journalEntries, equalTo("0"));

        assertPathsExist(installation.pidDir.resolve("elasticsearch.pid"));
        assertPathsExist(installation.logs.resolve("elasticsearch_server.json"));

        runElasticsearchTests();
        verifyPackageInstallation(installation, distribution(), sh); // check startup script didn't change permissions
        stopElasticsearch();
    }

    public void test50Remove() throws Exception {
        // add fake bin directory as if a plugin was installed
        Files.createDirectories(installation.bin.resolve("myplugin"));

        remove(distribution());

        // removing must stop the service
        assertThat(sh.run("ps aux").stdout, not(containsString("org.elasticsearch.bootstrap.Elasticsearch")));

        if (isSystemd()) {

            final int statusExitCode;

            // Before version 231 systemctl returned exit code 3 for both services that were stopped, and nonexistent
            // services [1]. In version 231 and later it returns exit code 4 for non-existent services.
            //
            // The exception is Centos 7 and oel 7 where it returns exit code 4 for non-existent services from a systemd reporting a version
            // earlier than 231. Centos 6 does not have an /etc/os-release, but that's fine because it also doesn't use systemd.
            //
            // [1] https://github.com/systemd/systemd/pull/3385
            if (getOsRelease().contains("ID=\"centos\"") || getOsRelease().contains("ID=\"ol\"")) {
                statusExitCode = 4;
            } else {

                final Result versionResult = sh.run("systemctl --version");
                final Matcher matcher = Pattern.compile("^systemd (\\d+)").matcher(versionResult.stdout);
                matcher.find();
                final int version = Integer.parseInt(matcher.group(1));

                statusExitCode = version < 231
                    ? 3
                    : 4;
            }

            assertThat(sh.runIgnoreExitCode("systemctl status elasticsearch.service").exitCode, is(statusExitCode));
            assertThat(sh.runIgnoreExitCode("systemctl is-enabled elasticsearch.service").exitCode, is(1));

        }

        assertPathsDontExist(
            installation.bin,
            installation.lib,
            installation.modules,
            installation.plugins,
            installation.logs,
            installation.pidDir
        );

        assertFalse(Files.exists(SYSTEMD_SERVICE));
    }

    public void test60Reinstall() throws Exception {
        install();
        assertInstalled(distribution());
        verifyPackageInstallation(installation, distribution(), sh);

        remove(distribution());
        assertRemoved(distribution());
    }

    public void test70RestartServer() throws Exception {
        try {
            install();
            assertInstalled(distribution());

            startElasticsearch();
            restartElasticsearch(sh, installation);
            runElasticsearchTests();
            stopElasticsearch();
        } finally {
            cleanup();
        }
    }


    public void test72TestRuntimeDirectory() throws Exception {
        try {
            install();
            FileUtils.rm(installation.pidDir);
            startElasticsearch();
            assertPathsExist(installation.pidDir);
            stopElasticsearch();
        } finally {
            cleanup();
        }
    }

    public void test73gcLogsExist() throws Exception {
        install();
        startElasticsearch();
        // it can be gc.log or gc.log.0.current
        assertThat(installation.logs, fileWithGlobExist("gc.log*"));
        stopElasticsearch();
    }

    // TEST CASES FOR SYSTEMD ONLY


    /**
     * # Simulates the behavior of a system restart:
     * # the PID directory is deleted by the operating system
     * # but it should not block ES from starting
     * # see https://github.com/elastic/elasticsearch/issues/11594
     */
    public void test80DeletePID_DIRandRestart() throws Exception {
        assumeTrue(isSystemd());

        rm(installation.pidDir);

        sh.run("systemd-tmpfiles --create");

        startElasticsearch();

        final Path pidFile = installation.pidDir.resolve("elasticsearch.pid");

        assertTrue(Files.exists(pidFile));

        stopElasticsearch();
    }

    public void test81CustomPathConfAndJvmOptions() throws Exception {
        withCustomConfig(tempConf -> {
            append(installation.envFile, "ES_JAVA_OPTS=-XX:-UseCompressedOops");

            startElasticsearch();

            final String nodesResponse = makeRequest(Request.Get("http://localhost:9200/_nodes"));
            assertThat(nodesResponse, containsString("\"heap_init_in_bytes\":536870912"));
            assertThat(nodesResponse, containsString("\"using_compressed_ordinary_object_pointers\":\"false\""));

            stopElasticsearch();
        });
    }

    public void test82SystemdMask() throws Exception {
        try {
            assumeTrue(isSystemd());

            sh.run("systemctl mask systemd-sysctl.service");
            install();

            sh.run("systemctl unmask systemd-sysctl.service");
        } finally {
            cleanup();
        }
    }

    public void test83serviceFileSetsLimits() throws Exception {
        // Limits are changed on systemd platforms only
        assumeTrue(isSystemd());

        install();

        startElasticsearch();

        final Path pidFile = installation.pidDir.resolve("elasticsearch.pid");
        assertTrue(Files.exists(pidFile));
        String pid = slurp(pidFile).trim();
        String maxFileSize = sh.run("cat /proc/%s/limits | grep \"Max file size\" | awk '{ print $4 }'", pid).stdout.trim();
        assertThat(maxFileSize, equalTo("unlimited"));

        String maxProcesses = sh.run("cat /proc/%s/limits | grep \"Max processes\" | awk '{ print $3 }'", pid).stdout.trim();
        assertThat(maxProcesses, equalTo("4096"));

        String maxOpenFiles = sh.run("cat /proc/%s/limits | grep \"Max open files\" | awk '{ print $4 }'", pid).stdout.trim();
        assertThat(maxOpenFiles, equalTo("65535"));

        String maxAddressSpace = sh.run("cat /proc/%s/limits | grep \"Max address space\" | awk '{ print $4 }'", pid).stdout.trim();
        assertThat(maxAddressSpace, equalTo("unlimited"));

        stopElasticsearch();
    }

    public void test90DoNotCloseStderrWhenQuiet() throws Exception {
        withCustomConfig(tempConf -> {
            // Create a startup problem by adding an invalid YAML line to the config
            append(tempConf.resolve("elasticsearch.yml"), "discovery.zen.ping.unicast.hosts:15172.30.5.3416172.30.5.35, 172.30.5.17]\n");

            // Make sure we don't pick up the journal entries for previous ES instances.
            Packages.JournaldWrapper journald = new Packages.JournaldWrapper(sh);
            runElasticsearchStartCommand();
            final Result logs = journald.getLogs();

            assertThat(logs.stdout, containsString("Failed to load settings from [elasticsearch.yml]"));
        });
    }

    @FunctionalInterface
    private interface CustomConfigConsumer {
        void accept(Path path) throws Exception;
    }

    private void withCustomConfig(CustomConfigConsumer pathConsumer) throws Exception {
        assumeTrue(isSystemd());

        assertPathsExist(installation.envFile);

        stopElasticsearch();

        // The custom config directory is not under /tmp or /var/tmp because
        // systemd's private temp directory functionally means different
        // processes can have different views of what's in these directories
        String randomName = RandomStrings.randomAsciiAlphanumOfLength(getRandom(), 10);
        sh.run("mkdir /etc/" + randomName);
        final Path tempConf = Paths.get("/etc/" + randomName);

        try {
            mkdir(tempConf);
            cp(installation.config("elasticsearch.yml"), tempConf.resolve("elasticsearch.yml"));
            cp(installation.config("log4j2.properties"), tempConf.resolve("log4j2.properties"));

            // we have to disable Log4j from using JMX lest it will hit a security
            // manager exception before we have configured logging; this will fail
            // startup since we detect usages of logging before it is configured
            final String jvmOptions = "-Xms512m\n-Xmx512m\n-Dlog4j2.disable.jmx=true\n";
            append(tempConf.resolve("jvm.options"), jvmOptions);

            sh.runIgnoreExitCode("chown -R elasticsearch:elasticsearch " + tempConf);

            cp(installation.envFile, tempConf.resolve("elasticsearch.bk"));// backup
            append(installation.envFile, "ES_PATH_CONF=" + tempConf + "\n");

            pathConsumer.accept(tempConf);
        } finally {
            rm(installation.envFile);
            cp(tempConf.resolve("elasticsearch.bk"), installation.envFile);
            rm(tempConf);
            cleanup();
        }
    }
}
