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

import com.carrotsearch.randomizedtesting.annotations.TestCaseOrdering;
import org.apache.http.client.fluent.Request;
import org.elasticsearch.packaging.util.Archives;
import org.elasticsearch.packaging.util.Distribution;
import org.elasticsearch.packaging.util.Installation;
import org.elasticsearch.packaging.util.Platforms;
import org.elasticsearch.packaging.util.ServerUtils;
import org.elasticsearch.packaging.util.Shell;
import org.elasticsearch.packaging.util.Shell.Result;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.elasticsearch.packaging.util.Archives.ARCHIVE_OWNER;
import static org.elasticsearch.packaging.util.Archives.installArchive;
import static org.elasticsearch.packaging.util.Archives.verifyArchiveInstallation;
import static org.elasticsearch.packaging.util.FileMatcher.Fileness.File;
import static org.elasticsearch.packaging.util.FileMatcher.file;
import static org.elasticsearch.packaging.util.FileMatcher.p660;
import static org.elasticsearch.packaging.util.FileUtils.append;
import static org.elasticsearch.packaging.util.FileUtils.cp;
import static org.elasticsearch.packaging.util.FileUtils.getTempDir;
import static org.elasticsearch.packaging.util.FileUtils.mkdir;
import static org.elasticsearch.packaging.util.FileUtils.mv;
import static org.elasticsearch.packaging.util.FileUtils.rm;
import static org.elasticsearch.packaging.util.ServerUtils.makeRequest;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.isEmptyString;
import static org.junit.Assume.assumeThat;

/**
 * Tests that apply to the archive distributions (tar, zip). To add a case for a distribution, subclass and
 * override {@link ArchiveTestCase#distribution()}. These tests should be the same across all archive distributions
 */
@TestCaseOrdering(TestCaseOrdering.AlphabeticOrder.class)
public abstract class ArchiveTestCase extends PackagingTestCase {

    public void test10Install() {
        installation = installArchive(distribution());
        verifyArchiveInstallation(installation, distribution());
    }

    public void test20PluginsListWithNoPlugins() {
        assumeThat(installation, is(notNullValue()));

        final Installation.Executables bin = installation.executables();
        final Shell sh = new Shell();
        final Result r = sh.run(bin.elasticsearchPlugin + " list");

        assertThat(r.stdout, isEmptyString());
    }

    public void test30NoJava() {
        assumeThat(installation, is(notNullValue()));

        final Installation.Executables bin = installation.executables();
        final Shell sh = new Shell();

        final Path relocatedJdk = installation.bundledJdk.getParent().resolve("jdk.relocated");

        try {
            mv(installation.bundledJdk, relocatedJdk);
            // ask for elasticsearch version to quickly exit if java is actually found (ie test failure)
            final Result runResult = sh.runIgnoreExitCode(bin.elasticsearch.toString() + " -v");
            assertThat(runResult.exitCode, is(1));
            assertThat(runResult.stderr, containsString("could not find java in JAVA_HOME or bundled"));
        } finally {
            mv(relocatedJdk, installation.bundledJdk);
        }
    }

    public void test40CreateKeystoreManually() {
        assumeThat(installation, is(notNullValue()));

        final Installation.Executables bin = installation.executables();
        final Shell sh = new Shell();

        Platforms.onLinux(() -> sh.run("sudo -u " + ARCHIVE_OWNER + " " + bin.elasticsearchKeystore + " create"));

        // this is a hack around the fact that we can't run a command in the same session as the same user but not as administrator.
        // the keystore ends up being owned by the Administrators group, so we manually set it to be owned by the vagrant user here.
        // from the server's perspective the permissions aren't really different, this is just to reflect what we'd expect in the tests.
        // when we run these commands as a role user we won't have to do this
        Platforms.onWindows(() -> sh.run(
                bin.elasticsearchKeystore + " create; " +
                "$account = New-Object System.Security.Principal.NTAccount 'vagrant'; " +
                "$acl = Get-Acl '" + installation.config("elasticsearch.keystore") + "'; " +
                "$acl.SetOwner($account); " +
                "Set-Acl '" + installation.config("elasticsearch.keystore") + "' $acl"
        ));

        assertThat(installation.config("elasticsearch.keystore"), file(File, ARCHIVE_OWNER, ARCHIVE_OWNER, p660));

        Platforms.onLinux(() -> {
            final Result r = sh.run("sudo -u " + ARCHIVE_OWNER + " " + bin.elasticsearchKeystore + " list");
            assertThat(r.stdout, containsString("keystore.seed"));
        });

        Platforms.onWindows(() -> {
            final Result r = sh.run(bin.elasticsearchKeystore + " list");
            assertThat(r.stdout, containsString("keystore.seed"));
        });
    }

    public void test50StartAndStop() throws IOException {
        assumeThat(installation, is(notNullValue()));

        // cleanup from previous test
        rm(installation.config("elasticsearch.keystore"));

        Archives.runElasticsearch(installation);

        assertTrue("gc logs exist", Files.exists(installation.logs.resolve("gc.log")));
        ServerUtils.runElasticsearchTests();

        Archives.stopElasticsearch(installation);
    }

    public void assertRunsWithJavaHome() throws IOException {
        Shell sh = new Shell();

        Platforms.onLinux(() -> {
            String systemJavaHome = sh.run("echo $SYSTEM_JAVA_HOME").stdout.trim();
            sh.getEnv().put("JAVA_HOME", systemJavaHome);
        });
        Platforms.onWindows(() -> {
            final String systemJavaHome = sh.run("$Env:SYSTEM_JAVA_HOME").stdout.trim();
            sh.getEnv().put("JAVA_HOME", systemJavaHome);
        });

        Archives.runElasticsearch(installation, sh);
        ServerUtils.runElasticsearchTests();
        Archives.stopElasticsearch(installation);

        String systemJavaHome = sh.getEnv().get("JAVA_HOME");
        Path log = installation.logs.resolve("elasticsearch.log");
        assertThat(new String(Files.readAllBytes(log), StandardCharsets.UTF_8), containsString(systemJavaHome));
    }

    public void test51JavaHomeOverride() throws IOException {
        assumeThat(installation, is(notNullValue()));

        assertRunsWithJavaHome();
    }

    public void test52BundledJdkRemoved() throws IOException {
        assumeThat(installation, is(notNullValue()));

        Path relocatedJdk = installation.bundledJdk.getParent().resolve("jdk.relocated");
        try {
            mv(installation.bundledJdk, relocatedJdk);
            assertRunsWithJavaHome();
        } finally {
            mv(relocatedJdk, installation.bundledJdk);
        }
    }

    public void test60AutoCreateKeystore() {
        assumeThat(installation, is(notNullValue()));

        assertThat(installation.config("elasticsearch.keystore"), file(File, ARCHIVE_OWNER, ARCHIVE_OWNER, p660));

        final Installation.Executables bin = installation.executables();
        final Shell sh = new Shell();

        Platforms.onLinux(() -> {
            final Result result = sh.run("sudo -u " + ARCHIVE_OWNER + " " + bin.elasticsearchKeystore + " list");
            assertThat(result.stdout, containsString("keystore.seed"));
        });

        Platforms.onWindows(() -> {
            final Result result = sh.run(bin.elasticsearchKeystore + " list");
            assertThat(result.stdout, containsString("keystore.seed"));
        });
    }

    public void test70CustomPathConfAndJvmOptions() throws IOException {
        assumeThat(installation, is(notNullValue()));

        final Path tempConf = getTempDir().resolve("esconf-alternate");

        try {
            mkdir(tempConf);
            cp(installation.config("elasticsearch.yml"), tempConf.resolve("elasticsearch.yml"));
            cp(installation.config("log4j2.properties"), tempConf.resolve("log4j2.properties"));

            // we have to disable Log4j from using JMX lest it will hit a security
            // manager exception before we have configured logging; this will fail
            // startup since we detect usages of logging before it is configured
            final String jvmOptions =
                "-Xms512m\n" +
                "-Xmx512m\n" +
                "-Dlog4j2.disable.jmx=true\n";
            append(tempConf.resolve("jvm.options"), jvmOptions);

            final Shell sh = new Shell();
            Platforms.onLinux(() -> sh.run("chown -R elasticsearch:elasticsearch " + tempConf));
            Platforms.onWindows(() -> sh.run(
                "$account = New-Object System.Security.Principal.NTAccount 'vagrant'; " +
                "$tempConf = Get-ChildItem '" + tempConf + "' -Recurse; " +
                "$tempConf += Get-Item '" + tempConf + "'; " +
                "$tempConf | ForEach-Object { " +
                    "$acl = Get-Acl $_.FullName; " +
                    "$acl.SetOwner($account); " +
                    "Set-Acl $_.FullName $acl " +
                "}"
            ));

            final Shell serverShell = new Shell();
            serverShell.getEnv().put("ES_PATH_CONF", tempConf.toString());
            serverShell.getEnv().put("ES_JAVA_OPTS", "-XX:-UseCompressedOops");

            Archives.runElasticsearch(installation, serverShell);

            final String nodesResponse = makeRequest(Request.Get("http://localhost:9200/_nodes"));
            assertThat(nodesResponse, containsString("\"heap_init_in_bytes\":536870912"));
            assertThat(nodesResponse, containsString("\"using_compressed_ordinary_object_pointers\":\"false\""));

            Archives.stopElasticsearch(installation);

        } finally {
            rm(tempConf);
        }
    }

    public void test80RelativePathConf() throws IOException {
        assumeThat(installation, is(notNullValue()));

        final Path temp = getTempDir().resolve("esconf-alternate");
        final Path tempConf = temp.resolve("config");

        try {
            mkdir(tempConf);
            Stream.of(
                "elasticsearch.yml",
                "log4j2.properties",
                "jvm.options"
            ).forEach(file -> cp(installation.config(file), tempConf.resolve(file)));

            append(tempConf.resolve("elasticsearch.yml"), "node.name: relative");

            final Shell sh = new Shell();
            Platforms.onLinux(() -> sh.run("chown -R elasticsearch:elasticsearch " + temp));
            Platforms.onWindows(() -> sh.run(
                "$account = New-Object System.Security.Principal.NTAccount 'vagrant'; " +
                "$tempConf = Get-ChildItem '" + temp + "' -Recurse; " +
                "$tempConf += Get-Item '" + temp + "'; " +
                "$tempConf | ForEach-Object { " +
                    "$acl = Get-Acl $_.FullName; " +
                    "$acl.SetOwner($account); " +
                    "Set-Acl $_.FullName $acl " +
                "}"
            ));

            final Shell serverShell = new Shell(temp);
            serverShell.getEnv().put("ES_PATH_CONF", "config");
            Archives.runElasticsearch(installation, serverShell);

            final String nodesResponse = makeRequest(Request.Get("http://localhost:9200/_nodes"));
            assertThat(nodesResponse, containsString("\"name\":\"relative\""));

            Archives.stopElasticsearch(installation);

        } finally {
            rm(tempConf);
        }
    }

    public void test90SecurityCliPackaging() {
        assumeThat(installation, is(notNullValue()));

        final Installation.Executables bin = installation.executables();
        final Shell sh = new Shell();

        if (distribution().equals(Distribution.DEFAULT_LINUX) || distribution().equals(Distribution.DEFAULT_WINDOWS)) {
            assertTrue(Files.exists(installation.lib.resolve("tools").resolve("security-cli")));
            final Platforms.PlatformAction action = () -> {
                Result result = sh.run(bin.elasticsearchCertutil + " --help");
                assertThat(result.stdout, containsString("Simplifies certificate creation for use with the Elastic Stack"));

                // Ensure that the exit code from the java command is passed back up through the shell script
                result = sh.runIgnoreExitCode(bin.elasticsearchCertutil + " invalid-command");
                assertThat(result.exitCode, is(not(0)));
                assertThat(result.stdout, containsString("Unknown command [invalid-command]"));
            };
            Platforms.onLinux(action);
            Platforms.onWindows(action);
        } else if (distribution().equals(Distribution.OSS_LINUX) || distribution().equals(Distribution.OSS_WINDOWS)) {
            assertFalse(Files.exists(installation.lib.resolve("tools").resolve("security-cli")));
        }
    }

    public void test91ElasticsearchShardCliPackaging() {
        assumeThat(installation, is(notNullValue()));

        final Installation.Executables bin = installation.executables();
        final Shell sh = new Shell();

        Platforms.PlatformAction action = () -> {
            final Result result = sh.run(bin.elasticsearchShard + " -h");
            assertThat(result.stdout, containsString("A CLI tool to remove corrupted parts of unrecoverable shards"));
        };

        if (distribution().equals(Distribution.DEFAULT_LINUX) || distribution().equals(Distribution.DEFAULT_WINDOWS)) {
            Platforms.onLinux(action);
            Platforms.onWindows(action);
        }
    }

    public void test92ElasticsearchNodeCliPackaging() {
        assumeThat(installation, is(notNullValue()));

        final Installation.Executables bin = installation.executables();
        final Shell sh = new Shell();

        Platforms.PlatformAction action = () -> {
            final Result result = sh.run(bin.elasticsearchNode + " -h");
            assertThat(result.stdout,
                    containsString("A CLI tool to unsafely recover a cluster after the permanent loss of too many master-eligible nodes"));
        };

        if (distribution().equals(Distribution.DEFAULT_LINUX) || distribution().equals(Distribution.DEFAULT_WINDOWS)) {
            Platforms.onLinux(action);
            Platforms.onWindows(action);
        }
    }

    public void test93ElasticsearchNodeCustomDataPathAndNotEsHomeWorkDir() throws IOException {
        assumeThat(installation, is(notNullValue()));

        Path relativeDataPath = installation.data.relativize(installation.home);
        append(installation.config("elasticsearch.yml"), "path.data: " + relativeDataPath);

        Archives.runElasticsearch(installation);
        Archives.stopElasticsearch(installation);

        final Shell sh = new Shell(getTempDir());

        Result result = sh.run("echo y | " + installation.executables().elasticsearchNode + " unsafe-bootstrap");
        assertThat(result.stdout, containsString("Master node was successfully bootstrapped"));
    }

}
