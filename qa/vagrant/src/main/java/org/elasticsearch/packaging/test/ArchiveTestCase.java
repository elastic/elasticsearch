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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
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
import static org.elasticsearch.packaging.util.FileUtils.rm;
import static org.elasticsearch.packaging.util.ServerUtils.makeRequest;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
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

    public void test30AbortWhenJavaMissing() {
        assumeThat(installation, is(notNullValue()));

        final Installation.Executables bin = installation.executables();
        final Shell sh = new Shell();

        Platforms.onWindows(() -> {
            // on windows, removing java from PATH and removing JAVA_HOME is less involved than changing the permissions of the java
            // executable. we also don't check permissions in the windows scripts anyway
            final String originalPath = sh.run("$Env:PATH").stdout.trim();
            final String newPath = Arrays.stream(originalPath.split(";"))
                .filter(path -> path.contains("Java") == false)
                .collect(joining(";"));

            // note the lack of a $ when clearing the JAVA_HOME env variable - with a $ it deletes the java home directory
            // https://docs.microsoft.com/en-us/powershell/module/microsoft.powershell.core/providers/environment-provider?view=powershell-6
            //
            // this won't persist to another session so we don't have to reset anything
            final Result runResult = sh.runIgnoreExitCode(
                "$Env:PATH = '" + newPath + "'; " +
                "Remove-Item Env:JAVA_HOME; " +
                bin.elasticsearch
            );

            assertThat(runResult.exitCode, is(1));
            assertThat(runResult.stderr, containsString("could not find java; set JAVA_HOME or ensure java is in PATH"));
        });

        Platforms.onLinux(() -> {
            final String javaPath = sh.run("command -v java").stdout.trim();

            try {
                sh.run("chmod -x '" + javaPath + "'");
                final Result runResult = sh.runIgnoreExitCode(bin.elasticsearch.toString());
                assertThat(runResult.exitCode, is(1));
                assertThat(runResult.stderr, containsString("could not find java; set JAVA_HOME or ensure java is in PATH"));
            } finally {
                sh.run("chmod +x '" + javaPath + "'");
            }
        });
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

        final String gcLogName = Platforms.LINUX
            ? "gc.log.0.current"
            : "gc.log";
        assertTrue("gc logs exist", Files.exists(installation.logs.resolve(gcLogName)));
        ServerUtils.runElasticsearchTests();

        Archives.stopElasticsearch(installation);
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

        if (distribution().equals(Distribution.DEFAULT_TAR) || distribution().equals(Distribution.DEFAULT_ZIP)) {
            assertTrue(Files.exists(installation.lib.resolve("tools").resolve("security-cli")));
            Platforms.onLinux(() -> {
                final Result result = sh.run(bin.elasticsearchCertutil + " help");
                assertThat(result.stdout, containsString("Simplifies certificate creation for use with the Elastic Stack"));
            });

            Platforms.onWindows(() -> {
                final Result result = sh.run(bin.elasticsearchCertutil + " help");
                assertThat(result.stdout, containsString("Simplifies certificate creation for use with the Elastic Stack"));
            });
        } else if (distribution().equals(Distribution.OSS_TAR) || distribution().equals(Distribution.OSS_ZIP)) {
            assertFalse(Files.exists(installation.lib.resolve("tools").resolve("security-cli")));
        }
    }

    public void test100RepairIndexCliPackaging() {
        assumeThat(installation, is(notNullValue()));

        final Installation.Executables bin = installation.executables();
        final Shell sh = new Shell();

        Platforms.PlatformAction action = () -> {
            final Result result = sh.run(bin.elasticsearchShard + " help");
            assertThat(result.stdout, containsString("A CLI tool to remove corrupted parts of unrecoverable shards"));
        };

        if (distribution().equals(Distribution.DEFAULT_TAR) || distribution().equals(Distribution.DEFAULT_ZIP)) {
            Platforms.onLinux(action);
            Platforms.onWindows(action);
        }
    }

}
