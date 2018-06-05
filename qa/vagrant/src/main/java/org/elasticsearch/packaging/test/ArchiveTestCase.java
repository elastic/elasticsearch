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
import org.elasticsearch.packaging.util.Archives;
import org.elasticsearch.packaging.util.Platforms;
import org.elasticsearch.packaging.util.ServerUtils;
import org.elasticsearch.packaging.util.Shell;
import org.elasticsearch.packaging.util.Shell.Result;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import org.elasticsearch.packaging.util.Distribution;
import org.elasticsearch.packaging.util.Installation;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static org.elasticsearch.packaging.util.Archives.ARCHIVE_OWNER;
import static org.elasticsearch.packaging.util.Cleanup.cleanEverything;
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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isEmptyString;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;
import static org.junit.Assume.assumeTrue;

/**
 * Tests that apply to the archive distributions (tar, zip). To add a case for a distribution, subclass and
 * override {@link ArchiveTestCase#distribution()}. These tests should be the same across all archive distributions
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public abstract class ArchiveTestCase {

    private static Installation installation;

    /** The {@link Distribution} that should be tested in this case */
    protected abstract Distribution distribution();

    @BeforeClass
    public static void cleanup() {
        installation = null;
        cleanEverything();
    }

    @Before
    public void onlyCompatibleDistributions() {
        assumeTrue("only compatible distributions", distribution().packaging.compatible);
    }

    @Test
    public void test10Install() {
        installation = installArchive(distribution());
        verifyArchiveInstallation(installation, distribution());
    }

    @Test
    public void test20PluginsListWithNoPlugins() {
        assumeThat(installation, is(notNullValue()));

        final Shell sh = new Shell();
        final Result r = Platforms.WINDOWS
            ? sh.powershell(installation.bin("elasticsearch-plugin.bat") + " list")
            : sh.bash(installation.bin("elasticsearch-plugin") + " list");

        assertThat(r.stdout, isEmptyString());
    }

    @Test
    public void test30AbortWhenJavaMissing() {
        assumeThat(installation, is(notNullValue()));

        final Shell sh = new Shell();
        if (Platforms.WINDOWS) {
            // on windows, removing java from PATH and removing JAVA_HOME is less involved than changing the permissions of the java
            // executable. we also don't check permissions in the windows scripts anyway
            final String originalPath = sh.powershell("$Env:PATH").stdout.trim();
            final String newPath = Arrays.stream(originalPath.split(";"))
                .filter(path -> path.contains("Java") == false)
                .collect(joining(";"));

            final String javaHome = sh.powershell("$Env:JAVA_HOME").stdout.trim();

            // note the lack of a $ when clearing the JAVA_HOME env variable - with a $ it deletes the java home directory
            // https://docs.microsoft.com/en-us/powershell/module/microsoft.powershell.core/providers/environment-provider?view=powershell-6
            //
            // this won't persist to another session so we don't have to reset anything
            final Result runResult = sh.powershellIgnoreExitCode(
                "$Env:PATH = '" + newPath + "'; " +
                "Remove-Item Env:JAVA_HOME; " +
                installation.bin("elasticsearch.bat")
            );

            assertThat(runResult.exitCode, is(1));
            assertThat(runResult.stderr, containsString("could not find java; set JAVA_HOME or ensure java is in PATH"));

        } else {
            final String javaPath = sh.bash("which java").stdout.trim();
            sh.bash("chmod -x '" + javaPath + "'");
            final Result runResult = sh.bashIgnoreExitCode(installation.bin("elasticsearch").toString());
            sh.bash("chmod +x '" + javaPath + "'");

            assertThat(runResult.exitCode, is(1));
            assertThat(runResult.stdout, containsString("could not find java; set JAVA_HOME or ensure java is in PATH"));
        }
    }

    @Test
    public void test40CreateKeystoreManually() {
        assumeThat(installation, is(notNullValue()));

        final Shell sh = new Shell();
        if (Platforms.WINDOWS) {
            // this is a hack around the fact that we can't run a command in the same session as the same user but not as administrator.
            // the keystore ends up being owned by the Administrators group, so we manually set it to be owned by the vagrant user here.
            // from the server's perspective the permissions aren't really different, this is just to reflect what we'd expect in the tests.
            // when we run these commands as a role user we won't have to do this
            sh.powershell(
                installation.bin("elasticsearch-keystore.bat") + " create; " +
                "$account = New-Object System.Security.Principal.NTAccount 'vagrant'; " +
                "$acl = Get-Acl '" + installation.config("elasticsearch.keystore") + "'; " +
                "$acl.SetOwner($account); " +
                "Set-Acl '" + installation.config("elasticsearch.keystore") + "' $acl"
            );
        } else {
            sh.bash("sudo -u " + ARCHIVE_OWNER + " " + installation.bin("elasticsearch-keystore") + " create");
        }

        assertThat(installation.config("elasticsearch.keystore"), file(File, ARCHIVE_OWNER, ARCHIVE_OWNER, p660));

        final Result r = Platforms.WINDOWS
            ? sh.powershell(installation.bin("elasticsearch-keystore.bat") + " list")
            : sh.bash("sudo -u " + ARCHIVE_OWNER + " " + installation.bin("elasticsearch-keystore") + " list");
        assertThat(r.stdout, containsString("keystore.seed"));

        // cleanup for next test
        rm(installation.config("elasticsearch.keystore"));
    }

    @Test
    public void test50StartAndStop() {
        assumeThat(installation, is(notNullValue()));

        Archives.runElasticsearch(installation);

        final String gcLogName = Platforms.LINUX
            ? "gc.log.0.current"
            : "gc.log";
        assertTrue("gc logs exist", Files.exists(installation.logs.resolve(gcLogName)));
        ServerUtils.runElasticsearchTests();

        Archives.stopElasticsearch(installation);
    }

    @Test
    public void test60AutoCreateKeystore() {
        assumeThat(installation, is(notNullValue()));

        assertThat(installation.config("elasticsearch.keystore"), file(File, ARCHIVE_OWNER, ARCHIVE_OWNER, p660));

        final Shell sh = new Shell();
        final Result result;
        if (Platforms.WINDOWS) {
            result = sh.powershell(installation.bin("elasticsearch-keystore.bat") + " list");
        } else {
            result = sh.bash("sudo -u " + ARCHIVE_OWNER + " " + installation.bin("elasticsearch-keystore") + " list");
        }
        assertThat(result.stdout, containsString("keystore.seed"));
    }

    @Test
    public void test70CustomPathConfAndJvmOptions() {
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
            if (Platforms.WINDOWS) {
                sh.powershell(
                    "$account = New-Object System.Security.Principal.NTAccount 'vagrant'; " +
                    "$tempConf = Get-ChildItem '" + tempConf + "' -Recurse; " +
                    "$tempConf += Get-Item '" + tempConf + "'; " +
                    "$tempConf | ForEach-Object { " +
                        "$acl = Get-Acl $_.FullName; " +
                        "$acl.SetOwner($account); " +
                        "Set-Acl $_.FullName $acl " +
                    "}"
                );
            } else {
                sh.bash("chown -R elasticsearch:elasticsearch " + tempConf);
            }

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

    @Test
    public void test80RelativePathConf() {
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
            if (Platforms.WINDOWS) {
                sh.powershell(
                    "$account = New-Object System.Security.Principal.NTAccount 'vagrant'; " +
                    "$tempConf = Get-ChildItem '" + temp + "' -Recurse; " +
                    "$tempConf += Get-Item '" + temp + "'; " +
                    "$tempConf | ForEach-Object { " +
                        "$acl = Get-Acl $_.FullName; " +
                        "$acl.SetOwner($account); " +
                        "Set-Acl $_.FullName $acl " +
                    "}"
                );
            } else {
                sh.bash("chown -R elasticsearch:elasticsearch " + temp);
            }

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


}
