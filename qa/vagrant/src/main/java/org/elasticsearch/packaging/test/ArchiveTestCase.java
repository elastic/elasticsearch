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
import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import org.apache.http.client.fluent.Request;
import org.elasticsearch.packaging.util.Archives;
import org.elasticsearch.packaging.util.Distribution;
import org.elasticsearch.packaging.util.FileUtils;
import org.elasticsearch.packaging.util.Installation;
import org.elasticsearch.packaging.util.Platforms;
import org.elasticsearch.packaging.util.ServerUtils;
import org.elasticsearch.packaging.util.Shell;
import org.elasticsearch.packaging.util.Shell.Result;
import org.junit.Before;
import org.junit.BeforeClass;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.stream.Stream;

import static com.carrotsearch.randomizedtesting.RandomizedTest.getRandom;
import static java.util.stream.Collectors.joining;
import static org.elasticsearch.packaging.util.Archives.ARCHIVE_OWNER;
import static org.elasticsearch.packaging.util.Archives.installArchive;
import static org.elasticsearch.packaging.util.Archives.verifyArchiveInstallation;
import static org.elasticsearch.packaging.util.Cleanup.cleanEverything;
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
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isEmptyString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;
import static org.junit.Assume.assumeTrue;

/**
 * Tests that apply to the archive distributions (tar, zip). To add a case for a distribution, subclass and
 * override {@link ArchiveTestCase#distribution()}. These tests should be the same across all archive distributions
 */
@TestCaseOrdering(TestCaseOrdering.AlphabeticOrder.class)
public abstract class ArchiveTestCase extends PackagingTestCase {

    private static Installation installation;

    /** The {@link Distribution} that should be tested in this case */
    protected abstract Distribution distribution();

    @BeforeClass
    public static void cleanup() throws Exception {
        installation = null;
        cleanEverything();
    }

    @Before
    public void onlyCompatibleDistributions() {
        assumeTrue("only compatible distributions", distribution().packaging.compatible);
    }

    public void test10Install() throws Exception {
        installation = installArchive(distribution());
        verifyArchiveInstallation(installation, distribution());
    }

    public void test20PluginsListWithNoPlugins() throws Exception {
        assumeThat(installation, is(notNullValue()));

        final Installation.Executables bin = installation.executables();
        final Shell sh = new Shell();
        final Result r = sh.run(bin.elasticsearchPlugin + " list");

        assertThat(r.stdout, isEmptyString());
    }

    public void test30AbortWhenJavaMissing() throws Exception {
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
            assertThat(runResult.stderr, containsString("could not find java; set JAVA_HOME"));
        });

        Platforms.onLinux(() -> {
            final String javaPath = sh.run("command -v java").stdout.trim();

            try {
                sh.run("chmod -x '" + javaPath + "'");
                final Result runResult = sh.runIgnoreExitCode(bin.elasticsearch.toString());
                assertThat(runResult.exitCode, is(1));
                assertThat(runResult.stderr, containsString("could not find java; set JAVA_HOME"));
            } finally {
                sh.run("chmod +x '" + javaPath + "'");
            }
        });
    }

    public void test40CreateKeystoreManually() throws Exception {
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

    public void test50StartAndStop() throws Exception {
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
    public void test51JavaHomeWithSpecialCharacters() throws Exception {
        assumeThat(installation, is(notNullValue()));

        Platforms.onWindows(() -> {
            final Shell sh = new Shell();
            try {
                // once windows 2012 is no longer supported and powershell 5.0 is always available we can change this command
                sh.run("cmd /c mklink /D 'C:\\Program Files (x86)\\java' $Env:JAVA_HOME");

                sh.getEnv().put("JAVA_HOME", "C:\\Program Files (x86)\\java");

                //verify ES can start, stop and run plugin list
                Archives.runElasticsearch(installation, sh);

                Archives.stopElasticsearch(installation);

                String pluginListCommand = installation.bin + "/elasticsearch-plugin list";
                Result result = sh.run(pluginListCommand);
                assertThat(result.exitCode, equalTo(0));

            } finally {
                //clean up sym link
                sh.run("cmd /c del /F /Q 'C:\\Program Files (x86)\\java' ");
            }
        });

        Platforms.onLinux(() -> {
            final Shell sh = new Shell();
            // Create temporary directory with a space and link to java binary.
            // Use it as java_home
            String nameWithSpace = RandomStrings.randomAsciiAlphanumOfLength(getRandom(), 10) + "java home";
            String test_java_home = FileUtils.mkdir(Paths.get("/home",ARCHIVE_OWNER, nameWithSpace)).toAbsolutePath().toString();
            try {
                final String systemJavaHome = sh.run("echo $SYSTEM_JAVA_HOME").stdout.trim();
                final String java = systemJavaHome + "/bin/java";

                sh.run("mkdir -p \"" + test_java_home + "/bin\"");
                sh.run("ln -s \"" + java + "\" \"" + test_java_home + "/bin/java\"");
                sh.run("chown -R " + ARCHIVE_OWNER + ":" + ARCHIVE_OWNER + " \"" + test_java_home + "\"");

                sh.getEnv().put("JAVA_HOME", test_java_home);

                //verify ES can start, stop and run plugin list
                Archives.runElasticsearch(installation, sh);

                Archives.stopElasticsearch(installation);

                String pluginListCommand = installation.bin + "/elasticsearch-plugin list";
                Result result = sh.run(pluginListCommand);
                assertThat(result.exitCode, equalTo(0));
            } finally {
                FileUtils.rm(Paths.get("\"" + test_java_home + "\""));
            }
        });
    }

    public void test60AutoCreateKeystore() throws Exception {
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

    public void test70CustomPathConfAndJvmOptions() throws Exception {
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

    public void test80RelativePathConf() throws Exception {
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

    public void test90SecurityCliPackaging() throws Exception {
        assumeThat(installation, is(notNullValue()));

        final Installation.Executables bin = installation.executables();
        final Shell sh = new Shell();

        if (distribution().equals(Distribution.DEFAULT_TAR) || distribution().equals(Distribution.DEFAULT_ZIP)) {
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
        } else if (distribution().equals(Distribution.OSS_TAR) || distribution().equals(Distribution.OSS_ZIP)) {
            assertFalse(Files.exists(installation.lib.resolve("tools").resolve("security-cli")));
        }
    }

    public void test91ElasticsearchShardCliPackaging() throws Exception {
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
