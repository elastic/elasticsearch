/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.test;

import com.carrotsearch.randomizedtesting.RandomizedTest;

import org.apache.http.client.fluent.Request;
import org.elasticsearch.packaging.util.Distribution;
import org.elasticsearch.packaging.util.FileUtils;
import org.elasticsearch.packaging.util.Installation;
import org.elasticsearch.packaging.util.Platforms;
import org.elasticsearch.packaging.util.ServerUtils;
import org.elasticsearch.packaging.util.Shell;
import org.elasticsearch.packaging.util.Shell.Result;
import org.junit.BeforeClass;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static org.elasticsearch.packaging.util.Archives.ARCHIVE_OWNER;
import static org.elasticsearch.packaging.util.Archives.installArchive;
import static org.elasticsearch.packaging.util.Archives.verifyArchiveInstallation;
import static org.elasticsearch.packaging.util.FileExistenceMatchers.fileExists;
import static org.elasticsearch.packaging.util.FileUtils.append;
import static org.elasticsearch.packaging.util.FileUtils.mv;
import static org.elasticsearch.packaging.util.FileUtils.rm;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assume.assumeThat;
import static org.junit.Assume.assumeTrue;

public class ArchiveTests extends PackagingTestCase {

    @BeforeClass
    public static void filterDistros() {
        assumeTrue("only archives", distribution.isArchive());
    }

    public void test10Install() throws Exception {
        installation = installArchive(sh, distribution());
        verifyArchiveInstallation(installation, distribution());
        setFileSuperuser("test_superuser", "test_superuser_password");
        // See https://bugs.openjdk.java.net/browse/JDK-8267701. In short, when generating PKCS#12 keystores in JDK 12 and later
        // the MAC algorithm used for integrity protection is incompatible with any previous JDK version. This affects us as we generate
        // PKCS12 keystores on startup ( with the bundled JDK ) but we also need to run certain tests with a JDK other than the bundled
        // one, and we still use JDK11 for that.
        // We're manually setting the HMAC algorithm to something that is compatible with previous versions here. Moving forward, when
        // min compat JDK is JDK17, we can remove this hack and use the standard security properties file.
        final Path jdkSecurityProperties = installation.bundledJdk.resolve("conf").resolve("security").resolve("java.security");
        List<String> lines;
        try (Stream<String> allLines = Files.readAllLines(jdkSecurityProperties).stream()) {
            lines = allLines.filter(s -> s.startsWith("#keystore.pkcs12.macAlgorithm") == false)
                .filter(s -> s.startsWith("#keystore.pkcs12.macIterationCount") == false)
                .collect(Collectors.toList());
        }
        lines.add("keystore.pkcs12.macAlgorithm = HmacPBESHA1");
        lines.add("keystore.pkcs12.macIterationCount = 100000");
        Files.write(jdkSecurityProperties, lines, TRUNCATE_EXISTING);
    }

    public void test20PluginsListWithNoPlugins() throws Exception {
        final Installation.Executables bin = installation.executables();
        final Result r = bin.pluginTool.run("list");

        assertThat(r.stdout(), emptyString());
    }

    public void test30MissingBundledJdk() throws Exception {
        final Installation.Executables bin = installation.executables();
        sh.getEnv().remove("ES_JAVA_HOME");

        final Path relocatedJdk = installation.bundledJdk.getParent().resolve("jdk.relocated");

        try {
            if (distribution().hasJdk) {
                mv(installation.bundledJdk, relocatedJdk);
            }
            // ask for elasticsearch version to quickly exit if java is actually found (ie test failure)
            final Result runResult = sh.runIgnoreExitCode(bin.elasticsearch.toString() + " -V");
            assertThat(runResult.exitCode(), is(1));
            assertThat(runResult.stderr(), containsString("could not find java in bundled JDK"));
        } finally {
            if (distribution().hasJdk) {
                mv(relocatedJdk, installation.bundledJdk);
            }
        }
    }

    public void test31BadJavaHome() throws Exception {
        final Installation.Executables bin = installation.executables();
        sh.getEnv().put("ES_JAVA_HOME", "doesnotexist");

        // ask for elasticsearch version to quickly exit if java is actually found (ie test failure)
        final Result runResult = sh.runIgnoreExitCode(bin.elasticsearch.toString() + " -V");
        assertThat(runResult.exitCode(), is(1));
        assertThat(runResult.stderr(), containsString("could not find java in ES_JAVA_HOME"));
    }

    public void test32SpecialCharactersInJdkPath() throws Exception {
        final Installation.Executables bin = installation.executables();
        assumeTrue("Only run this test when we know where the JDK is.", distribution().hasJdk);

        final Path relocatedJdk = installation.bundledJdk.getParent().resolve("a (special) path");
        sh.getEnv().put("ES_JAVA_HOME", relocatedJdk.toString());

        try {
            mv(installation.bundledJdk, relocatedJdk);
            // ask for elasticsearch version to avoid starting the app
            final Result runResult = sh.run(bin.elasticsearch.toString() + " -V");
            assertThat(runResult.stdout(), startsWith("Version: "));
        } finally {
            mv(relocatedJdk, installation.bundledJdk);
        }
    }

    public void test40AutoconfigurationNotTriggeredWhenNodeIsMeantToJoinExistingCluster() throws Exception {
        // auto-config requires that the archive owner and the process user be the same,
        Platforms.onWindows(() -> sh.chown(installation.config, installation.getOwner()));
        FileUtils.assertPathsDoNotExist(installation.data);
        ServerUtils.addSettingToExistingConfiguration(installation, "discovery.seed_hosts", "[\"127.0.0.1:9300\"]");
        startElasticsearch();
        verifySecurityNotAutoConfigured(installation);
        stopElasticsearch();
        ServerUtils.removeSettingFromExistingConfiguration(installation, "discovery.seed_hosts");
        Platforms.onWindows(() -> sh.chown(installation.config));
        FileUtils.rm(installation.data);
    }

    public void test41AutoconfigurationNotTriggeredWhenNodeCannotContainData() throws Exception {
        // auto-config requires that the archive owner and the process user be the same
        Platforms.onWindows(() -> sh.chown(installation.config, installation.getOwner()));
        ServerUtils.addSettingToExistingConfiguration(installation, "node.roles", "[\"voting_only\", \"master\"]");
        startElasticsearch();
        verifySecurityNotAutoConfigured(installation);
        stopElasticsearch();
        ServerUtils.removeSettingFromExistingConfiguration(installation, "node.roles");
        Platforms.onWindows(() -> sh.chown(installation.config));
        FileUtils.rm(installation.data);
    }

    public void test42AutoconfigurationNotTriggeredWhenNodeCannotBecomeMaster() throws Exception {
        // auto-config requires that the archive owner and the process user be the same
        Platforms.onWindows(() -> sh.chown(installation.config, installation.getOwner()));
        ServerUtils.addSettingToExistingConfiguration(installation, "node.roles", "[\"ingest\"]");
        startElasticsearch();
        verifySecurityNotAutoConfigured(installation);
        stopElasticsearch();
        ServerUtils.removeSettingFromExistingConfiguration(installation, "node.roles");
        Platforms.onWindows(() -> sh.chown(installation.config));
        FileUtils.rm(installation.data);
    }

    public void test43AutoconfigurationNotTriggeredWhenTlsAlreadyConfigured() throws Exception {
        // auto-config requires that the archive owner and the process user be the same
        Platforms.onWindows(() -> sh.chown(installation.config, installation.getOwner()));
        ServerUtils.addSettingToExistingConfiguration(installation, "xpack.security.http.ssl.enabled", "false");
        startElasticsearch();
        verifySecurityNotAutoConfigured(installation);
        stopElasticsearch();
        ServerUtils.removeSettingFromExistingConfiguration(installation, "xpack.security.http.ssl.enabled");
        Platforms.onWindows(() -> sh.chown(installation.config));
        FileUtils.rm(installation.data);
    }

    public void test44AutoConfigurationNotTriggeredOnNotWriteableConfDir() throws Exception {
        Platforms.onWindows(() -> {
            // auto-config requires that the archive owner and the process user be the same
            sh.chown(installation.config, installation.getOwner());
            // prevent modifications to the config directory
            sh.run(
                String.format(
                    Locale.ROOT,
                    "$ACL = Get-ACL -Path '%s'; "
                        + "$AccessRule = New-Object System.Security.AccessControl.FileSystemAccessRule('%s','Write','Deny'); "
                        + "$ACL.SetAccessRule($AccessRule); "
                        + "$ACL | Set-Acl -Path '%s';",
                    installation.config,
                    installation.getOwner(),
                    installation.config
                )
            );
        });
        Platforms.onLinux(() -> { sh.run("chmod u-w " + installation.config); });
        try {
            startElasticsearch();
            verifySecurityNotAutoConfigured(installation);
            // the node still starts, with Security enabled, but without TLS auto-configured (so only authentication)
            runElasticsearchTests();
            stopElasticsearch();
        } finally {
            Platforms.onWindows(() -> {
                sh.run(
                    String.format(
                        Locale.ROOT,
                        "$ACL = Get-ACL -Path '%s'; "
                            + "$AccessRule = New-Object System.Security.AccessControl.FileSystemAccessRule('%s','Write','Deny'); "
                            + "$ACL.RemoveAccessRule($AccessRule); "
                            + "$ACL | Set-Acl -Path '%s';",
                        installation.config,
                        installation.getOwner(),
                        installation.config
                    )
                );
                sh.chown(installation.config);
            });
            Platforms.onLinux(() -> { sh.run("chmod u+w " + installation.config); });
            FileUtils.rm(installation.data);
        }
    }

    public void test50AutoConfigurationFailsWhenCertificatesNotGenerated() throws Exception {
        // auto-config requires that the archive owner and the process user be the same
        Platforms.onWindows(() -> sh.chown(installation.config, installation.getOwner()));
        FileUtils.assertPathsDoNotExist(installation.data);
        Path tempDir = createTempDir("bc-backup");
        Files.move(
            installation.lib.resolve("tools").resolve("security-cli").resolve("bcprov-jdk15on-1.64.jar"),
            tempDir.resolve("bcprov-jdk15on-1.64.jar")
        );
        Shell.Result result = runElasticsearchStartCommand(null, false, false);
        assertElasticsearchFailure(result, "java.lang.NoClassDefFoundError: org/bouncycastle/", null);
        Files.move(
            tempDir.resolve("bcprov-jdk15on-1.64.jar"),
            installation.lib.resolve("tools").resolve("security-cli").resolve("bcprov-jdk15on-1.64.jar")
        );
        Platforms.onWindows(() -> sh.chown(installation.config));
        FileUtils.rm(tempDir);
    }

    public void test51AutoConfigurationWithPasswordProtectedKeystore() throws Exception {
        /* Windows issue awaits fix: https://github.com/elastic/elasticsearch/issues/49340 */
        assumeTrue("expect command isn't on Windows", distribution.platform != Distribution.Platform.WINDOWS);
        FileUtils.assertPathsDoNotExist(installation.data);
        final Installation.Executables bin = installation.executables();
        final String password = "some-keystore-password";
        Platforms.onLinux(() -> bin.keystoreTool.run("passwd", password + "\n" + password + "\n"));
        Platforms.onWindows(
            () -> {
                sh.run("Invoke-Command -ScriptBlock {echo '" + password + "'; echo '" + password + "'} | " + bin.keystoreTool + " passwd");
            }
        );
        Shell.Result result = runElasticsearchStartCommand("some-wrong-password-here", false, false);
        assertElasticsearchFailure(result, "Provided keystore password was incorrect", null);
        verifySecurityNotAutoConfigured(installation);
        if (RandomizedTest.randomBoolean()) {
            ServerUtils.addSettingToExistingConfiguration(installation, "node.name", "my-custom-random-node-name-here");
        }
        awaitElasticsearchStartup(runElasticsearchStartCommand(password, true, true));
        verifySecurityAutoConfigured(installation);

        stopElasticsearch();

        // Revert to an empty password for the rest of the tests
        Platforms.onLinux(() -> bin.keystoreTool.run("passwd", password + "\n" + "" + "\n"));
        Platforms.onWindows(
            () -> sh.run("Invoke-Command -ScriptBlock {echo '" + password + "'; echo '" + "" + "'} | " + bin.keystoreTool + " passwd")
        );
    }

    public void test52AutoConfigurationOnWindows() throws Exception {
        assumeTrue(
            "run this in place of test51AutoConfigurationWithPasswordProtectedKeystore on windows",
            distribution.platform == Distribution.Platform.WINDOWS
        );
        sh.chown(installation.config, installation.getOwner());
        FileUtils.assertPathsDoNotExist(installation.data);
        if (RandomizedTest.randomBoolean()) {
            ServerUtils.addSettingToExistingConfiguration(installation, "node.name", "my-custom-random-node-name-here");
        }
        startElasticsearch();
        verifySecurityAutoConfigured(installation);
        stopElasticsearch();
        sh.chown(installation.config);
    }

    public void test60StartAndStop() throws Exception {
        startElasticsearch();
        assertThat(installation.logs.resolve("gc.log"), fileExists());
        runElasticsearchTests();
        stopElasticsearch();
    }

    public void test61EsJavaHomeOverride() throws Exception {
        Platforms.onLinux(() -> {
            String systemJavaHome1 = sh.run("echo $SYSTEM_JAVA_HOME").stdout().trim();
            sh.getEnv().put("ES_JAVA_HOME", systemJavaHome1);
        });
        Platforms.onWindows(() -> {
            final String systemJavaHome1 = sh.run("$Env:SYSTEM_JAVA_HOME").stdout().trim();
            sh.getEnv().put("ES_JAVA_HOME", systemJavaHome1);
        });

        startElasticsearch();
        runElasticsearchTests();
        stopElasticsearch();

        String systemJavaHome1 = sh.getEnv().get("ES_JAVA_HOME");
        assertThat(FileUtils.slurpAllLogs(installation.logs, "elasticsearch.log", "*.log.gz"), containsString(systemJavaHome1));
    }

    public void test62JavaHomeIgnored() throws Exception {
        assumeTrue(distribution().hasJdk);
        Platforms.onLinux(() -> {
            String systemJavaHome1 = sh.run("echo $SYSTEM_JAVA_HOME").stdout().trim();
            sh.getEnv().put("JAVA_HOME", systemJavaHome1);
            // ensure that ES_JAVA_HOME is not set for the test
            sh.getEnv().remove("ES_JAVA_HOME");
        });
        Platforms.onWindows(() -> {
            final String systemJavaHome1 = sh.run("$Env:SYSTEM_JAVA_HOME").stdout().trim();
            sh.getEnv().put("JAVA_HOME", systemJavaHome1);
            // ensure that ES_JAVA_HOME is not set for the test
            sh.getEnv().remove("ES_JAVA_HOME");
        });

        final Installation.Executables bin = installation.executables();
        final Result runResult = sh.run(bin.elasticsearch.toString() + " -V");
        assertThat(runResult.stderr(), containsString("warning: ignoring JAVA_HOME=" + systemJavaHome + "; using bundled JDK"));

        startElasticsearch();
        runElasticsearchTests();
        stopElasticsearch();

        // if the JDK started with the bundled JDK then we know that JAVA_HOME was ignored
        String bundledJdk = installation.bundledJdk.toString();
        assertThat(FileUtils.slurpAllLogs(installation.logs, "elasticsearch.log", "*.log.gz"), containsString(bundledJdk));
    }

    public void test63BundledJdkRemoved() throws Exception {
        assumeThat(distribution().hasJdk, is(true));

        Path relocatedJdk = installation.bundledJdk.getParent().resolve("jdk.relocated");
        try {
            mv(installation.bundledJdk, relocatedJdk);
            Platforms.onLinux(() -> {
                String systemJavaHome1 = sh.run("echo $SYSTEM_JAVA_HOME").stdout().trim();
                sh.getEnv().put("ES_JAVA_HOME", systemJavaHome1);
            });
            Platforms.onWindows(() -> {
                final String systemJavaHome1 = sh.run("$Env:SYSTEM_JAVA_HOME").stdout().trim();
                sh.getEnv().put("ES_JAVA_HOME", systemJavaHome1);
            });

            startElasticsearch();
            runElasticsearchTests();
            stopElasticsearch();

            String systemJavaHome1 = sh.getEnv().get("ES_JAVA_HOME");
            assertThat(FileUtils.slurpAllLogs(installation.logs, "elasticsearch.log", "*.log.gz"), containsString(systemJavaHome1));
        } finally {
            mv(relocatedJdk, installation.bundledJdk);
        }
    }

    public void test64JavaHomeWithSpecialCharacters() throws Exception {
        Platforms.onWindows(() -> {
            String javaPath = "C:\\Program Files (x86)\\java";
            try {
                // once windows 2012 is no longer supported and powershell 5.0 is always available we can change this command
                sh.run("cmd /c mklink /D '" + javaPath + "' $Env:SYSTEM_JAVA_HOME");

                sh.getEnv().put("ES_JAVA_HOME", "C:\\Program Files (x86)\\java");

                // verify ES can start, stop and run plugin list
                startElasticsearch();
                runElasticsearchTests();
                stopElasticsearch();

                String pluginListCommand = installation.bin + "/elasticsearch-plugin list";
                Result result = sh.run(pluginListCommand);
                assertThat(result.exitCode(), equalTo(0));

            } finally {
                // clean up sym link
                if (Files.exists(Paths.get(javaPath))) {
                    sh.run("cmd /c rmdir '" + javaPath + "' ");
                }
            }
        });

        Platforms.onLinux(() -> {
            // Create temporary directory with a space and link to real java home
            String testJavaHome = Paths.get("/tmp", "java home").toString();
            try {
                final String systemJavaHome = sh.run("echo $SYSTEM_JAVA_HOME").stdout().trim();
                sh.run("ln -s \"" + systemJavaHome + "\" \"" + testJavaHome + "\"");
                sh.getEnv().put("ES_JAVA_HOME", testJavaHome);

                // verify ES can start, stop and run plugin list
                startElasticsearch();
                runElasticsearchTests();
                stopElasticsearch();

                String pluginListCommand = installation.bin + "/elasticsearch-plugin list";
                Result result = sh.run(pluginListCommand);
                assertThat(result.exitCode(), equalTo(0));
            } finally {
                FileUtils.rm(Paths.get(testJavaHome));
            }
        });
    }

    public void test65ForceBundledJdkEmptyJavaHome() throws Exception {
        assumeThat(distribution().hasJdk, is(true));

        sh.getEnv().put("ES_JAVA_HOME", "");

        startElasticsearch();
        runElasticsearchTests();
        stopElasticsearch();
    }

    /**
     * Checks that an installation succeeds when <code>POSIXLY_CORRECT</code> is set in the environment.
     * <p>
     * This test purposefully ignores the existence of the Windows POSIX sub-system.
     */
    public void test66InstallUnderPosix() throws Exception {
        sh.getEnv().put("POSIXLY_CORRECT", "1");
        startElasticsearch();
        runElasticsearchTests();
        stopElasticsearch();
    }

    public void test70CustomPathConfAndJvmOptions() throws Exception {
        withCustomConfig(tempConf -> {
            setHeap("512m", tempConf);
            final List<String> jvmOptions = List.of("-Dlog4j2.disable.jmx=true");
            Files.write(tempConf.resolve("jvm.options"), jvmOptions, CREATE, APPEND);

            sh.getEnv().put("ES_JAVA_OPTS", "-XX:-UseCompressedOops");
            startElasticsearch();

            final String nodesResponse = ServerUtils.makeRequest(
                Request.Get("https://localhost:9200/_nodes"),
                "test_superuser",
                "test_superuser_password",
                ServerUtils.getCaCert(tempConf)
            );
            assertThat(nodesResponse, containsString("\"heap_init_in_bytes\":536870912"));
            assertThat(nodesResponse, containsString("\"using_compressed_ordinary_object_pointers\":\"false\""));

            stopElasticsearch();
        });
    }

    public void test71CustomJvmOptionsDirectoryFile() throws Exception {
        final Path heapOptions = installation.config(Paths.get("jvm.options.d", "heap.options"));
        try {
            setHeap(null); // delete default options
            append(heapOptions, "-Xms512m\n-Xmx512m\n");

            startElasticsearch();

            final String nodesResponse = makeRequest("https://localhost:9200/_nodes");
            assertThat(nodesResponse, containsString("\"heap_init_in_bytes\":536870912"));

            stopElasticsearch();
        } finally {
            rm(heapOptions);
        }
    }

    public void test72CustomJvmOptionsDirectoryFilesAreProcessedInSortedOrder() throws Exception {
        final Path firstOptions = installation.config(Paths.get("jvm.options.d", "first.options"));
        final Path secondOptions = installation.config(Paths.get("jvm.options.d", "second.options"));
        try {
            setHeap(null); // delete default options
            /*
             * We override the heap in the first file, and disable compressed oops, and override the heap in the second file. By doing this,
             * we can test that both files are processed by the JVM options parser, and also that they are processed in lexicographic order.
             */
            append(firstOptions, "-Xms384m\n-Xmx384m\n-XX:-UseCompressedOops\n");
            append(secondOptions, "-Xms512m\n-Xmx512m\n");

            startElasticsearch();

            final String nodesResponse = makeRequest("https://localhost:9200/_nodes");
            assertThat(nodesResponse, containsString("\"heap_init_in_bytes\":536870912"));
            assertThat(nodesResponse, containsString("\"using_compressed_ordinary_object_pointers\":\"false\""));

            stopElasticsearch();
        } finally {
            rm(firstOptions);
            rm(secondOptions);
        }
    }

    public void test73CustomJvmOptionsDirectoryFilesWithoutOptionsExtensionIgnored() throws Exception {
        final Path jvmOptionsIgnored = installation.config(Paths.get("jvm.options.d", "jvm.options.ignored"));
        try {
            append(jvmOptionsIgnored, "-Xthis_is_not_a_valid_option\n");

            startElasticsearch();
            runElasticsearchTests();
            stopElasticsearch();
        } finally {
            rm(jvmOptionsIgnored);
        }
    }

    public void test74CustomJvmOptionsTotalMemoryOverride() throws Exception {
        final Path heapOptions = installation.config(Paths.get("jvm.options.d", "total_memory.options"));
        try {
            setHeap(null); // delete default options
            // Work as though total system memory is 850MB
            append(heapOptions, "-Des.total_memory_bytes=891289600\n");

            startElasticsearch();

            final String nodesStatsResponse = makeRequest("https://localhost:9200/_nodes/stats");
            assertThat(nodesStatsResponse, containsString("\"adjusted_total_in_bytes\":891289600"));
            final String nodesResponse = makeRequest("https://localhost:9200/_nodes");
            // 40% of 850MB
            assertThat(nodesResponse, containsString("\"heap_init_in_bytes\":356515840"));

            stopElasticsearch();
        } finally {
            rm(heapOptions);
        }
    }

    public void test80RelativePathConf() throws Exception {
        withCustomConfig(tempConf -> {
            ServerUtils.removeSettingFromExistingConfiguration(tempConf, "node.name");
            ServerUtils.addSettingToExistingConfiguration(tempConf, "node.name", "relative");
            startElasticsearch();

            final String nodesResponse = makeRequest("https://localhost:9200/_nodes");
            assertThat(nodesResponse, containsString("\"name\":\"relative\""));

            stopElasticsearch();
        });
    }

    public void test90SecurityCliPackaging() throws Exception {
        final Installation.Executables bin = installation.executables();

        assertThat(installation.lib.resolve("tools").resolve("security-cli"), fileExists());
        final Platforms.PlatformAction action = () -> {
            Result result = sh.run(bin.certutilTool + " --help");
            assertThat(result.stdout(), containsString("Simplifies certificate creation for use with the Elastic Stack"));

            // Ensure that the exit code from the java command is passed back up through the shell script
            result = sh.runIgnoreExitCode(bin.certutilTool + " invalid-command");
            assertThat(result.exitCode(), is(not(0)));
            assertThat(result.stderr(), containsString("Unknown command [invalid-command]"));
        };
        Platforms.onLinux(action);
        Platforms.onWindows(action);
    }

    public void test91ElasticsearchShardCliPackaging() throws Exception {
        final Installation.Executables bin = installation.executables();

        Platforms.PlatformAction action = () -> {
            final Result result = sh.run(bin.shardTool + " -h");
            assertThat(result.stdout(), containsString("A CLI tool to remove corrupted parts of unrecoverable shards"));
        };

        Platforms.onLinux(action);
        Platforms.onWindows(action);
    }

    public void test92ElasticsearchNodeCliPackaging() throws Exception {
        final Installation.Executables bin = installation.executables();

        Platforms.PlatformAction action = () -> {
            final Result result = sh.run(bin.nodeTool + " -h");
            assertThat(result.stdout(), containsString("A CLI tool to do unsafe cluster and index manipulations on current node"));
        };

        Platforms.onLinux(action);
        Platforms.onWindows(action);
    }

    public void test93ElasticsearchNodeCustomDataPathAndNotEsHomeWorkDir() throws Exception {
        Path relativeDataPath = installation.data.relativize(installation.home);
        append(installation.config("elasticsearch.yml"), "path.data: " + relativeDataPath);
        sh.setWorkingDirectory(getRootTempDir());

        startElasticsearch();
        stopElasticsearch();

        String nodeTool = installation.executables().nodeTool.toString();
        if (Platforms.WINDOWS == false) {
            nodeTool = "sudo -E -u " + ARCHIVE_OWNER + " " + nodeTool;
        }

        Result result = sh.run("echo y | " + nodeTool + " unsafe-bootstrap");
        assertThat(result.stdout(), containsString("Master node was successfully bootstrapped"));
    }

    public void test94ElasticsearchNodeExecuteCliNotEsHomeWorkDir() throws Exception {
        final Installation.Executables bin = installation.executables();
        // Run the cli tools from the tmp dir
        sh.setWorkingDirectory(getRootTempDir());

        Platforms.PlatformAction action = () -> {
            Result result = sh.run(bin.certutilTool + " -h");
            assertThat(result.stdout(), containsString("Simplifies certificate creation for use with the Elastic Stack"));
            result = sh.run(bin.syskeygenTool + " -h");
            assertThat(result.stdout(), containsString("system key tool"));
            result = sh.run(bin.setupPasswordsTool + " -h");
            assertThat(result.stdout(), containsString("Sets the passwords for reserved users"));
            result = sh.run(bin.usersTool + " -h");
            assertThat(result.stdout(), containsString("Manages elasticsearch file users"));
            result = sh.run(bin.serviceTokensTool + " -h");
            assertThat(result.stdout(), containsString("Manages elasticsearch service account file-tokens"));
        };

        Platforms.onLinux(action);
        Platforms.onWindows(action);
    }
}
