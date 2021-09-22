/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.test;

import org.apache.http.client.fluent.Request;
import org.elasticsearch.core.internal.io.IOUtils;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static org.elasticsearch.packaging.util.Archives.ARCHIVE_OWNER;
import static org.elasticsearch.packaging.util.Archives.installArchive;
import static org.elasticsearch.packaging.util.Archives.verifyArchiveInstallation;
import static org.elasticsearch.packaging.util.Archives.verifySecurityAutoConfigured;
import static org.elasticsearch.packaging.util.Archives.verifySecurityNotAutoConfigured;
import static org.elasticsearch.packaging.util.FileExistenceMatchers.fileExists;
import static org.elasticsearch.packaging.util.FileUtils.append;
import static org.elasticsearch.packaging.util.FileUtils.mv;
import static org.elasticsearch.packaging.util.FileUtils.rm;
import static org.elasticsearch.packaging.util.ServerUtils.makeRequest;
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

    private static String superuser = "test_superuser";
    private static String superuserPassword = "test_superuser";

    public void test10Install() throws Exception {
        installation = installArchive(sh, distribution());
        verifyArchiveInstallation(installation, distribution());
        Result result = sh.run(
            installation.executables().usersTool + " useradd " + superuser + " -p " + superuserPassword + " -r " + "superuser"
        );
        assumeTrue(result.isSuccess());
    }

    public void test20PluginsListWithNoPlugins() throws Exception {
        final Installation.Executables bin = installation.executables();
        final Result r = bin.pluginTool.run("list");

        assertThat(r.stdout, emptyString());
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
            assertThat(runResult.exitCode, is(1));
            assertThat(runResult.stderr, containsString("could not find java in bundled JDK"));
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
        assertThat(runResult.exitCode, is(1));
        assertThat(runResult.stderr, containsString("could not find java in ES_JAVA_HOME"));
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
            assertThat(runResult.stdout, startsWith("Version: "));
        } finally {
            mv(relocatedJdk, installation.bundledJdk);
        }
    }

    public void test40AutoconfigurationNotTriggered() throws Exception {
        FileUtils.assertPathsDoNotExist(installation.data);
        ServerUtils.addSettingToExistingConfiguration(installation, "discovery.seed_hosts", "[\"127.0.0.1:9300\"]");
        startElasticsearch();
        verifySecurityNotAutoConfigured(installation);
        stopElasticsearch();
        ServerUtils.removeSettingFromExistingConfiguration(installation, "discovery.seed_hosts");
        FileUtils.rm(installation.data);

        ServerUtils.addSettingToExistingConfiguration(installation, "node.roles", "[\"voting_only\", \"master\"]");
        startElasticsearch();
        verifySecurityNotAutoConfigured(installation);
        stopElasticsearch();
        ServerUtils.removeSettingFromExistingConfiguration(installation, "node.roles");
        FileUtils.rm(installation.data);

        ServerUtils.addSettingToExistingConfiguration(installation, "node.roles", "[\"ingest\"]");
        startElasticsearch();
        verifySecurityNotAutoConfigured(installation);
        stopElasticsearch();
        ServerUtils.removeSettingFromExistingConfiguration(installation, "node.roles");
        FileUtils.rm(installation.data);

        ServerUtils.addSettingToExistingConfiguration(installation, "xpack.security.http.ssl.enabled", "false");
        startElasticsearch();
        verifySecurityNotAutoConfigured(installation);
        stopElasticsearch();
        ServerUtils.removeSettingFromExistingConfiguration(installation, "xpack.security.http.ssl.enabled");
        FileUtils.rm(installation.data);
    }

    public void test41AutoConfigurationNotTriggeredOnNotWriteableConfDir() throws Exception {
        Path tempDir = createTempDir("custom-config");
        Path tempConf = tempDir.resolve("elasticsearch");
        FileUtils.copyDirectory(installation.config, tempConf);
        Platforms.onLinux(() -> sh.run("chown -R elasticsearch:elasticsearch " + tempDir));
        Platforms.onLinux(() -> sh.run("chmod -w " + tempDir));
        sh.getEnv().put("ES_PATH_CONF", tempConf.toString());
        startElasticsearch();
        verifySecurityNotAutoConfigured(installation);
        stopElasticsearch();
        sh.getEnv().remove("ES_PATH_CONF");
        IOUtils.rm(tempDir);
        FileUtils.rm(installation.data);
    }

    public void test50AutoConfigurationFailsWhenCertificatesNotGenerated() throws Exception {
        FileUtils.assertPathsDoNotExist(installation.data);
        Path tempDir = createTempDir("bc-backup");
        Files.move(installation.lib.resolve("tools/security-cli/bcprov-jdk15on-1.64.jar"), tempDir.resolve("bcprov-jdk15on-1.64.jar"));
        Shell.Result result = runElasticsearchStartCommand(null, false, false);
        assertElasticsearchFailure(result, "java.lang.NoClassDefFoundError: org/bouncycastle/asn1/x509/GeneralName", null);
        Files.move(tempDir.resolve("bcprov-jdk15on-1.64.jar"), installation.lib.resolve("tools/security-cli/bcprov-jdk15on-1.64.jar"));
        IOUtils.rm(tempDir);
    }

    public void test51AutoConfigurationWithPasswordProtectedKeystore() throws Exception {
        FileUtils.assertPathsDoNotExist(installation.data);
        final Installation.Executables bin = installation.executables();
        final String password = "some-keystore-password";
        bin.keystoreTool.run("create");
        Platforms.onLinux(() -> bin.keystoreTool.run("passwd", password + "\n" + password + "\n"));
        Platforms.onWindows(() -> {
            Path keystore = installation.config("elasticsearch.keystore");
            sh.chown(keystore);
            sh.run("Invoke-Command -ScriptBlock {echo '" + password + "'; echo '" + password + "'} | " + bin.keystoreTool + " passwd");
        });
        Shell.Result result = runElasticsearchStartCommand("", false, false);
        assertElasticsearchFailure(result, "Provided keystore password was incorrect", null);
        verifySecurityNotAutoConfigured(installation);

        awaitElasticsearchStartup(runElasticsearchStartCommand(password, true, true));
        verifySecurityAutoConfigured(installation);

        stopElasticsearch();

        // Revert to an empty password for the rest of the tests
        Platforms.onLinux(() -> bin.keystoreTool.run("passwd", password + "\n" + "" + "\n"));
        Platforms.onWindows(
            () -> sh.run("Invoke-Command -ScriptBlock {echo '" + password + "'; echo '" + "" + "'} | " + bin.keystoreTool + " passwd")
        );
    }

    public void test60StartAndStop() throws Exception {

        startElasticsearch();

        assertThat(installation.logs.resolve("gc.log"), fileExists());
        ServerUtils.runElasticsearchTests(superuser, superuserPassword, ServerUtils.getCaCert(installation));

        stopElasticsearch();
    }

    @AwaitsFix(bugUrl = "Change host OS JDK version to 17")
    public void test61EsJavaHomeOverride() throws Exception {
        Platforms.onLinux(() -> {
            String systemJavaHome1 = sh.run("echo $SYSTEM_JAVA_HOME").stdout.trim();
            sh.getEnv().put("ES_JAVA_HOME", systemJavaHome1);
        });
        Platforms.onWindows(() -> {
            final String systemJavaHome1 = sh.run("$Env:SYSTEM_JAVA_HOME").stdout.trim();
            sh.getEnv().put("ES_JAVA_HOME", systemJavaHome1);
        });

        startElasticsearch();
        ServerUtils.runElasticsearchTests(superuser, superuserPassword, ServerUtils.getCaCert(installation));
        stopElasticsearch();

        String systemJavaHome1 = sh.getEnv().get("ES_JAVA_HOME");
        assertThat(FileUtils.slurpAllLogs(installation.logs, "elasticsearch.log", "*.log.gz"), containsString(systemJavaHome1));
    }

    @AwaitsFix(bugUrl = "Change host OS JDK version to 17")
    public void test62JavaHomeIgnored() throws Exception {
        assumeTrue(distribution().hasJdk);
        Platforms.onLinux(() -> {
            String systemJavaHome1 = sh.run("echo $SYSTEM_JAVA_HOME").stdout.trim();
            sh.getEnv().put("JAVA_HOME", systemJavaHome1);
            // ensure that ES_JAVA_HOME is not set for the test
            sh.getEnv().remove("ES_JAVA_HOME");
        });
        Platforms.onWindows(() -> {
            final String systemJavaHome1 = sh.run("$Env:SYSTEM_JAVA_HOME").stdout.trim();
            sh.getEnv().put("JAVA_HOME", systemJavaHome1);
            // ensure that ES_JAVA_HOME is not set for the test
            sh.getEnv().remove("ES_JAVA_HOME");
        });

        final Installation.Executables bin = installation.executables();
        final Result runResult = sh.run(bin.elasticsearch.toString() + " -V");
        assertThat(runResult.stderr, containsString("warning: ignoring JAVA_HOME=" + systemJavaHome + "; using bundled JDK"));

        startElasticsearch();
        ServerUtils.runElasticsearchTests(superuser, superuserPassword, ServerUtils.getCaCert(installation));
        stopElasticsearch();

        // if the JDK started with the bundled JDK then we know that JAVA_HOME was ignored
        String bundledJdk = installation.bundledJdk.toString();
        assertThat(FileUtils.slurpAllLogs(installation.logs, "elasticsearch.log", "*.log.gz"), containsString(bundledJdk));
    }

    @AwaitsFix(bugUrl = "Change host OS JDK version to 17")
    public void test63BundledJdkRemoved() throws Exception {
        assumeThat(distribution().hasJdk, is(true));

        Path relocatedJdk = installation.bundledJdk.getParent().resolve("jdk.relocated");
        try {
            mv(installation.bundledJdk, relocatedJdk);
            Platforms.onLinux(() -> {
                String systemJavaHome1 = sh.run("echo $SYSTEM_JAVA_HOME").stdout.trim();
                sh.getEnv().put("ES_JAVA_HOME", systemJavaHome1);
            });
            Platforms.onWindows(() -> {
                final String systemJavaHome1 = sh.run("$Env:SYSTEM_JAVA_HOME").stdout.trim();
                sh.getEnv().put("ES_JAVA_HOME", systemJavaHome1);
            });

            startElasticsearch();
            ServerUtils.runElasticsearchTests(superuser, superuserPassword, ServerUtils.getCaCert(installation));
            stopElasticsearch();

            String systemJavaHome1 = sh.getEnv().get("ES_JAVA_HOME");
            assertThat(FileUtils.slurpAllLogs(installation.logs, "elasticsearch.log", "*.log.gz"), containsString(systemJavaHome1));
        } finally {
            mv(relocatedJdk, installation.bundledJdk);
        }
    }

    @AwaitsFix(bugUrl = "Change host OS JDK version to 17")
    public void test64JavaHomeWithSpecialCharacters() throws Exception {
        Platforms.onWindows(() -> {
            String javaPath = "C:\\Program Files (x86)\\java";
            try {
                // once windows 2012 is no longer supported and powershell 5.0 is always available we can change this command
                sh.run("cmd /c mklink /D '" + javaPath + "' $Env:SYSTEM_JAVA_HOME");

                sh.getEnv().put("ES_JAVA_HOME", "C:\\Program Files (x86)\\java");

                // verify ES can start, stop and run plugin list
                startElasticsearch();
                ServerUtils.runElasticsearchTests(superuser, superuserPassword, ServerUtils.getCaCert(installation));
                stopElasticsearch();

                String pluginListCommand = installation.bin + "/elasticsearch-plugin list";
                Result result = sh.run(pluginListCommand);
                assertThat(result.exitCode, equalTo(0));

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
                final String systemJavaHome = sh.run("echo $SYSTEM_JAVA_HOME").stdout.trim();
                sh.run("ln -s \"" + systemJavaHome + "\" \"" + testJavaHome + "\"");
                sh.getEnv().put("ES_JAVA_HOME", testJavaHome);

                // verify ES can start, stop and run plugin list
                startElasticsearch();
                ServerUtils.runElasticsearchTests(superuser, superuserPassword, ServerUtils.getCaCert(installation));
                stopElasticsearch();

                String pluginListCommand = installation.bin + "/elasticsearch-plugin list";
                Result result = sh.run(pluginListCommand);
                assertThat(result.exitCode, equalTo(0));
            } finally {
                FileUtils.rm(Paths.get(testJavaHome));
            }
        });
    }

    @AwaitsFix(bugUrl = "Change host OS JDK version to 17")
    public void test65ForceBundledJdkEmptyJavaHome() throws Exception {
        assumeThat(distribution().hasJdk, is(true));

        sh.getEnv().put("ES_JAVA_HOME", "");

        startElasticsearch();
        ServerUtils.runElasticsearchTests(superuser, superuserPassword, ServerUtils.getCaCert(installation));
        stopElasticsearch();
    }

    /**
     * Checks that an installation succeeds when <code>POSIXLY_CORRECT</code> is set in the environment.
     * <p>
     * This test purposefully ignores the existence of the Windows POSIX sub-system.
     */
    public void test66InstallUnderPosix() throws Exception {
        assumeTrue("Only run this test on Unix-like systems", Platforms.WINDOWS == false);
        sh.getEnv().put("POSIXLY_CORRECT", "1");
        startElasticsearch();
        ServerUtils.runElasticsearchTests(superuser, superuserPassword, ServerUtils.getCaCert(installation));
        stopElasticsearch();
    }

    public void test70CustomPathConfAndJvmOptions() throws Exception {

        withCustomConfig(tempConf -> {
            setHeap("512m", tempConf);
            final List<String> jvmOptions = List.of("-Dlog4j2.disable.jmx=true");
            Files.write(tempConf.resolve("jvm.options"), jvmOptions, CREATE, APPEND);

            sh.getEnv().put("ES_JAVA_OPTS", "-XX:-UseCompressedOops");
            // Auto-configuration file paths are absolute so we need to replace them in the config now that we copied them to tempConf
            Path yml = tempConf.resolve("elasticsearch.yml");
            List<String> lines;
            try (Stream<String> allLines = Files.readAllLines(yml).stream()) {
                lines = allLines.map(l -> {
                    if (l.contains(installation.config.toString())) {
                        return l.replace(installation.config.toString(), tempConf.toString());
                    }
                    return l;
                }).collect(Collectors.toList());
            }
            Files.write(yml, lines, TRUNCATE_EXISTING);
            startElasticsearch();

            final String nodesResponse = makeRequest(
                Request.Get("https://localhost:9200/_nodes"),
                superuser,
                superuserPassword,
                ServerUtils.getCaCert(installation)
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

            final String nodesResponse = makeRequest(
                Request.Get("https://localhost:9200/_nodes"),
                superuser,
                superuserPassword,
                ServerUtils.getCaCert(installation)
            );
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

            final String nodesResponse = makeRequest(
                Request.Get("https://localhost:9200/_nodes"),
                superuser,
                superuserPassword,
                ServerUtils.getCaCert(installation)
            );
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
            ServerUtils.runElasticsearchTests(superuser, superuserPassword, ServerUtils.getCaCert(installation));
            stopElasticsearch();
        } finally {
            rm(jvmOptionsIgnored);
        }
    }

    public void test80RelativePathConf() throws Exception {

        withCustomConfig(tempConf -> {
            // Auto-configuration file paths are absolute so we need to replace them in the config now that we copied them to tempConf
            Path yml = tempConf.resolve("elasticsearch.yml");
            List<String> lines;
            try (Stream<String> allLines = Files.readAllLines(yml).stream()) {
                lines = allLines.map(l -> {
                    if (l.contains(installation.config.toString())) {
                        return l.replace(installation.config.toString(), tempConf.toString());
                    }
                    return l;
                }).collect(Collectors.toList());
            }
            lines.add("node.name: relative");
            Files.write(yml, lines, TRUNCATE_EXISTING);
            startElasticsearch();

            final String nodesResponse = makeRequest(
                Request.Get("https://localhost:9200/_nodes"),
                superuser,
                superuserPassword,
                ServerUtils.getCaCert(installation)
            );
            assertThat(nodesResponse, containsString("\"name\":\"relative\""));

            stopElasticsearch();
        });
    }

    public void test90SecurityCliPackaging() throws Exception {
        final Installation.Executables bin = installation.executables();

        assertThat(installation.lib.resolve("tools").resolve("security-cli"), fileExists());
        final Platforms.PlatformAction action = () -> {
            Result result = sh.run(bin.certutilTool + " --help");
            assertThat(result.stdout, containsString("Simplifies certificate creation for use with the Elastic Stack"));

            // Ensure that the exit code from the java command is passed back up through the shell script
            result = sh.runIgnoreExitCode(bin.certutilTool + " invalid-command");
            assertThat(result.exitCode, is(not(0)));
            assertThat(result.stderr, containsString("Unknown command [invalid-command]"));
        };
        Platforms.onLinux(action);
        Platforms.onWindows(action);
    }

    public void test91ElasticsearchShardCliPackaging() throws Exception {
        final Installation.Executables bin = installation.executables();

        Platforms.PlatformAction action = () -> {
            final Result result = sh.run(bin.shardTool + " -h");
            assertThat(result.stdout, containsString("A CLI tool to remove corrupted parts of unrecoverable shards"));
        };

        Platforms.onLinux(action);
        Platforms.onWindows(action);
    }

    public void test92ElasticsearchNodeCliPackaging() throws Exception {
        final Installation.Executables bin = installation.executables();

        Platforms.PlatformAction action = () -> {
            final Result result = sh.run(bin.nodeTool + " -h");
            assertThat(result.stdout, containsString("A CLI tool to do unsafe cluster and index manipulations on current node"));
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
        assertThat(result.stdout, containsString("Master node was successfully bootstrapped"));
    }

    public void test94ElasticsearchNodeExecuteCliNotEsHomeWorkDir() throws Exception {
        final Installation.Executables bin = installation.executables();
        // Run the cli tools from the tmp dir
        sh.setWorkingDirectory(getRootTempDir());

        Platforms.PlatformAction action = () -> {
            Result result = sh.run(bin.certutilTool + " -h");
            assertThat(result.stdout, containsString("Simplifies certificate creation for use with the Elastic Stack"));
            result = sh.run(bin.syskeygenTool + " -h");
            assertThat(result.stdout, containsString("system key tool"));
            result = sh.run(bin.setupPasswordsTool + " -h");
            assertThat(result.stdout, containsString("Sets the passwords for reserved users"));
            result = sh.run(bin.usersTool + " -h");
            assertThat(result.stdout, containsString("Manages elasticsearch file users"));
            result = sh.run(bin.serviceTokensTool + " -h");
            assertThat(result.stdout, containsString("Manages elasticsearch service account file-tokens"));
        };

        Platforms.onLinux(action);
        Platforms.onWindows(action);
    }
}
