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
import org.elasticsearch.packaging.util.FileUtils;
import org.elasticsearch.packaging.util.Installation;
import org.elasticsearch.packaging.util.Platforms;
import org.elasticsearch.packaging.util.ServerUtils;
import org.elasticsearch.packaging.util.Shell.Result;
import org.junit.BeforeClass;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static org.elasticsearch.packaging.util.Archives.installArchive;
import static org.elasticsearch.packaging.util.Archives.verifyArchiveInstallation;
import static org.elasticsearch.packaging.util.FileExistenceMatchers.fileDoesNotExist;
import static org.elasticsearch.packaging.util.FileExistenceMatchers.fileExists;
import static org.elasticsearch.packaging.util.FileUtils.append;
import static org.elasticsearch.packaging.util.FileUtils.cp;
import static org.elasticsearch.packaging.util.FileUtils.getTempDir;
import static org.elasticsearch.packaging.util.FileUtils.mkdir;
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

    public void test10Install() throws Exception {
        installation = installArchive(sh, distribution());
        verifyArchiveInstallation(installation, distribution());
    }

    public void test20PluginsListWithNoPlugins() throws Exception {
        final Installation.Executables bin = installation.executables();
        final Result r = bin.pluginTool.run("list");

        assertThat(r.stdout, emptyString());
    }

    public void test30MissingBundledJdk() throws Exception {
        final Installation.Executables bin = installation.executables();
        sh.getEnv().remove("JAVA_HOME");

        final Path relocatedJdk = installation.bundledJdk.getParent().resolve("jdk.relocated");

        try {
            if (distribution().hasJdk) {
                mv(installation.bundledJdk, relocatedJdk);
            }
            // ask for elasticsearch version to quickly exit if java is actually found (ie test failure)
            final Result runResult = sh.runIgnoreExitCode(bin.elasticsearch.toString() + " -v");
            assertThat(runResult.exitCode, is(1));
            assertThat(runResult.stderr, containsString("could not find java in bundled jdk"));
        } finally {
            if (distribution().hasJdk) {
                mv(relocatedJdk, installation.bundledJdk);
            }
        }
    }

    public void test31BadJavaHome() throws Exception {
        final Installation.Executables bin = installation.executables();
        sh.getEnv().put("JAVA_HOME", "doesnotexist");

        // ask for elasticsearch version to quickly exit if java is actually found (ie test failure)
        final Result runResult = sh.runIgnoreExitCode(bin.elasticsearch.toString() + " -V");
        assertThat(runResult.exitCode, is(1));
        assertThat(runResult.stderr, containsString("could not find java in JAVA_HOME"));

    }

    public void test32SpecialCharactersInJdkPath() throws Exception {
        final Installation.Executables bin = installation.executables();
        assumeTrue("Only run this test when we know where the JDK is.", distribution().hasJdk);

        final Path relocatedJdk = installation.bundledJdk.getParent().resolve("a (special) path");
        sh.getEnv().put("JAVA_HOME", relocatedJdk.toString());

        try {
            mv(installation.bundledJdk, relocatedJdk);
            // ask for elasticsearch version to avoid starting the app
            final Result runResult = sh.run(bin.elasticsearch.toString() + " -V");
            assertThat(runResult.stdout, startsWith("Version: "));
        } finally {
            mv(relocatedJdk, installation.bundledJdk);
        }
    }

    public void test50StartAndStop() throws Exception {
        // cleanup from previous test
        rm(installation.config("elasticsearch.keystore"));

        try {
            startElasticsearch();
        } catch (Exception e) {
            if (Files.exists(installation.home.resolve("elasticsearch.pid"))) {
                String pid = FileUtils.slurp(installation.home.resolve("elasticsearch.pid")).trim();
                logger.info("Dumping jstack of elasticsearch processb ({}) that failed to start", pid);
                sh.runIgnoreExitCode("jstack " + pid);
            }
            throw e;
        }

        assertThat(installation.logs.resolve("gc.log"), fileExists());
        ServerUtils.runElasticsearchTests();

        stopElasticsearch();
    }

    public void test51JavaHomeOverride() throws Exception {
        Platforms.onLinux(() -> {
            String systemJavaHome1 = sh.run("echo $SYSTEM_JAVA_HOME").stdout.trim();
            sh.getEnv().put("JAVA_HOME", systemJavaHome1);
        });
        Platforms.onWindows(() -> {
            final String systemJavaHome1 = sh.run("$Env:SYSTEM_JAVA_HOME").stdout.trim();
            sh.getEnv().put("JAVA_HOME", systemJavaHome1);
        });

        startElasticsearch();
        ServerUtils.runElasticsearchTests();
        stopElasticsearch();

        String systemJavaHome1 = sh.getEnv().get("JAVA_HOME");
        assertThat(FileUtils.slurpAllLogs(installation.logs, "elasticsearch.log", "*.log.gz"), containsString(systemJavaHome1));
    }

    public void test52BundledJdkRemoved() throws Exception {
        assumeThat(distribution().hasJdk, is(true));

        Path relocatedJdk = installation.bundledJdk.getParent().resolve("jdk.relocated");
        try {
            mv(installation.bundledJdk, relocatedJdk);
            Platforms.onLinux(() -> {
                String systemJavaHome1 = sh.run("echo $SYSTEM_JAVA_HOME").stdout.trim();
                sh.getEnv().put("JAVA_HOME", systemJavaHome1);
            });
            Platforms.onWindows(() -> {
                final String systemJavaHome1 = sh.run("$Env:SYSTEM_JAVA_HOME").stdout.trim();
                sh.getEnv().put("JAVA_HOME", systemJavaHome1);
            });

            startElasticsearch();
            ServerUtils.runElasticsearchTests();
            stopElasticsearch();

            String systemJavaHome1 = sh.getEnv().get("JAVA_HOME");
            assertThat(FileUtils.slurpAllLogs(installation.logs, "elasticsearch.log", "*.log.gz"), containsString(systemJavaHome1));
        } finally {
            mv(relocatedJdk, installation.bundledJdk);
        }
    }

    public void test53JavaHomeWithSpecialCharacters() throws Exception {
        Platforms.onWindows(() -> {
            String javaPath = "C:\\Program Files (x86)\\java";
            try {
                // once windows 2012 is no longer supported and powershell 5.0 is always available we can change this command
                sh.run("cmd /c mklink /D '" + javaPath + "' $Env:SYSTEM_JAVA_HOME");

                sh.getEnv().put("JAVA_HOME", "C:\\Program Files (x86)\\java");

                // verify ES can start, stop and run plugin list
                startElasticsearch();

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
                sh.getEnv().put("JAVA_HOME", testJavaHome);

                // verify ES can start, stop and run plugin list
                startElasticsearch();

                stopElasticsearch();

                String pluginListCommand = installation.bin + "/elasticsearch-plugin list";
                Result result = sh.run(pluginListCommand);
                assertThat(result.exitCode, equalTo(0));
            } finally {
                FileUtils.rm(Paths.get(testJavaHome));
            }
        });
    }

    public void test54ForceBundledJdkEmptyJavaHome() throws Exception {
        assumeThat(distribution().hasJdk, is(true));
        // cleanup from previous test
        rm(installation.config("elasticsearch.keystore"));

        sh.getEnv().put("JAVA_HOME", "");

        startElasticsearch();
        ServerUtils.runElasticsearchTests();
        stopElasticsearch();
    }

    public void test70CustomPathConfAndJvmOptions() throws Exception {

        final Path tempConf = getTempDir().resolve("esconf-alternate");

        try {
            mkdir(tempConf);
            cp(installation.config("elasticsearch.yml"), tempConf.resolve("elasticsearch.yml"));
            cp(installation.config("log4j2.properties"), tempConf.resolve("log4j2.properties"));

            // we have to disable Log4j from using JMX lest it will hit a security
            // manager exception before we have configured logging; this will fail
            // startup since we detect usages of logging before it is configured
            final List<String> jvmOptions = List.of("-Xms512m", "-Xmx512m", "-Dlog4j2.disable.jmx=true");
            Files.write(tempConf.resolve("jvm.options"), jvmOptions, CREATE, APPEND);

            sh.chown(tempConf);

            sh.getEnv().put("ES_PATH_CONF", tempConf.toString());
            sh.getEnv().put("ES_JAVA_OPTS", "-XX:-UseCompressedOops");

            startElasticsearch();

            final String nodesResponse = makeRequest(Request.Get("http://localhost:9200/_nodes"));
            assertThat(nodesResponse, containsString("\"heap_init_in_bytes\":536870912"));
            assertThat(nodesResponse, containsString("\"using_compressed_ordinary_object_pointers\":\"false\""));

            stopElasticsearch();

        } finally {
            rm(tempConf);
        }
    }

    public void test71CustomJvmOptionsDirectoryFile() throws Exception {
        final Path heapOptions = installation.config(Paths.get("jvm.options.d", "heap.options"));
        try {
            append(heapOptions, "-Xms512m\n-Xmx512m\n");

            startElasticsearch();

            final String nodesResponse = makeRequest(Request.Get("http://localhost:9200/_nodes"));
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
            /*
             * We override the heap in the first file, and disable compressed oops, and override the heap in the second file. By doing this,
             * we can test that both files are processed by the JVM options parser, and also that they are processed in lexicographic order.
             */
            append(firstOptions, "-Xms384m\n-Xmx384m\n-XX:-UseCompressedOops\n");
            append(secondOptions, "-Xms512m\n-Xmx512m\n");

            startElasticsearch();

            final String nodesResponse = makeRequest(Request.Get("http://localhost:9200/_nodes"));
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
            append(jvmOptionsIgnored, "-Xms512\n-Xmx512m\n");

            startElasticsearch();

            final String nodesResponse = makeRequest(Request.Get("http://localhost:9200/_nodes"));
            assertThat(nodesResponse, containsString("\"heap_init_in_bytes\":1073741824"));

            stopElasticsearch();
        } finally {
            rm(jvmOptionsIgnored);
        }
    }

    public void test80RelativePathConf() throws Exception {

        final Path temp = getTempDir().resolve("esconf-alternate");
        final Path tempConf = temp.resolve("config");

        try {
            mkdir(tempConf);
            Stream.of("elasticsearch.yml", "log4j2.properties", "jvm.options")
                .forEach(file -> cp(installation.config(file), tempConf.resolve(file)));

            append(tempConf.resolve("elasticsearch.yml"), "node.name: relative");

            sh.chown(temp);

            sh.setWorkingDirectory(temp);
            sh.getEnv().put("ES_PATH_CONF", "config");
            startElasticsearch();

            final String nodesResponse = makeRequest(Request.Get("http://localhost:9200/_nodes"));
            assertThat(nodesResponse, containsString("\"name\":\"relative\""));

            stopElasticsearch();

        } finally {
            rm(tempConf);
        }
    }

    public void test90SecurityCliPackaging() throws Exception {
        final Installation.Executables bin = installation.executables();

        if (distribution().isDefault()) {
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
        } else {
            assertThat(installation.lib.resolve("tools").resolve("security-cli"), fileDoesNotExist());
        }
    }

    public void test91ElasticsearchShardCliPackaging() throws Exception {
        final Installation.Executables bin = installation.executables();

        Platforms.PlatformAction action = () -> {
            final Result result = sh.run(bin.shardTool + " -h");
            assertThat(result.stdout, containsString("A CLI tool to remove corrupted parts of unrecoverable shards"));
        };

        // TODO: this should be checked on all distributions
        if (distribution().isDefault()) {
            Platforms.onLinux(action);
            Platforms.onWindows(action);
        }
    }

    public void test92ElasticsearchNodeCliPackaging() throws Exception {
        final Installation.Executables bin = installation.executables();

        Platforms.PlatformAction action = () -> {
            final Result result = sh.run(bin.nodeTool + " -h");
            assertThat(result.stdout, containsString("A CLI tool to do unsafe cluster and index manipulations on current node"));
        };

        // TODO: this should be checked on all distributions
        if (distribution().isDefault()) {
            Platforms.onLinux(action);
            Platforms.onWindows(action);
        }
    }

    public void test93ElasticsearchNodeCustomDataPathAndNotEsHomeWorkDir() throws Exception {
        Path relativeDataPath = installation.data.relativize(installation.home);
        append(installation.config("elasticsearch.yml"), "path.data: " + relativeDataPath);

        sh.setWorkingDirectory(getTempDir());

        startElasticsearch();
        stopElasticsearch();

        Result result = sh.run("echo y | " + installation.executables().nodeTool + " unsafe-bootstrap");
        assertThat(result.stdout, containsString("Master node was successfully bootstrapped"));
    }

    public void test94ElasticsearchNodeExecuteCliNotEsHomeWorkDir() throws Exception {
        final Installation.Executables bin = installation.executables();
        // Run the cli tools from the tmp dir
        sh.setWorkingDirectory(getTempDir());

        Platforms.PlatformAction action = () -> {
            Result result = sh.run(bin.certutilTool + " -h");
            assertThat(result.stdout, containsString("Simplifies certificate creation for use with the Elastic Stack"));
            result = sh.run(bin.syskeygenTool + " -h");
            assertThat(result.stdout, containsString("system key tool"));
            result = sh.run(bin.setupPasswordsTool + " -h");
            assertThat(result.stdout, containsString("Sets the passwords for reserved users"));
            result = sh.run(bin.usersTool + " -h");
            assertThat(result.stdout, containsString("Manages elasticsearch file users"));
        };

        // TODO: this should be checked on all distributions
        if (distribution().isDefault()) {
            Platforms.onLinux(action);
            Platforms.onWindows(action);
        }
    }

}
