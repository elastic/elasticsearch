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

package org.elasticsearch.bootstrap;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.monitor.jvm.JvmInfo;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Compile a simple daemon controller, put it in the right place and check that it runs.
 * Ignore the problem of g++ not being installed to avoid making g++ a pre-requisite for
 * building Elasticsearch.
 *
 * Extends LuceneTestCase rather than ESTestCase as ESTestCase installs seccomp, and that
 * prevents the Spawner class doing its job.  Also needs to run in a separate JVM to other
 * tests that extend ESTestCase for the same reason.
 */
public class SpawnerNoBootstrapTests extends LuceneTestCase {

    private static final String CPP_COMPILER = "/usr/bin/g++";

    private static final String HELLO_FILE_NAME = JvmInfo.jvmInfo().pid() + "hello.txt";
    private static final String GOODBYE_FILE_NAME = JvmInfo.jvmInfo().pid() + "goodbye.txt";

    private static final String CONTROLLER_SOURCE = "\n"
        + "#include <fstream>\n"
        + "#include <iostream>\n"
        + "#include <string>\n"
        + "\n"
        + "#include <stdlib.h>\n"
        + "\n"
        + "int main(int, char **) {\n"
        + "    const char *tmpDir = ::getenv(\"TMPDIR\");\n"
        + "    if (tmpDir == 0) {\n"
        + "        return EXIT_FAILURE;\n"
        + "    }\n"
        + "\n"
        + "    std::string helloFile(tmpDir);\n"
        + "    helloFile += '/';\n"
        + "    helloFile += \"" + HELLO_FILE_NAME + "\";\n"
        + "    std::string goodbyeFile(tmpDir);\n"
        + "    goodbyeFile += '/';\n"
        + "    goodbyeFile += \"" + GOODBYE_FILE_NAME + "\";\n"
        + "\n"
        + "    std::ofstream helloStrm(helloFile.c_str());\n"
        + "    helloStrm << \"Hello, world!\\n\";\n"
        + "    helloStrm.close();\n"
        + "\n"
        + "    // Assuming no input on stdin, this will pause the program until end-of-file on stdin\n"
        + "    char c;\n"
        + "    std::cin >> c;\n"
        + "\n"
        + "    std::ofstream goodbyeStrm(goodbyeFile.c_str());\n"
        + "    goodbyeStrm << \"Goodbye, world!\\n\";\n"
        + "    goodbyeStrm.close();\n"
        + "\n"
        + "    return EXIT_SUCCESS;\n"
        + "}\n";

    public void testControllerSpawn() throws IOException, InterruptedException {
        Path esHome = createTempDir().resolve("esHome");
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(Environment.PATH_HOME_SETTING.getKey(), esHome.toString());
        Settings settings = settingsBuilder.build();

        Environment environment = new Environment(settings);

        // This plugin will have a controller daemon built for it
        Path plugin = environment.pluginsFile().resolve("test_plugin");
        Files.createDirectories(plugin);
        Path controllerProgram = Spawner.makeSpawnPath(plugin);
        if (compileControllerProgram(environment, controllerProgram) == false) {
            // Don't fail the test if there's an error compiling the plugin - the build machine probably doesn't have g++ installed
            Loggers.getLogger(SpawnerNoBootstrapTests.class).warn("Could not compile native controller program for testing");
            return;
        }
        Loggers.getLogger(SpawnerNoBootstrapTests.class).info("Successfully compiled native controller program for testing");

        // This plugin will NOT have a controller daemon
        Path otherPlugin = environment.pluginsFile().resolve("other_plugin");
        Files.createDirectories(otherPlugin);

        Path helloFile = environment.tmpFile().resolve(HELLO_FILE_NAME);
        Path goodbyeFile = environment.tmpFile().resolve(GOODBYE_FILE_NAME);

        // Clean up any files left behind by previous debugging
        Files.deleteIfExists(helloFile);
        Files.deleteIfExists(goodbyeFile);

        Spawner spawner = new Spawner();
        spawner.spawnNativePluginControllers(environment);

        try {
            // Give the program time to start up
            Thread.sleep(500);
            List<OutputStream> stdinReferences = spawner.getStdinReferences();
            // 1 because there should only be a reference in the list for the plugin that had the controller daemon, not the other plugin
            assertEquals(1, stdinReferences.size());
            assertTrue(Files.isRegularFile(helloFile));
            assertFalse(Files.exists(goodbyeFile));
            List<String> lines = Files.readAllLines(helloFile, StandardCharsets.UTF_8);
            assertFalse(lines.isEmpty());
            assertEquals("Hello, world!", lines.get(0));
            // This should cause the native controller to receive an end-of-file on its stdin and hence exit
            spawner.close();
            // Give the program time to exit
            Thread.sleep(250);
            assertTrue(Files.isRegularFile(goodbyeFile));
            lines = Files.readAllLines(goodbyeFile, StandardCharsets.UTF_8);
            assertFalse(lines.isEmpty());
            assertEquals("Goodbye, world!", lines.get(0));
        } finally {
            // These have to go in java.io.tmpdir rather than a LuceneTestCase managed temporary directory
            // because the Spawner class only tells the spawned program about java.io.tmpdir
            Files.deleteIfExists(helloFile);
            Files.deleteIfExists(goodbyeFile);
        }
    }

    private boolean compileControllerProgram(Environment environment, Path outputFile) {
        boolean compileSucceeded = false;
        try {
            Path sourceFile = Files.write(Files.createTempFile(environment.tmpFile(), "controller", ".cc"),
                    CONTROLLER_SOURCE.getBytes(StandardCharsets.UTF_8));
            Path outputDir = outputFile.getParent();
            Files.createDirectories(outputDir);
            Process proc = Runtime.getRuntime().exec(new String[] { CPP_COMPILER, "-o", outputFile.toString(), sourceFile.toString() });
            compileSucceeded = proc.waitFor() == 0;
            Files.delete(sourceFile);
        } catch (IOException | InterruptedException e) {
            return false;
        }
        return compileSucceeded;
    }
}
