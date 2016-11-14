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
 * prevents compilation of the dummy controller program.  Even then it will fail to compile
 * the controller if a previous test in the same JVM has installed seccomp.  It works well
 * on old versions of Linux where seccomp is not available, from an IDE, and also should
 * work from a full gradle build once the TODO in qa/evil-tests/build.gradle to run each
 * evil test in a separate JVM is implemented.
 */
public class EvilSpawnerTests extends LuceneTestCase {

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

    public void testControllerSpawn() throws IOException {
        Path esHome = createTempDir().resolve("esHome");
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(Environment.PATH_HOME_SETTING.getKey(), esHome.toString());
        Settings settings = settingsBuilder.build();

        Environment environment = new Environment(settings);

        Path plugin = environment.pluginsFile().resolve("test_plugin");
        Path controllerProgram = Spawner.makeSpawnPath(plugin);
        if (compileControllerProgram(environment, controllerProgram) == false) {
            // Don't fail the test if there's an error compiling the plugin - the build machine probably doesn't have g++ installed
            Loggers.getLogger(EvilSpawnerTests.class).warn("Could not compile native controller program for testing");
            return;
        }

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
            assertEquals(1, stdinReferences.size());
            assertTrue(Files.isRegularFile(helloFile));
            assertFalse(Files.exists(goodbyeFile));
            List<String> lines = Files.readAllLines(helloFile, StandardCharsets.UTF_8);
            assertFalse(lines.isEmpty());
            assertEquals("Hello, world!", lines.get(0));
            // This should cause the native controller to receive an end-of-file on its stdin and hence exit
            stdinReferences.get(0).close();
            // Give the program time to exit
            Thread.sleep(250);
            assertTrue(Files.isRegularFile(goodbyeFile));
            lines = Files.readAllLines(goodbyeFile, StandardCharsets.UTF_8);
            assertFalse(lines.isEmpty());
            assertEquals("Goodbye, world!", lines.get(0));
        } catch (InterruptedException e) {
            // Shouldn't happen, but if it does it's not a problem related to this test
        } finally {
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
