/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import org.apache.lucene.util.Constants;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.compiler.InMemoryJavaCompiler;
import org.elasticsearch.test.jar.JarUtils;
import org.junit.BeforeClass;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.elasticsearch.simdvec.SimdVecLibrary.ENABLE_JDK_VECTOR_LIBRARY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;

public class VectorSystemPropertyTests extends ESTestCase {

    static Path jarPath;

    @BeforeClass
    public static void setup() throws Exception {
        assumeTrue("native scorers are not on Windows", Constants.WINDOWS == false);

        var classBytes = InMemoryJavaCompiler.compile("p.Test", TEST_SOURCE);
        Map<String, byte[]> jarEntries = new HashMap<>();
        jarEntries.put("p/Test.class", classBytes);
        Path topLevelDir = createTempDir();
        jarPath = topLevelDir.resolve("test.jar");
        JarUtils.createJarWithEntries(jarPath, jarEntries);
    }

    public void testSystemPropertyDisabled() throws Exception {
        String output = run(System.getProperty("es.nativelibs.path"), "-D" + ENABLE_JDK_VECTOR_LIBRARY + "=false");

        assertFalse(isLibraryLoaded(output));
    }

    /**
     * {@code es.nativelibs.path} may hold several directories, so a library is whichever entry first has it.
     * Prefixing an entry that does not must therefore leave the outcome alone, whatever it is on this host.
     */
    @SuppressForbidden(reason = "pathSeparator")
    public void testLibraryIsFoundPastAnEntryWithoutIt() throws Exception {
        String nativeLibsPath = System.getProperty("es.nativelibs.path");
        assumeTrue("vec is not available on this host", isLibraryLoaded(run(nativeLibsPath)));

        assertTrue(isLibraryLoaded(run(createTempDir() + File.pathSeparator + nativeLibsPath)));
    }

    private static boolean isLibraryLoaded(String output) {
        assertThat(output, either(containsString("SimdVecLibrary.loaded=true")).or(containsString("SimdVecLibrary.loaded=false")));
        return output.contains("SimdVecLibrary.loaded=true");
    }

    @SuppressForbidden(reason = "pathSeparator")
    private String run(String nativeLibsPath, String... vmArgs) throws Exception {
        var command = new ArrayList<String>();
        command.add(getJavaExecutable());
        command.addAll(List.of(vmArgs));
        command.addAll(
            List.of(
                "-Xms16m",
                "-Xmx16m",
                "-cp",
                jarPath + File.pathSeparator + System.getProperty("java.class.path"),
                "-Des.nativelibs.path=" + nativeLibsPath,
                "p.Test"
            )
        );

        var process = new ProcessBuilder(command).start();
        String output = new String(process.getInputStream().readAllBytes(), UTF_8);
        process.waitFor(30, TimeUnit.SECONDS);
        assertThat(process.exitValue(), equalTo(0));
        return output;
    }

    static String getJavaExecutable() {
        return Path.of(System.getProperty("java.home")).toAbsolutePath().resolve("bin").resolve("java").toString();
    }

    static final String TEST_SOURCE = """
        package p;
        import org.elasticsearch.simdvec.SimdVecLibrary;
        import org.elasticsearch.common.logging.LogConfigurator;

        public class Test {
            static {
                LogConfigurator.configureESLogging(); // native access requires logging to be initialized
            }

            public static void main(String... args) {
                System.out.println("SimdVecLibrary.loaded=" + SimdVecLibrary.instance().isPresent());
            }
        }
        """;
}
