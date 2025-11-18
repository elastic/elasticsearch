/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess;

import org.apache.lucene.util.Constants;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.compiler.InMemoryJavaCompiler;
import org.elasticsearch.test.jar.JarUtils;
import org.junit.BeforeClass;

import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.elasticsearch.nativeaccess.PosixNativeAccess.ENABLE_JDK_VECTOR_LIBRARY;
import static org.hamcrest.Matchers.containsString;
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

    @SuppressForbidden(reason = "pathSeparator")
    public void testSystemPropertyDisabled() throws Exception {
        var process = new ProcessBuilder(
            getJavaExecutable(),
            "-D" + ENABLE_JDK_VECTOR_LIBRARY + "=false",
            "-Xms16m",
            "-Xmx16m",
            "-cp",
            jarPath + File.pathSeparator + System.getProperty("java.class.path"),
            "-Des.nativelibs.path=" + System.getProperty("es.nativelibs.path"),
            "p.Test"
        ).start();
        String output = new String(process.getInputStream().readAllBytes(), UTF_8);
        String error = new String(process.getErrorStream().readAllBytes(), UTF_8);
        // System.out.println(output);
        // System.out.println(error);
        process.waitFor(30, TimeUnit.SECONDS);
        assertThat(output, containsString("getVectorSimilarityFunctions=[Optional.empty]"));
        assertThat(process.exitValue(), equalTo(0));
    }

    static String getJavaExecutable() {
        return Path.of(System.getProperty("java.home")).toAbsolutePath().resolve("bin").resolve("java").toString();
    }

    static final String TEST_SOURCE = """
        package p;
        import org.elasticsearch.nativeaccess.NativeAccess;
        import org.elasticsearch.common.logging.LogConfigurator;

        public class Test {
            static {
                LogConfigurator.loadLog4jPlugins();
                LogConfigurator.configureESLogging(); // native access requires logging to be initialized
            }

            public static void main(String... args) {
                var na = NativeAccess.instance().getVectorSimilarityFunctions();
                System.out.println("getVectorSimilarityFunctions=[" + NativeAccess.instance().getVectorSimilarityFunctions() + "]");
            }
        }
        """;
}
