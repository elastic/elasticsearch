/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.scanner;

import org.elasticsearch.core.IOUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.compiler.InMemoryJavaCompiler;
import org.elasticsearch.test.jar.JarUtils;
import org.hamcrest.Matchers;
import org.objectweb.asm.ClassReader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ClassReadersTests extends ESTestCase {

    public void testModuleInfoIsNotReturnedAsAClassFromJar() throws IOException {
        final Path tmp = createTempDir(getTestName());
        final Path dirWithJar = tmp.resolve("jars-dir");
        Files.createDirectories(dirWithJar);
        Path jar = dirWithJar.resolve("api.jar");
        JarUtils.createJarWithEntries(jar, Map.of("module-info.class", InMemoryJavaCompiler.compile("module-info", """
            module p {}
            """)));

        List<ClassReader> classReaders = ClassReaders.ofPaths(Stream.of(jar));
        org.hamcrest.MatcherAssert.assertThat(classReaders, Matchers.empty());

        // aggressively delete the jar dir, so that any leaked filed handles fail this specific test on windows
        IOUtils.rm(tmp);
    }

    public void testTwoClassesInAStreamFromJar() throws IOException {
        final Path tmp = createTempDir(getTestName());
        final Path dirWithJar = tmp.resolve("jars-dir");
        Files.createDirectories(dirWithJar);
        Path jar = dirWithJar.resolve("api.jar");
        JarUtils.createJarWithEntries(jar, Map.of("p/A.class", InMemoryJavaCompiler.compile("p.A", """
            package p;
            public class A {}
            """), "p/B.class", InMemoryJavaCompiler.compile("p.B", """
            package p;
            public class B {}
            """)));

        List<ClassReader> classReaders = ClassReaders.ofPaths(Stream.of(jar));
        List<String> collect = classReaders.stream().map(cr -> cr.getClassName()).collect(Collectors.toList());
        org.hamcrest.MatcherAssert.assertThat(collect, Matchers.containsInAnyOrder("p/A", "p/B"));

        // aggressively delete the jar dir, so that any leaked filed handles fail this specific test on windows
        IOUtils.rm(tmp);
    }

    public void testStreamOfJarsAndIndividualClasses() throws IOException {
        final Path tmp = createTempDir(getTestName());
        final Path dirWithJar = tmp.resolve("jars-dir");
        Files.createDirectories(dirWithJar);

        Path jar = dirWithJar.resolve("a_b.jar");
        JarUtils.createJarWithEntries(jar, Map.of("p/A.class", InMemoryJavaCompiler.compile("p.A", """
            package p;
            public class A {}
            """), "p/B.class", InMemoryJavaCompiler.compile("p.B", """
            package p;
            public class B {}
            """)));

        Path jar2 = dirWithJar.resolve("c_d.jar");
        JarUtils.createJarWithEntries(jar2, Map.of("p/C.class", InMemoryJavaCompiler.compile("p.C", """
            package p;
            public class C {}
            """), "p/D.class", InMemoryJavaCompiler.compile("p.D", """
            package p;
            public class D {}
            """)));

        InMemoryJavaCompiler.compile("p.E", """
            package p;
            public class E {}
            """);
        Files.write(tmp.resolve("E.class"), InMemoryJavaCompiler.compile("p.E", """
            package p;
            public class E {}
            """));

        List<ClassReader> classReaders = ClassReaders.ofPaths(Stream.of(tmp, jar, jar2));
        List<String> collect = classReaders.stream().map(cr -> cr.getClassName()).collect(Collectors.toList());
        org.hamcrest.MatcherAssert.assertThat(collect, Matchers.containsInAnyOrder("p/A", "p/B", "p/C", "p/D", "p/E"));
    }

    public void testMultipleJarsInADir() throws IOException {
        final Path tmp = createTempDir(getTestName());
        final Path dirWithJar = tmp.resolve("jars-dir");
        Files.createDirectories(dirWithJar);

        Path jar = dirWithJar.resolve("a_b.jar");
        JarUtils.createJarWithEntries(jar, Map.of("p/A.class", InMemoryJavaCompiler.compile("p.A", """
            package p;
            public class A {}
            """), "p/B.class", InMemoryJavaCompiler.compile("p.B", """
            package p;
            public class B {}
            """)));

        Path jar2 = dirWithJar.resolve("c_d.jar");
        JarUtils.createJarWithEntries(jar2, Map.of("p/C.class", InMemoryJavaCompiler.compile("p.C", """
            package p;
            public class C {}
            """), "p/D.class", InMemoryJavaCompiler.compile("p.D", """
            package p;
            public class D {}
            """)));

        List<ClassReader> classReaders = ClassReaders.ofDirWithJars(dirWithJar);
        List<String> collect = classReaders.stream().map(cr -> cr.getClassName()).collect(Collectors.toList());
        org.hamcrest.MatcherAssert.assertThat(collect, Matchers.containsInAnyOrder("p/A", "p/B", "p/C", "p/D"));
    }
}
