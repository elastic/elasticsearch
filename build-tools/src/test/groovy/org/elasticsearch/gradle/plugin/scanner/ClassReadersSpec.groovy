/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.plugin.scanner

import spock.lang.Specification

import org.elasticsearch.gradle.internal.test.InMemoryJavaCompiler;

import org.elasticsearch.gradle.internal.test.JarUtils
import org.hamcrest.Matchers
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import org.objectweb.asm.ClassReader

import java.nio.file.Files
import java.nio.file.Path
import java.util.stream.Collectors
import java.util.stream.Stream

import static org.hamcrest.MatcherAssert.assertThat

class ClassReadersSpec extends Specification {
    @Rule
    TemporaryFolder testProjectDir = new TemporaryFolder()

    private Path tmpDir() throws IOException {
        return testProjectDir.root.toPath();
    }

    def "module-info is not returned as a class from jar"() {
        when:
        final Path tmp = tmpDir();
        final Path dirWithJar = tmp.resolve("jars-dir");
        Files.createDirectories(dirWithJar);
        Path jar = dirWithJar.resolve("api.jar");
        JarUtils.createJarWithEntries(
            jar, Map.of(
            "module-info.class", InMemoryJavaCompiler.compile(
            "module-info", """
            module p {}
            """)
            )
        )


        then:
        try (Stream<ClassReader> classReaderStream = ClassReaders.ofPaths(Stream.of(jar))) {

            assertThat(classReaderStream.collect(Collectors.toList()), Matchers.empty());
        }
    }


    def "two classes are returned in a stream from jar"() {
        when:
        final Path tmp = tmpDir();
        final Path dirWithJar = tmp.resolve("jars-dir");
        Files.createDirectories(dirWithJar);
        Path jar = dirWithJar.resolve("api.jar");
        JarUtils.createJarWithEntries(
            jar, Map.of(
            "p/A.class", InMemoryJavaCompiler.compile(
            "p.A", """
            package p;
            public class A {}
            """),
        "p/B.class", InMemoryJavaCompiler.compile(
            "p.B", """
            package p;
            public class B {}
            """)
            )
        );


        then:
        try (Stream<ClassReader> classReaderStream = ClassReaders.ofPaths(Stream.of(jar))) {
            List<String> collect = classReaderStream.map(cr -> cr.getClassName()).collect(Collectors.toList());
            assertThat(collect, Matchers.containsInAnyOrder("p/A", "p/B"));
        }
    }


    def "on a classpath jars and individual classes are returned"() {
        when:
        final Path tmp = tmpDir();
        final Path dirWithJar = tmp.resolve("jars-dir");
        Files.createDirectories(dirWithJar);

        Path jar = dirWithJar.resolve("a_b.jar");
        JarUtils.createJarWithEntries(
            jar, Map.of(
            "p/A.class", InMemoryJavaCompiler.compile(
            "p.A", """
            package p;
            public class A {}
            """),
        "p/B.class", InMemoryJavaCompiler.compile(
            "p.B", """
            package p;
            public class B {}
            """)
            )
        );

        Path jar2 = dirWithJar.resolve("c_d.jar");
        JarUtils.createJarWithEntries(
            jar2, Map.of(
            "p/C.class", InMemoryJavaCompiler.compile(
            "p.C", """
            package p;
            public class C {}
            """),
        "p/D.class", InMemoryJavaCompiler.compile(
            "p.D", """
            package p;
            public class D {}
            """)
            )
        );

        InMemoryJavaCompiler.compile(
            "p.E", """
            package p;
            public class E {}
            """
        );
        Files.write(
            tmp.resolve("E.class"), InMemoryJavaCompiler.compile(
            "p.E", """
            package p;
            public class E {}
            """)
        );


        then:
        try (Stream<ClassReader> classReaderStream = ClassReaders.ofPaths(Stream.of(tmp, jar, jar2))) {

            List<String> collect = classReaderStream.map(cr -> cr.getClassName()).collect(Collectors.toList());
            assertThat(collect, Matchers.containsInAnyOrder("p/A", "p/B", "p/C", "p/D", "p/E"));
        }
    }

    def "classes from multiple jars in a dir are returned"() {
        when:
        final Path tmp = tmpDir();
        final Path dirWithJar = tmp.resolve("jars-dir");
        Files.createDirectories(dirWithJar);


        Path jar = dirWithJar.resolve("a_b.jar");
        JarUtils.createJarWithEntries(
            jar, Map.of(
            "p/A.class", InMemoryJavaCompiler.compile(
            "p.A", """
            package p;
            public class A {}
            """),
        "p/B.class", InMemoryJavaCompiler.compile(
            "p.B", """
            package p;
            public class B {}
            """)
            )
        );

        Path jar2 = dirWithJar.resolve("c_d.jar");
        JarUtils.createJarWithEntries(
            jar2, Map.of(
            "p/C.class", InMemoryJavaCompiler.compile(
            "p.C", """
            package p;
            public class C {}
            """),
        "p/D.class", InMemoryJavaCompiler.compile(
            "p.D", """
            package p;
            public class D {}
            """)
            )
        );

        then:
        try (Stream<ClassReader> classReaderStream = ClassReaders.ofDirWithJars(dirWithJar.toString())) {
            List<String> collect = classReaderStream.map(cr -> cr.getClassName()).collect(Collectors.toList());
            assertThat(collect, Matchers.containsInAnyOrder("p/A", "p/B", "p/C", "p/D"));
        }
    }
}
