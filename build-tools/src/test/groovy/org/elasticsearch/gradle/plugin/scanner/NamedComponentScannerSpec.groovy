/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.scanner

import net.bytebuddy.ByteBuddy
import net.bytebuddy.dynamic.DynamicType
import spock.lang.Specification

import org.elasticsearch.gradle.internal.test.InMemoryJavaCompiler
import org.elasticsearch.gradle.internal.test.JarUtils
import org.elasticsearch.gradle.internal.test.StableApiJarMocks
import org.elasticsearch.gradle.plugin.scanner.ClassReaders
import org.elasticsearch.gradle.plugin.scanner.NamedComponentScanner
import org.elasticsearch.plugin.scanner.test_classes.ExtensibleClass
import org.elasticsearch.plugin.scanner.test_classes.ExtensibleInterface
import org.elasticsearch.plugin.scanner.test_classes.TestNamedComponent
import org.elasticsearch.plugin.api.Extensible
import org.elasticsearch.plugin.api.NamedComponent
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import org.objectweb.asm.ClassReader

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.stream.Collectors

import static org.hamcrest.MatcherAssert.assertThat
import static org.hamcrest.Matchers.equalTo

class NamedComponentScannerSpec extends Specification {
    @Rule
    TemporaryFolder testProjectDir = new TemporaryFolder()

    private Path tmpDir() throws IOException {
        return testProjectDir.root.toPath();
    }

    NamedComponentScanner namedComponentScanner = new NamedComponentScanner();

    def "named component is found when single class provided"() {
        when:
        Map<String, Map<String, String>> namedComponents = namedComponentScanner.scanForNamedClasses(
            classReaderStream(TestNamedComponent.class, ExtensibleInterface.class)
        )

        then:
        assertThat(
            namedComponents,
            equalTo(
                Map.of(
                    ExtensibleInterface.class.getCanonicalName(),
                    Map.of("test_named_component", TestNamedComponent.class.getCanonicalName())
                )
            )
        )

    }

    def "named components are found when single jar provided"() {
        given:
        final Path tmp = tmpDir();
        final Path dirWithJar = tmp.resolve("jars-dir");
        Files.createDirectories(dirWithJar);
        Path jar = dirWithJar.resolve("plugin.jar");
        JarUtils.createJarWithEntries(
            jar, Map.of(
            "p/A.class", InMemoryJavaCompiler.compile(
            "p.A", """
            package p;
            import org.elasticsearch.plugin.api.*;
            import org.elasticsearch.plugin.scanner.test_classes.*;
            @NamedComponent(name = "a_component")
            public class A extends ExtensibleClass {}
            """
        ), "p/B.class", InMemoryJavaCompiler.compile(
            "p.B", """
            package p;
            import org.elasticsearch.plugin.api.*;
            import org.elasticsearch.plugin.scanner.test_classes.*;
            @NamedComponent(name = "b_component")
            public class B implements ExtensibleInterface{}
            """
        )
        )
        );
        StableApiJarMocks.createPluginApiJar(dirWithJar);
        StableApiJarMocks.createExtensibleApiJar(dirWithJar);//for instance analysis api


        Collection<ClassReader> classReaderStream = ClassReaders.ofDirWithJars(dirWithJar.toString()).collect(Collectors.toList())

        when:
        Map<String, Map<String, String>> namedComponents = namedComponentScanner.scanForNamedClasses(classReaderStream);

        then:
        assertThat(
            namedComponents,
            equalTo(
                Map.of(
                    ExtensibleClass.class.getCanonicalName(),
                    Map.of("a_component", "p.A"),
                    ExtensibleInterface.class.getCanonicalName(),
                    Map.of("b_component", "p.B")
                )
            )
        );
    }

    def "named components can extend common super class"() {
        given:
        Map<String, CharSequence> sources = Map.of(
            "p.CustomExtensibleInterface",
            """
                package p;
                import org.elasticsearch.plugin.api.*;
                import org.elasticsearch.plugin.scanner.test_classes.*;
                public interface CustomExtensibleInterface extends ExtensibleInterface {}
                """,
            // note that this class implements a custom interface
            "p.CustomExtensibleClass",
            """
                package p;
                import org.elasticsearch.plugin.api.*;
                import org.elasticsearch.plugin.scanner.test_classes.*;
                public class CustomExtensibleClass implements CustomExtensibleInterface {}
                """,
            "p.A",
            """
                package p;
                import org.elasticsearch.plugin.api.*;
                import org.elasticsearch.plugin.scanner.test_classes.*;
                @NamedComponent(name = "a_component")
                public class A extends CustomExtensibleClass {}
                """,
            "p.B",
            """
                package p;
                import org.elasticsearch.plugin.api.*;
                import org.elasticsearch.plugin.scanner.test_classes.*;
                @NamedComponent(name = "b_component")
                public class B implements CustomExtensibleInterface{}
                """
        );
        var classToBytes = InMemoryJavaCompiler.compile(sources);

        Map<String, byte[]> jarEntries = new HashMap<>();
        jarEntries.put("p/CustomExtensibleInterface.class", classToBytes.get("p.CustomExtensibleInterface"));
        jarEntries.put("p/CustomExtensibleClass.class", classToBytes.get("p.CustomExtensibleClass"));
        jarEntries.put("p/A.class", classToBytes.get("p.A"));
        jarEntries.put("p/B.class", classToBytes.get("p.B"));

        final Path tmp = tmpDir();
        final Path dirWithJar = tmp.resolve("jars-dir");
        Files.createDirectories(dirWithJar);
        Path jar = dirWithJar.resolve("plugin.jar");
        JarUtils.createJarWithEntries(jar, jarEntries);

        StableApiJarMocks.createPluginApiJar(dirWithJar)
        StableApiJarMocks.createExtensibleApiJar(dirWithJar);//for instance analysis api

        Collection<ClassReader> classReaderStream = ClassReaders.ofDirWithJars(dirWithJar.toString()).collect(Collectors.toList())

        when:
        Map<String, Map<String, String>> namedComponents = namedComponentScanner.scanForNamedClasses(classReaderStream);

        then:
        assertThat(
            namedComponents,
            equalTo(
                Map.of(
                    ExtensibleInterface.class.getCanonicalName(),
                    Map.of(
                        "a_component", "p.A",
                        "b_component", "p.B"
                    )
                )
            )
        );
    }



    private Collection<ClassReader> classReaderStream(Class<?>... classes) {
            try {
                return Arrays.stream(classes).map(
                    clazz -> {
                        String className = classNameToPath(clazz) + ".class";
                        def stream = this.getClass().getClassLoader().getResourceAsStream(className)
                        try (InputStream is = stream) {
                            byte[] classBytes = is.readAllBytes();
                            ClassReader classReader = new ClassReader(classBytes);
                            return classReader;
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }
                ).collect(Collectors.toList())
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

    }

    private String classNameToPath(Class<?> clazz) {
        return clazz.getCanonicalName().replace(".", "/");
    }


}
