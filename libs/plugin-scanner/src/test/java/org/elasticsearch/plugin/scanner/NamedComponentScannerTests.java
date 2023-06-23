/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.scanner;

import org.elasticsearch.core.IOUtils;
import org.elasticsearch.plugin.scanner.test_model.ExtensibleClass;
import org.elasticsearch.plugin.scanner.test_model.ExtensibleInterface;
import org.elasticsearch.plugin.scanner.test_model.TestNamedComponent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.compiler.InMemoryJavaCompiler;
import org.elasticsearch.test.jar.JarUtils;
import org.objectweb.asm.ClassReader;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class NamedComponentScannerTests extends ESTestCase {

    private Path tmpDir() throws IOException {
        return createTempDir();
    }

    NamedComponentScanner namedComponentScanner = new NamedComponentScanner();

    public void testFindNamedComponentInSingleClass() throws URISyntaxException {
        Map<String, Map<String, String>> namedComponents = namedComponentScanner.scanForNamedClasses(
            classReaderStream(TestNamedComponent.class, ExtensibleInterface.class)
        );

        org.hamcrest.MatcherAssert.assertThat(
            namedComponents,
            equalTo(
                Map.of(
                    ExtensibleInterface.class.getCanonicalName(),
                    Map.of("test_named_component", TestNamedComponent.class.getCanonicalName())
                )
            )
        );

    }

    public void testNamedComponentsAreFoundWhenSingleJarProvided() throws IOException {
        final Path tmp = tmpDir();
        final Path dirWithJar = tmp.resolve("jars-dir");
        Files.createDirectories(dirWithJar);
        Path jar = dirWithJar.resolve("plugin.jar");
        JarUtils.createJarWithEntries(jar, Map.of("p/A.class", InMemoryJavaCompiler.compile("p.A", """
            package p;
            import org.elasticsearch.plugin.*;
            import org.elasticsearch.plugin.scanner.test_model.*;
            @NamedComponent("a_component")
            public class A extends ExtensibleClass {}
            """), "p/B.class", InMemoryJavaCompiler.compile("p.B", """
            package p;
            import org.elasticsearch.plugin.*;
            import org.elasticsearch.plugin.scanner.test_model.*;
            @NamedComponent("b_component")
            public class B implements ExtensibleInterface{}
            """)));
        List<ClassReader> classReaderStream = Stream.concat(
            ClassReaders.ofDirWithJars(dirWithJar).stream(),
            ClassReaders.ofClassPath().stream()
        )// contains plugin-api
            .toList();

        Map<String, Map<String, String>> namedComponents = namedComponentScanner.scanForNamedClasses(classReaderStream);

        org.hamcrest.MatcherAssert.assertThat(
            namedComponents,
            equalTo(
                Map.of(
                    ExtensibleClass.class.getCanonicalName(),
                    Map.of("a_component", "p.A"),
                    ExtensibleInterface.class.getCanonicalName(),
                    Map.of(
                        "b_component",
                        "p.B",
                        // noise from classpath
                        "test_named_component",
                        "org.elasticsearch.plugin.scanner.test_model.TestNamedComponent"
                    )
                )
            )
        );

        // aggressively delete the jar dir, so that any leaked filed handles fail this specific test on windows
        IOUtils.rm(tmp);
    }

    public void testNamedComponentsCanExtednCommonSuperClass() throws IOException {
        Map<String, CharSequence> sources = Map.of(
            "p.CustomExtensibleInterface",
            """
                package p;
                import org.elasticsearch.plugin.*;
                import org.elasticsearch.plugin.scanner.test_model.*;
                public interface CustomExtensibleInterface extends ExtensibleInterface {}
                """,
            // note that this class implements a custom interface
            "p.CustomExtensibleClass",
            """
                package p;
                import org.elasticsearch.plugin.*;
                import org.elasticsearch.plugin.scanner.test_model.*;
                public class CustomExtensibleClass implements CustomExtensibleInterface {}
                """,
            "p.A",
            """
                package p;
                import org.elasticsearch.plugin.*;
                import org.elasticsearch.plugin.scanner.test_model.*;
                @NamedComponent("a_component")
                public class A extends CustomExtensibleClass {}
                """,
            "p.B",
            """
                package p;
                import org.elasticsearch.plugin.*;
                import org.elasticsearch.plugin.scanner.test_model.*;
                @NamedComponent("b_component")
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

        Stream<ClassReader> classPath = ClassReaders.ofClassPath().stream();
        List<ClassReader> classReaders = Stream.concat(ClassReaders.ofDirWithJars(dirWithJar).stream(), classPath)// contains plugin-api
            .toList();

        Map<String, Map<String, String>> namedComponents = namedComponentScanner.scanForNamedClasses(classReaders);

        org.hamcrest.MatcherAssert.assertThat(
            namedComponents,
            equalTo(
                Map.of(
                    ExtensibleInterface.class.getCanonicalName(),
                    Map.of(
                        "a_component",
                        "p.A",
                        "b_component",
                        "p.B",
                        "test_named_component",
                        "org.elasticsearch.plugin.scanner.test_model.TestNamedComponent"// noise from classpath
                    )
                )
            )
        );

        // aggressively delete the jar dir, so that any leaked filed handles fail this specific test on windows
        IOUtils.rm(tmp);
    }

    public void testWriteToFile() throws IOException {
        Map<String, String> extensibleInterfaceComponents = new LinkedHashMap<>();
        extensibleInterfaceComponents.put("a_component", "p.A");
        extensibleInterfaceComponents.put("b_component", "p.B");
        Map<String, Map<String, String>> mapToWrite = new LinkedHashMap<>();
        mapToWrite.put(ExtensibleInterface.class.getCanonicalName(), extensibleInterfaceComponents);

        Path path = tmpDir().resolve("file.json");
        namedComponentScanner.writeToFile(mapToWrite, path);

        String jsonMap = Files.readString(path);
        assertThat(jsonMap, equalTo("""
            {
              "org.elasticsearch.plugin.scanner.test_model.ExtensibleInterface": {
                "a_component": "p.A",
                "b_component": "p.B"
              }
            }
            """.replaceAll("[\n\r\s]", "")));
    }

    private List<ClassReader> classReaderStream(Class<?>... classes) {
        try {
            return Arrays.stream(classes).map(clazz -> {
                String className = classNameToPath(clazz) + ".class";
                var stream = this.getClass().getClassLoader().getResourceAsStream(className);
                try (InputStream is = stream) {
                    byte[] classBytes = is.readAllBytes();
                    ClassReader classReader = new ClassReader(classBytes);
                    return classReader;
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }).collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String classNameToPath(Class<?> clazz) {
        return clazz.getCanonicalName().replace(".", "/");
    }

}
