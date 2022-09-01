/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.scanners;

import org.elasticsearch.core.PathUtils;
import org.elasticsearch.plugins.scanners.extensible_test_classes.ExtensibleClass;
import org.elasticsearch.plugins.scanners.extensible_test_classes.ExtensibleInterface;
import org.elasticsearch.plugins.scanners.named_components_test_classes.TestNamedComponent;
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
import java.util.Map;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.equalTo;

public class NamedComponentScannerTests extends ESTestCase {
    ExtensiblesRegistry extensiblesRegistry = new ExtensiblesRegistry("file_does_not_exist.txt");// forcing to do classpath scan
    NamedComponentScanner namedComponentScanner = new NamedComponentScanner(extensiblesRegistry);

    public void testReadNamedComponentsFromFile() throws IOException {
        final InputStream resource = this.getClass()
            .getClassLoader()
            .getResourceAsStream("org/elasticsearch/plugins/scanners/named_components.json");

        Map<String, NameToPluginInfo> namedComponents = namedComponentScanner.readFromFile(
            resource,
            TestNamedComponent.class.getClassLoader()
        );

        assertThat(
            namedComponents.get(ExtensibleInterface.class.getCanonicalName()).getForPluginName("test_named_component"),
            equalTo(
                new NamedPluginInfo(
                    "test_named_component",
                    TestNamedComponent.class.getCanonicalName(),
                    TestNamedComponent.class.getClassLoader()
                )
            )
        );
    }

    public void testFindNamedComponentInSingleClass() throws URISyntaxException {
        Map<String, NameToPluginInfo> namedComponents = namedComponentScanner.findNamedComponents(
            classReaderStream(TestNamedComponent.class),
            NamedComponentScannerTests.class.getClassLoader()
        );
        assertThat(
            namedComponents.get(ExtensibleInterface.class.getCanonicalName()).getForPluginName("test_named_component"),
            equalTo(
                new NamedPluginInfo(
                    "test_named_component",
                    TestNamedComponent.class.getCanonicalName(),
                    TestNamedComponent.class.getClassLoader()
                )
            )
        );
    }

    static byte[] bytes(String str) {
        return str.getBytes(UTF_8);
    }

    public void testFindNamedComponentInJarWithNamedComponentscacheFile() throws IOException {
        final Path tmp = createTempDir();
        final Path dirWithJar = tmp.resolve("jars-dir");
        Files.createDirectories(dirWithJar);
        Path jar = dirWithJar.resolve("plugin.jar");
        JarUtils.createJarWithEntries(jar, Map.of("named_components.json", bytes("""
            {
              "p.A":
              "org.elasticsearch.plugins.scanners.extensible_test_classes.ExtensibleClass",
            }
            """), "p/A.class", InMemoryJavaCompiler.compile("p.A", """
            package p;
            import org.elasticsearch.plugin.api.*;
            import org.elasticsearch.plugins.scanners.extensible_test_classes.*;
            @NamedComponent(name = "a_component")
            public class A extends ExtensibleClass {}
            """), "p/B.class", InMemoryJavaCompiler.compile("p.B", """
            package p;
            import org.elasticsearch.plugin.api.*;
            import org.elasticsearch.plugins.scanners.extensible_test_classes.*;
            @NamedComponent(name = "b_component")
            public class B implements ExtensibleInterface{}
            """)));

        ClassLoader classLoader = NamedComponentScannerTests.class.getClassLoader();
        Map<String, NameToPluginInfo> namedComponents = namedComponentScanner.findNamedComponents(
            ClassReaders.ofDirWithJars(dirWithJar.toString()),
            classLoader
        );

        assertThat(
            namedComponents.get(ExtensibleInterface.class.getCanonicalName()).getForPluginName("b_component"),
            equalTo(new NamedPluginInfo("b_component", "p.B", classLoader))
        );
        assertThat(
            namedComponents.get(ExtensibleClass.class.getCanonicalName()).getForPluginName("a_component"),
            equalTo(new NamedPluginInfo("a_component", "p.A", classLoader))
        );
    }

    public void testFindNamedComponentInJar() throws IOException {
        final Path tmp = createTempDir();
        final Path dirWithJar = tmp.resolve("jars-dir");
        Files.createDirectories(dirWithJar);
        Path jar = dirWithJar.resolve("plugin.jar");
        JarUtils.createJarWithEntries(jar, Map.of("p/A.class", InMemoryJavaCompiler.compile("p.A", """
            package p;
            import org.elasticsearch.plugin.api.*;
            import org.elasticsearch.plugins.scanners.extensible_test_classes.*;
            @NamedComponent(name = "a_component")
            public class A extends ExtensibleClass {}
            """), "p/B.class", InMemoryJavaCompiler.compile("p.B", """
            package p;
            import org.elasticsearch.plugin.api.*;
            import org.elasticsearch.plugins.scanners.extensible_test_classes.*;
            @NamedComponent(name = "b_component")
            public class B implements ExtensibleInterface{}
            """)));

        ClassLoader classLoader = NamedComponentScannerTests.class.getClassLoader();
        Map<String, NameToPluginInfo> namedComponents = namedComponentScanner.findNamedComponents(
            ClassReaders.ofDirWithJars(dirWithJar.toString()),
            classLoader
        );

        assertThat(
            namedComponents.get(ExtensibleInterface.class.getCanonicalName()).getForPluginName("b_component"),
            equalTo(new NamedPluginInfo("b_component", "p.B", classLoader))
        );
        assertThat(
            namedComponents.get(ExtensibleClass.class.getCanonicalName()).getForPluginName("a_component"),
            equalTo(new NamedPluginInfo("a_component", "p.A", classLoader))
        );
    }

    public void testCommonSuperClassInJar() throws IOException {

        Map<String, CharSequence> sources = Map.of(
            "p.CustomExtensibleInterface",
            """
                package p;
                import org.elasticsearch.plugin.api.*;
                import org.elasticsearch.plugins.scanners.extensible_test_classes.*;
                public interface CustomExtensibleInterface extends ExtensibleInterface {}
                """,
            // note that this class implements a custom interface
            "p.CustomExtensibleClass",
            """
                package p;
                import org.elasticsearch.plugin.api.*;
                import org.elasticsearch.plugins.scanners.extensible_test_classes.*;
                public class CustomExtensibleClass implements CustomExtensibleInterface {}
                """,
            "p.A",
            """
                package p;
                import org.elasticsearch.plugin.api.*;
                import org.elasticsearch.plugins.scanners.extensible_test_classes.*;
                @NamedComponent(name = "a_component")
                public class A extends CustomExtensibleClass {}
                """,
            "p.B",
            """
                package p;
                import org.elasticsearch.plugin.api.*;
                import org.elasticsearch.plugins.scanners.extensible_test_classes.*;
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

        final Path tmp = createTempDir();
        final Path dirWithJar = tmp.resolve("jars-dir");
        Files.createDirectories(dirWithJar);
        Path jar = dirWithJar.resolve("plugin.jar");
        JarUtils.createJarWithEntries(jar, jarEntries);

        ClassLoader classLoader = NamedComponentScannerTests.class.getClassLoader();
        Map<String, NameToPluginInfo> namedComponents = namedComponentScanner.findNamedComponents(
            ClassReaders.ofDirWithJars(dirWithJar.toString()),
            classLoader
        );

        assertThat(
            namedComponents.get(ExtensibleInterface.class.getCanonicalName()).getForPluginName("b_component"),
            equalTo(new NamedPluginInfo("b_component", "p.B", classLoader))
        );
        assertThat(
            namedComponents.get(ExtensibleInterface.class.getCanonicalName()).getForPluginName("a_component"),
            equalTo(new NamedPluginInfo("a_component", "p.A", classLoader))
        );
    }

    // duplication
    private Stream<ClassReader> classReaderStream(Class<?>... classes) throws URISyntaxException {
        Path mainPath = PathUtils.get(NamedComponentScannerTests.class.getProtectionDomain().getCodeSource().getLocation().toURI());

        return Arrays.stream(classes).map(clazz -> {
            String className = classNameToPath(clazz) + ".class";
            Path path = mainPath.resolve(className);
            try (InputStream is = Files.newInputStream(path)) {
                byte[] classBytes = is.readAllBytes();
                ClassReader classReader = new ClassReader(classBytes);
                return classReader;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        })/*.filter(cr -> cr != null)*/;
    }

    private String classNameToPath(Class<?> clazz) {
        return clazz.getCanonicalName().replace(".", "/");
    }
}
