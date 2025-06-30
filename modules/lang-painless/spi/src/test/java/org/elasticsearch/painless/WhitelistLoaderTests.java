/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistClass;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.painless.spi.WhitelistMethod;
import org.elasticsearch.painless.spi.annotation.DeprecatedAnnotation;
import org.elasticsearch.painless.spi.annotation.NoImportAnnotation;
import org.elasticsearch.painless.spi.annotation.WhitelistAnnotationParser;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.compiler.InMemoryJavaCompiler;
import org.elasticsearch.test.jar.JarUtils;

import java.lang.ModuleLayer.Controller;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class WhitelistLoaderTests extends ESTestCase {

    public void testUnknownAnnotations() {
        Map<String, WhitelistAnnotationParser> parsers = new HashMap<>(WhitelistAnnotationParser.BASE_ANNOTATION_PARSERS);

        RuntimeException expected = expectThrows(RuntimeException.class, () -> {
            WhitelistLoader.loadFromResourceFiles(Whitelist.class, parsers, "org.elasticsearch.painless.annotation.unknown");
        });
        assertEquals("invalid annotation: parser not found for [unknownAnnotation] [@unknownAnnotation]", expected.getCause().getMessage());
        assertEquals(IllegalArgumentException.class, expected.getCause().getClass());

        expected = expectThrows(RuntimeException.class, () -> {
            WhitelistLoader.loadFromResourceFiles(Whitelist.class, parsers, "org.elasticsearch.painless.annotation.unknown_with_options");
        });
        assertEquals(
            "invalid annotation: parser not found for [unknownAnnotationWithMessage] [@unknownAnnotationWithMessage[arg=\"arg value\"]]",
            expected.getCause().getMessage()
        );
        assertEquals(IllegalArgumentException.class, expected.getCause().getClass());
    }

    public void testAnnotations() {
        Map<String, WhitelistAnnotationParser> parsers = new HashMap<>(WhitelistAnnotationParser.BASE_ANNOTATION_PARSERS);
        parsers.put(AnnotationTestObject.TestAnnotation.NAME, AnnotationTestObject.TestAnnotationParser.INSTANCE);
        Whitelist whitelist = WhitelistLoader.loadFromResourceFiles(Whitelist.class, parsers, "org.elasticsearch.painless.annotation");

        assertEquals(1, whitelist.whitelistClasses.size());

        WhitelistClass whitelistClass = whitelist.whitelistClasses.get(0);

        assertNotNull(whitelistClass.painlessAnnotations.get(NoImportAnnotation.class));
        assertEquals(1, whitelistClass.painlessAnnotations.size());
        assertEquals(3, whitelistClass.whitelistMethods.size());

        int count = 0;

        for (WhitelistMethod whitelistMethod : whitelistClass.whitelistMethods) {
            if ("deprecatedMethod".equals(whitelistMethod.methodName)) {
                assertEquals(
                    "use another method",
                    ((DeprecatedAnnotation) whitelistMethod.painlessAnnotations.get(DeprecatedAnnotation.class)).message()
                );
                assertEquals(1, whitelistMethod.painlessAnnotations.size());
                ++count;
            }

            if ("annotatedTestMethod".equals(whitelistMethod.methodName)) {
                AnnotationTestObject.TestAnnotation ta = ((AnnotationTestObject.TestAnnotation) whitelistMethod.painlessAnnotations.get(
                    AnnotationTestObject.TestAnnotation.class
                ));
                assertEquals("one", ta.one());
                assertEquals("two", ta.two());
                assertEquals("three", ta.three());
                assertEquals(1, whitelistMethod.painlessAnnotations.size());
                ++count;
            }

            if ("annotatedMultipleMethod".equals(whitelistMethod.methodName)) {
                assertEquals(
                    "test",
                    ((DeprecatedAnnotation) whitelistMethod.painlessAnnotations.get(DeprecatedAnnotation.class)).message()
                );
                AnnotationTestObject.TestAnnotation ta = ((AnnotationTestObject.TestAnnotation) whitelistMethod.painlessAnnotations.get(
                    AnnotationTestObject.TestAnnotation.class
                ));
                assertEquals("one", ta.one());
                assertEquals("two", ta.two());
                assertEquals("three", ta.three());
                assertEquals(2, whitelistMethod.painlessAnnotations.size());
                ++count;
            }
        }

        assertEquals(3, count);
    }

    public void testMissingWhitelistResource() {
        var e = expectThrows(ResourceNotFoundException.class, () -> WhitelistLoader.loadFromResourceFiles(Whitelist.class, "missing.txt"));
        assertThat(
            e.getMessage(),
            equalTo(
                "Whitelist file [org/elasticsearch/painless/spi/missing.txt] not found"
                    + " from owning class [org.elasticsearch.painless.spi.Whitelist]."
            )
        );
    }

    public void testMissingWhitelistResourceInModule() throws Exception {
        Map<String, CharSequence> sources = new HashMap<>();
        sources.put("module-info", "module m {}");
        sources.put("p.TestOwner", "package p; public class TestOwner { }");
        var classToBytes = InMemoryJavaCompiler.compile(sources);

        Path dir = createTempDir(getTestName());
        Path jar = dir.resolve("m.jar");
        Map<String, byte[]> jarEntries = new HashMap<>();
        jarEntries.put("module-info.class", classToBytes.get("module-info"));
        jarEntries.put("p/TestOwner.class", classToBytes.get("p.TestOwner"));
        jarEntries.put("p/resource.txt", "# test resource".getBytes(StandardCharsets.UTF_8));
        JarUtils.createJarWithEntries(jar, jarEntries);

        try (var loader = JarUtils.loadJar(jar)) {
            Controller controller = JarUtils.loadModule(jar, loader, "m");
            Module module = controller.layer().findModule("m").orElseThrow();

            Class<?> ownerClass = module.getClassLoader().loadClass("p.TestOwner");

            // first check we get a nice error message when accessing the resource
            var e = expectThrows(ResourceNotFoundException.class, () -> WhitelistLoader.loadFromResourceFiles(ownerClass, "resource.txt"));
            assertThat(
                e.getMessage(),
                equalTo(
                    "Whitelist file [p/resource.txt] not found from owning class [p.TestOwner]."
                        + " Check that the file exists and the package [p] is opened to module null"
                )
            );

            // now check we can actually read it once the package is opened to us
            controller.addOpens(module, "p", WhitelistLoader.class.getModule());
            var whitelist = WhitelistLoader.loadFromResourceFiles(ownerClass, "resource.txt");
            assertThat(whitelist, notNullValue());
        }
    }
}
