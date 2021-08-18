/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistClass;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.painless.spi.WhitelistMethod;
import org.elasticsearch.painless.spi.annotation.DeprecatedAnnotation;
import org.elasticsearch.painless.spi.annotation.NoImportAnnotation;
import org.elasticsearch.painless.spi.annotation.WhitelistAnnotationParser;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

public class WhitelistLoaderTests extends ESTestCase {
    public void testUnknownAnnotations() {
        Map<String, WhitelistAnnotationParser> parsers = new HashMap<>(WhitelistAnnotationParser.BASE_ANNOTATION_PARSERS);

        RuntimeException expected = expectThrows(RuntimeException.class, () -> {
            WhitelistLoader.loadFromResourceFiles(Whitelist.class, parsers, "org.elasticsearch.painless.annotation.unknown");
        });
        assertEquals(
            "invalid annotation: parser not found for [unknownAnnotation] [@unknownAnnotation]", expected.getCause().getMessage()
        );
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
                assertEquals("use another method",
                        ((DeprecatedAnnotation)whitelistMethod.painlessAnnotations.get(DeprecatedAnnotation.class)).getMessage());
                assertEquals(1, whitelistMethod.painlessAnnotations.size());
                ++count;
            }

            if ("annotatedTestMethod".equals(whitelistMethod.methodName)) {
                AnnotationTestObject.TestAnnotation ta =
                        ((AnnotationTestObject.TestAnnotation)whitelistMethod.painlessAnnotations.get(
                                AnnotationTestObject.TestAnnotation.class));
                assertEquals("one", ta.getOne());
                assertEquals("two", ta.getTwo());
                assertEquals("three", ta.getThree());
                assertEquals(1, whitelistMethod.painlessAnnotations.size());
                ++count;
            }

            if ("annotatedMultipleMethod".equals(whitelistMethod.methodName)) {
                assertEquals("test",
                        ((DeprecatedAnnotation)whitelistMethod.painlessAnnotations.get(DeprecatedAnnotation.class)).getMessage());
                AnnotationTestObject.TestAnnotation ta =
                        ((AnnotationTestObject.TestAnnotation)whitelistMethod.painlessAnnotations.get(
                                AnnotationTestObject.TestAnnotation.class));
                assertEquals("one", ta.getOne());
                assertEquals("two", ta.getTwo());
                assertEquals("three", ta.getThree());
                assertEquals(2, whitelistMethod.painlessAnnotations.size());
                ++count;
            }
        }

        assertEquals(3, count);
    }
}
