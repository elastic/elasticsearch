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

package org.elasticsearch.painless;

import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistClass;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.painless.spi.WhitelistMethod;
import org.elasticsearch.painless.spi.annotation.DeprecatedAnnotation;
import org.elasticsearch.painless.spi.annotation.NoImportAnnotation;
import org.elasticsearch.painless.spi.annotation.WhitelistAnnotationParser;

import java.util.HashMap;
import java.util.Map;

public class WhitelistLoaderTests extends ScriptTestCase {
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
            "invalid annotation: parser not found for [unknownAnootationWithMessage] [@unknownAnootationWithMessage[arg=\"arg value\"]]",
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
