/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.scanner;

import org.elasticsearch.plugin.api.Extensible;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.Type;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ClassScannerTests extends ESTestCase {
    static final System.Logger logger = System.getLogger(ClassScannerTests.class.getName());

    public void testClassAndInterfaceHierarchy() throws IOException {
        var reader = new ClassScanner(Type.getDescriptor(Extensible.class), (classname, map) -> {
            map.put(classname, classname);
            return null;
        });
        Stream<ClassReader> classReaderStream = ClassReaders.ofClassPath();
        logger.log(System.Logger.Level.INFO, "classReaderStream size " + ClassReaders.ofClassPath().collect(Collectors.toList()).size());

        reader.visit(classReaderStream);
        Map<String, String> extensibleClasses = reader.getFoundClasses();

        org.hamcrest.MatcherAssert.assertThat(
            extensibleClasses,
            Matchers.allOf(
                Matchers.hasEntry(
                    "org/elasticsearch/plugin/scanner/test_model/ExtensibleClass",
                    "org/elasticsearch/plugin/scanner/test_model/ExtensibleClass"
                ),
                Matchers.hasEntry(
                    "org/elasticsearch/plugin/scanner/test_model/ImplementingExtensible",
                    "org/elasticsearch/plugin/scanner/test_model/ExtensibleInterface"
                ),
                Matchers.hasEntry(
                    "org/elasticsearch/plugin/scanner/test_model/SubClass",
                    "org/elasticsearch/plugin/scanner/test_model/ExtensibleClass"
                )
            )
        );
    }

}
