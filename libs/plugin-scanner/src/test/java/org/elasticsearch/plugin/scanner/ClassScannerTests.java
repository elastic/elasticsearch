/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.scanner;

import org.elasticsearch.plugin.Extensible;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.Type;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ClassScannerTests extends ESTestCase {
    static final System.Logger logger = System.getLogger(ClassScannerTests.class.getName());

    public void testClassAndInterfaceHierarchy() throws IOException {
        var reader = new ClassScanner(Type.getDescriptor(Extensible.class), (classname, map) -> {
            map.put(classname, classname);
            return null;
        });
        List<ClassReader> classReaders = ClassReaders.ofClassPath();
        logger.log(System.Logger.Level.INFO, "classReaderStream size " + classReaders.size());

        reader.visit(classReaders);
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
