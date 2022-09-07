/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.scanner.impl;

import org.elasticsearch.plugin.api.Extensible;
import org.elasticsearch.plugin.scanner.impl.ClassReaders;
import org.elasticsearch.plugin.scanner.impl.ClassScanner;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Map;

public class ClassScannerTests extends ESTestCase {

    public void testExtensibleInHierarchy() throws IOException {
        ClassScanner reader = new ClassScanner(Extensible.class, (classname, map) -> {
            map.put(classname, classname);
            return null;
        });
        reader.visit(ClassReaders.ofClassPath());
        Map<String, String> extensibleClasses = reader.getFoundClasses();

        assertThat(
            extensibleClasses,
            Matchers.allOf(
                Matchers.hasEntry(
                    "org/elasticsearch/plugin/scanner/extensible_test_classes/ExtensibleClass",
                    "org/elasticsearch/plugin/scanner/extensible_test_classes/ExtensibleClass"
                ),
                Matchers.hasEntry(
                    "org/elasticsearch/plugin/scanner/extensible_test_classes/ImplementingExtensible",
                    "org/elasticsearch/plugin/scanner/extensible_test_classes/ExtensibleInterface"
                ),
                Matchers.hasEntry(
                    "org/elasticsearch/plugin/scanner/extensible_test_classes/SubClass",
                    "org/elasticsearch/plugin/scanner/extensible_test_classes/ExtensibleClass"
                )
            )
        );
    }
}
