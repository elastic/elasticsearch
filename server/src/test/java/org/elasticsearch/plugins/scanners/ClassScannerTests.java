/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.scanners;

import org.elasticsearch.plugin.api.Extensible;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Map;

public class ClassScannerTests extends ESTestCase {

    public void testExtensibleInHierarchy() throws IOException {
        ClassScanner reader = new ClassScanner(Extensible.class,
            (classname, map) -> map.put(classname, classname));
        reader.visit(ClassReaders.ofClassPath());
        Map<String, String> extensibleClasses = reader.getExtensibleClasses();

        assertThat(extensibleClasses, Matchers.allOf(Matchers.hasEntry(
                "org/elasticsearch/plugins/scanners/testclasses/DirectlyAnnotatedExtensible",
                "org/elasticsearch/plugins/scanners/testclasses/DirectlyAnnotatedExtensible"),
            Matchers.hasEntry(
                "org/elasticsearch/plugins/scanners/testclasses/ImplementingExtensible",
                "org/elasticsearch/plugins/scanners/testclasses/ExtensibleInterface"),
            Matchers.hasEntry("org/elasticsearch/plugins/scanners/testclasses/SubClass",
                "org/elasticsearch/plugins/scanners/testclasses/DirectlyAnnotatedExtensible")));
    }
}
