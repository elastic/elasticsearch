/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.precommit.transport;

import org.elasticsearch.gradle.internal.precommit.transport.test_classes.ExampleImpl;
import org.elasticsearch.gradle.internal.precommit.transport.test_classes.ExampleSubInterface;
import org.elasticsearch.gradle.internal.precommit.transport.test_classes.ExampleSubclass;
import org.elasticsearch.gradle.internal.precommit.transport.test_classes.WriteableSubClass;
import org.elasticsearch.gradle.internal.precommit.transport.test_classes.WriteableSubInterface;
import org.elasticsearch.gradle.internal.precommit.transport.test_classes.Writeable;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.objectweb.asm.ClassReader;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;

public class ClassHierarchyScannerTests {
    @Test
    public void testFindOneFile() throws IOException {
        ClassHierarchyScanner scanner = new ClassHierarchyScanner(Writeable.class.getCanonicalName());

        ClassReader classReader = new ClassReader(ExampleImpl.class.getCanonicalName());

        classReader.accept(scanner, ClassReader.SKIP_CODE);

        String canonicalName = classNameToPath(Writeable.class.getCanonicalName());
        Set<String> transportClasses = scanner.subClassesOf(Map.of(canonicalName, canonicalName));

        assertThat(transportClasses,
            Matchers.contains("org/elasticsearch/gradle/internal/precommit/transport/test_classes/ExampleImpl"));
    }

    private String classNameToPath(String className) {
        return className.replace('.', '/');
    }

    @Test
    public void testFindAll() throws IOException {
        ClassHierarchyScanner scanner = new ClassHierarchyScanner(Writeable.class.getCanonicalName());

        ClassReader writeable = new ClassReader(Writeable.class.getCanonicalName());
        ClassReader subInterfaceWriteable = new ClassReader(WriteableSubInterface.class.getCanonicalName());
        ClassReader subclassWriteable = new ClassReader(WriteableSubClass.class.getCanonicalName());

        ClassReader cr1 = new ClassReader(ExampleSubInterface.class.getCanonicalName());
        ClassReader cr2 = new ClassReader(ExampleSubclass.class.getCanonicalName());
        ClassReader cr3 = new ClassReader(ExampleImpl.class.getCanonicalName());

        Stream.of(cr1, cr2, cr3)
            .forEach(cr -> cr.accept(scanner, ClassReader.SKIP_CODE));

        Set<String> transportClasses = scanner.subClassesOf(Map.of(
            classNameToPath(Writeable.class.getCanonicalName()), classNameToPath(Writeable.class.getCanonicalName()),
            classNameToPath(WriteableSubInterface.class.getCanonicalName()), classNameToPath(Writeable.class.getCanonicalName()),
            classNameToPath(WriteableSubClass.class.getCanonicalName()), classNameToPath(Writeable.class.getCanonicalName())
        ));

        assertThat(transportClasses,
            Matchers.allOf(
                Matchers.hasItem("org/elasticsearch/gradle/internal/precommit/transport/test_classes/ExampleImpl"),
                Matchers.hasItem("org/elasticsearch/gradle/internal/precommit/transport/test_classes/ExampleSubclass"),
                Matchers.hasItem("org/elasticsearch/gradle/internal/precommit/transport/test_classes/ExampleSubInterface")
            ));
    }


}
