/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.precommit.transport;

import org.elasticsearch.gradle.internal.precommit.transport.test_classes.EnclosingInnerTransport;
import org.elasticsearch.gradle.internal.precommit.transport.test_classes.ExampleImpl;
import org.elasticsearch.gradle.internal.precommit.transport.test_classes.ExampleSubInterface;
import org.elasticsearch.gradle.internal.precommit.transport.test_classes.ExampleSubclass;
import org.elasticsearch.gradle.internal.precommit.transport.test_classes.Writeable;
import org.elasticsearch.gradle.internal.precommit.transport.test_classes.WriteableSubClass;
import org.elasticsearch.gradle.internal.precommit.transport.test_classes.WriteableSubInterface;
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
    public void testInnerClasses() throws IOException {
        ClassHierarchyScanner scanner = new ClassHierarchyScanner();
        ClassReader enclosing = new ClassReader(EnclosingInnerTransport.class.getCanonicalName());
        ClassReader enclosed = new ClassReader(EnclosingInnerTransport.Transport.class.getName());

        Stream.of(enclosing, enclosed).forEach(cr -> cr.accept(scanner, ClassReader.SKIP_CODE));

        String className = classNameToPath(Writeable.class.getCanonicalName());
        Set<String> transportClasses = scanner.getConcreteSubclasses(Map.of(className, className));

        // for inner classes report their enclosing class instead
        assertThat(
            transportClasses,
            Matchers.contains("org/elasticsearch/gradle/internal/precommit/transport/test_classes/EnclosingInnerTransport$Transport")
        );

    }

    @Test
    public void testFindOneFile() throws IOException {
        ClassHierarchyScanner scanner = new ClassHierarchyScanner();

        ClassReader classReader = new ClassReader(ExampleImpl.class.getCanonicalName());

        classReader.accept(scanner, ClassReader.SKIP_CODE);

        String className = classNameToPath(Writeable.class.getCanonicalName());
        Set<String> transportClasses = scanner.getConcreteSubclasses(Map.of(className, className));

        assertThat(transportClasses, Matchers.contains("org/elasticsearch/gradle/internal/precommit/transport/test_classes/ExampleImpl"));
    }

    private String classNameToPath(String className) {
        return className.replace('.', '/');
    }

    @Test
    public void testFindAll() throws IOException {
        ClassHierarchyScanner scanner = new ClassHierarchyScanner();

        ClassReader writeable = new ClassReader(Writeable.class.getCanonicalName());
        ClassReader subInterfaceWriteable = new ClassReader(WriteableSubInterface.class.getCanonicalName());
        ClassReader subclassWriteable = new ClassReader(WriteableSubClass.class.getCanonicalName());

        ClassReader cr1 = new ClassReader(ExampleSubInterface.class.getCanonicalName());
        ClassReader cr2 = new ClassReader(ExampleSubclass.class.getCanonicalName());
        ClassReader cr3 = new ClassReader(ExampleImpl.class.getCanonicalName());

        Stream.of(writeable, subInterfaceWriteable, subclassWriteable, cr1, cr2, cr3)
            .forEach(cr -> cr.accept(scanner, ClassReader.SKIP_CODE));

        String className = classNameToPath(Writeable.class.getCanonicalName());
        Set<String> transportClasses = scanner.getConcreteSubclasses(Map.of(className, className));

        assertThat(
            transportClasses,
            Matchers.containsInAnyOrder(
                "org/elasticsearch/gradle/internal/precommit/transport/test_classes/ExampleImpl",
                "org/elasticsearch/gradle/internal/precommit/transport/test_classes/ExampleSubclass",
                "org/elasticsearch/gradle/internal/precommit/transport/test_classes/ExampleSubInterface"
            )
        );
    }

    @Test
    public void testFindConcreteClasses() throws IOException {
        // tests should be concrete instances
        ClassHierarchyScanner scanner = new ClassHierarchyScanner();

        ClassReader writeable = new ClassReader(Writeable.class.getCanonicalName());
        ClassReader subInterfaceWriteable = new ClassReader(WriteableSubInterface.class.getCanonicalName());
        ClassReader subclassWriteable = new ClassReader(WriteableSubClass.class.getCanonicalName());

        ClassReader cr3 = new ClassReader(ExampleImpl.class.getCanonicalName());

        Stream.of(writeable, subInterfaceWriteable, subclassWriteable, cr3).forEach(cr -> cr.accept(scanner, ClassReader.SKIP_CODE));

        String className = classNameToPath(Writeable.class.getCanonicalName());
        Set<String> transportClasses = scanner.getConcreteSubclasses(Map.of(className, className));

        assertThat(
            transportClasses,
            Matchers.containsInAnyOrder("org/elasticsearch/gradle/internal/precommit/transport/test_classes/ExampleImpl")
        );
    }

    @Test
    public void testFindAllSubclassesIncludingRoot() throws IOException {
        // tests should be concrete instances
        ClassHierarchyScanner interfaceScanner = new ClassHierarchyScanner();

        ClassReader writeable = new ClassReader(Writeable.class.getCanonicalName());
        ClassReader subInterfaceWriteable = new ClassReader(WriteableSubInterface.class.getCanonicalName());

        Stream.of(writeable, subInterfaceWriteable).forEach(cr -> cr.accept(interfaceScanner, ClassReader.SKIP_CODE));

        String className = classNameToPath(Writeable.class.getCanonicalName());
        Map<String, String> allSubclasses = interfaceScanner.allFoundSubclasses(Map.of(className, className));

        ClassHierarchyScanner scanner = new ClassHierarchyScanner();

        ClassReader cr3 = new ClassReader(ExampleImpl.class.getCanonicalName());
        Stream.of(cr3).forEach(cr -> cr.accept(scanner, ClassReader.SKIP_CODE));

        Set<String> transportClasses = scanner.getConcreteSubclasses(allSubclasses);

        assertThat(
            transportClasses,
            Matchers.containsInAnyOrder("org/elasticsearch/gradle/internal/precommit/transport/test_classes/ExampleImpl")
        );
    }
}
