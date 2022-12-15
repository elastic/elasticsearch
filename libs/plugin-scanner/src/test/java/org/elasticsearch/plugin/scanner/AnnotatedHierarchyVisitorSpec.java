/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.scanner;

import junit.framework.TestCase;
import spock.lang.Specification;

import org.elasticsearch.plugin.api.NamedComponent;
import org.elasticsearch.plugin.scanner.test_classes.ExtensibleClass;
import org.elasticsearch.plugin.scanner.test_classes.ExtensibleInterface;
import org.elasticsearch.plugin.scanner.test_classes.ImplementingExtensible;
import org.elasticsearch.plugin.scanner.test_classes.SubClass;
import org.elasticsearch.plugin.api.Extensible;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.Type;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class AnnotatedHierarchyVisitorTests extends TestCase {
    Set<String> foundClasses;
    AnnotatedHierarchyVisitor visitor;

    public void init() {
        foundClasses = new HashSet<>();
        visitor =
            new AnnotatedHierarchyVisitor(
                Type.getDescriptor(Extensible.class), (className) -> {
                foundClasses.add(className);
                return null;
            }
            );
    }

    public void testNoClassesAnnotated() {
        performScan(visitor, NamedComponent.class);

        assertTrue(foundClasses.isEmpty());
    }

    public void testSingleAnnotatedClass() {
        performScan(visitor, ExtensibleClass.class);

        assertThat(foundClasses, equalTo(Set.of(classNameToPath(ExtensibleClass.class)));
    }

    public void testSubClassofExtensible() {

        performScan(visitor, ExtensibleClass.class, SubClass.class)

        assertThat(foundClasses, equalTo(Set.of(classNameToPath(ExtensibleClass.class)));
        assertThat(visitor.getClassHierarchy(), equalTo(Map.of(classNameToPath(ExtensibleClass.class), Set.of(classNameToPath(SubClass.class))));
    }

    public void testSubInterfaceOfExtensible() {
        performScan(visitor, ImplementingExtensible.class, ExtensibleInterface.class);

        assertThat(foundClasses, equalTo(Set.of(classNameToPath(ExtensibleInterface.class)));
        assertThat(visitor.getClassHierarchy(), equalTo(Map.of(classNameToPath(ExtensibleClass.class), Set.of(classNameToPath(SubClass.class))));

        foundClasses == [classNameToPath(ExtensibleInterface.class)]as Set
        visitor.getClassHierarchy() ==
            [(classNameToPath(ExtensibleInterface.class)): [classNameToPath(ImplementingExtensible.class)]as Set]
    }

    private String classNameToPath(Class<?> clazz) {
        return clazz.getCanonicalName().replace(".", "/")
    }

    private void performScan(AnnotatedHierarchyVisitor classVisitor, Class<?>... classes) throws IOException, URISyntaxException {
        for (Class<?> clazz : classes) {
            String className = classNameToPath(clazz) + ".class";
            var stream = this.getClass().getClassLoader().getResourceAsStream(className);
            try (InputStream fileInputStream = stream) {
                ClassReader cr = new ClassReader(fileInputStream);
                cr.accept(classVisitor, 0);
            }
        }
    }

}
