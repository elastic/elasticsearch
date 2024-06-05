/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.scanner;

import org.elasticsearch.plugin.Extensible;
import org.elasticsearch.plugin.NamedComponent;
import org.elasticsearch.plugin.scanner.test_model.ExtensibleClass;
import org.elasticsearch.plugin.scanner.test_model.ExtensibleInterface;
import org.elasticsearch.plugin.scanner.test_model.ImplementingExtensible;
import org.elasticsearch.plugin.scanner.test_model.SubClass;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.Type;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class AnnotatedHierarchyVisitorTests extends ESTestCase {
    Set<String> foundClasses;
    AnnotatedHierarchyVisitor visitor;

    @Before
    public void init() {
        foundClasses = new HashSet<>();
        visitor = new AnnotatedHierarchyVisitor(Type.getDescriptor(Extensible.class), (className) -> {
            foundClasses.add(className);
            return null;
        });
    }

    public void testNoClassesAnnotated() throws IOException, URISyntaxException {
        performScan(visitor, NamedComponent.class);

        assertTrue(foundClasses.isEmpty());
    }

    public void testSingleAnnotatedClass() throws IOException, URISyntaxException {
        performScan(visitor, ExtensibleClass.class);

        assertThat(foundClasses, equalTo(Set.of(classNameToPath(ExtensibleClass.class))));
    }

    public void testSubClassofExtensible() throws IOException, URISyntaxException {
        performScan(visitor, ExtensibleClass.class, SubClass.class);

        assertThat(foundClasses, equalTo(Set.of(classNameToPath(ExtensibleClass.class))));
        assertThat(
            visitor.getClassHierarchy(),
            equalTo(Map.of(classNameToPath(ExtensibleClass.class), Set.of(classNameToPath(SubClass.class))))
        );
    }

    public void testSubInterfaceOfExtensible() throws IOException, URISyntaxException {
        performScan(visitor, ImplementingExtensible.class, ExtensibleInterface.class);

        assertThat(foundClasses, equalTo(Set.of(classNameToPath(ExtensibleInterface.class))));
        assertThat(
            visitor.getClassHierarchy(),
            equalTo(Map.of(classNameToPath(ExtensibleInterface.class), Set.of(classNameToPath(ImplementingExtensible.class))))
        );
    }

    private String classNameToPath(Class<?> clazz) {
        return clazz.getCanonicalName().replace(".", "/");
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
