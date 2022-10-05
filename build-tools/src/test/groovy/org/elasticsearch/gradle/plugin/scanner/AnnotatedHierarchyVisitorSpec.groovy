/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1 you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.plugin.scanner

import spock.lang.Specification

import org.elasticsearch.plugin.api.NamedComponent
import org.elasticsearch.plugin.scanner.test_classes.ExtensibleClass
import org.elasticsearch.plugin.scanner.test_classes.ExtensibleInterface
import org.elasticsearch.plugin.scanner.test_classes.ImplementingExtensible
import org.elasticsearch.plugin.scanner.test_classes.SubClass
import org.elasticsearch.plugin.api.Extensible
import org.objectweb.asm.ClassReader
import org.objectweb.asm.Type

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

class AnnotatedHierarchyVisitorSpec extends Specification {
    Set<String> foundClasses
    AnnotatedHierarchyVisitor visitor

    def setup() {
        foundClasses = new HashSet<>()
        visitor =
            new AnnotatedHierarchyVisitor(
                Type.getDescriptor(Extensible.class), (className) -> {
                foundClasses.add(className)
                return null
            }
            )
    }

    def "empty result when no classes annotated"() {
        when:
        performScan(visitor, NamedComponent.class)

        then:
        foundClasses.empty
    }

    def "single class found when only one is annotated"() {
        when:
        performScan(visitor, ExtensibleClass.class)

        then:
        foundClasses == [classNameToPath(ExtensibleClass.class)] as Set
    }

    def "class extending an extensible is also found"() {
        when:
        performScan(visitor, ExtensibleClass.class, SubClass.class)

        then:
        foundClasses == [classNameToPath(ExtensibleClass.class)] as Set
        visitor.getClassHierarchy() == [(classNameToPath(ExtensibleClass.class)) : [classNameToPath(SubClass.class)] as Set]
    }

    def "interface extending an extensible is also found"() {
        when:
        performScan(visitor, ImplementingExtensible.class, ExtensibleInterface.class)

        then:
        foundClasses == [classNameToPath(ExtensibleInterface.class)] as Set
        visitor.getClassHierarchy() ==
            [(classNameToPath(ExtensibleInterface.class)) : [classNameToPath(ImplementingExtensible.class)] as Set]
    }

    private String classNameToPath(Class<?> clazz) {
        return clazz.getCanonicalName().replace(".", "/")
    }

    private void performScan(AnnotatedHierarchyVisitor classVisitor, Class<?>... classes) throws IOException, URISyntaxException {
        for (Class<?> clazz : classes) {
            String className = classNameToPath(clazz) + ".class"
            def stream = this.getClass().getClassLoader().getResourceAsStream(className)
            try (InputStream fileInputStream = stream) {
                ClassReader cr = new ClassReader(fileInputStream)
                cr.accept(classVisitor, 0)
            }
        }
    }

}
