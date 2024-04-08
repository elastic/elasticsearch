/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.scanner;

import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Opcodes;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * An ASM class visitor that captures the class hierarchy, as well as finds a specific annotation.
 */
public class AnnotatedHierarchyVisitor extends ClassVisitor {
    private String currentClassName;
    private final String targetAnnotationDescriptor;
    // a function taking the current class name the target annotation appeared on, and returning an AnnotationVisitor
    // that can be used to capture annotation specific args
    private final Function<String, AnnotationVisitor> visitor;
    private final Map<String, Set<String>> classToSubclasses = new HashMap<>();
    private static final String OBJECT_NAME = Object.class.getCanonicalName().replace('.', '/');

    AnnotatedHierarchyVisitor(String targetAnnotation, Function<String, AnnotationVisitor> annotationVisitor) {
        super(Opcodes.ASM9);
        this.targetAnnotationDescriptor = targetAnnotation;
        this.visitor = annotationVisitor;
    }

    @Override
    public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
        currentClassName = name;
        if (OBJECT_NAME.equals(superName) == false) {
            classToSubclasses.computeIfAbsent(superName, k -> new HashSet<>()).add(name);
        }

        for (String iface : interfaces) {
            classToSubclasses.computeIfAbsent(iface, k -> new HashSet<>()).add(name);
        }
    }

    @Override
    public AnnotationVisitor visitAnnotation(String descriptor, boolean visible) {
        if (descriptor.equals(targetAnnotationDescriptor)) {
            return visitor.apply(currentClassName);
        }
        return null;
    }

    /**
     * Returns a mapping of class name to subclasses of that class
     */
    public Map<String, Set<String>> getClassHierarchy() {
        return classToSubclasses;
    }

}
