/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.precommit.transport;

import org.objectweb.asm.ClassVisitor;

import java.lang.reflect.Modifier;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.objectweb.asm.Opcodes.ASM9;

public class ClassHierarchyScanner extends ClassVisitor {
    private final String descriptor;
    private static final String OBJECT_NAME = Object.class.getCanonicalName().replace('.', '/');
    private final Map<String, Set<String>> classToSubclasses = new HashMap<>();// parent-child
    // transport class cannot not be abstract and must be public
    private final Set<String> concreteClasses = new HashSet<>();

    public ClassHierarchyScanner(String classCannonicalName) {
        super(ASM9);
        this.descriptor = classNameToPath(classCannonicalName);
    }

    private String classNameToPath(String className) {
        return className.replace('.', '/');
    }

    @Override
    public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
        if (OBJECT_NAME.equals(superName) == false) {
            classToSubclasses.computeIfAbsent(superName, k -> new HashSet<>()).add(name);
        }

        for (String iface : interfaces) {
            classToSubclasses.computeIfAbsent(iface, k -> new HashSet<>()).add(name);
        }
        if (Modifier.isAbstract(access) == false && Modifier.isPublic(access)) {
            concreteClasses.add(name);
        }
    }

    public Map<String, String> foundClasses() {
        return findSubclasses(Map.of(descriptor, descriptor));
    }

    public Set<String> getSubclasses() {
        return findSubclasses(Map.of(descriptor, descriptor)).keySet();
    }

    public Set<String> subClassesOf(Map<String, String> root) {
        return findSubclasses(root).keySet();
    }

    private Map<String, String> findSubclasses(Map<String, String> root) {
        Deque<Map.Entry<String, String>> toCheckDescendants = new ArrayDeque<>(root.entrySet());
        Set<String> processed = new HashSet<>();
        Map<String, String> foundClasses = new HashMap<>();
        while (toCheckDescendants.isEmpty() == false) {
            var e = toCheckDescendants.removeFirst();
            String classname = e.getKey();
            if (processed.contains(classname)) {
                continue;
            }
            Set<String> subclasses = classToSubclasses.get(classname);
            if (subclasses == null) {
                continue;
            }

            for (String subclass : subclasses) {
                if (concreteClasses.contains(subclass)) {
                    foundClasses.put(subclass, e.getValue());
                }
                toCheckDescendants.addLast(Map.entry(subclass, e.getValue()));
            }
            processed.add(classname);
        }
        return foundClasses;
    }
}
