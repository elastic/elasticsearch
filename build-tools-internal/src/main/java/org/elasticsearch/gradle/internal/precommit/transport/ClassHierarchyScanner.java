/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.precommit.transport;

import org.objectweb.asm.ClassVisitor;

import java.io.File;
import java.lang.reflect.Modifier;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.objectweb.asm.Opcodes.ASM9;

public class ClassHierarchyScanner extends ClassVisitor {
    private static final String OBJECT_NAME = Object.class.getCanonicalName().replace('.', '/');
    private final Map<String, Set<String>> classToSubclasses = new HashMap<>();// parent-child
    private final Map<String, String> innerClasses = new HashMap<>();// inner - enclosing class names
    // transport class cannot not be abstract and must be public
    private final Set<String> concreteClasses = new HashSet<>();

    public ClassHierarchyScanner() {
        super(ASM9);
    }

    private String classNameToPath(String className) {
        return className.replace('.', File.separatorChar);
    }

    @Override
    public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
        super.visit(version, access, name, signature, superName, interfaces);
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

    @Override
    public void visitInnerClass(String name, String outerName, String innerName, int access) {
        super.visitInnerClass(name, outerName, innerName, access);
        // if (name.contains("$") ) {
        // concreteClasses.add(outerName);
        // }
        innerClasses.put(name, outerName);
    }

    /**
     * @param root - map of classname to its superclass. Allows to dfs traverse the class graph
     * @return a set of non abstract, public classes which are traversable from the root.
     */
    public Set<String> getConcreteSubclasses(Map<String, String> root) {
        Set<String> allSubclasses = allFoundSubclasses(root).keySet();
        return allSubclasses.stream()
            .filter(name -> concreteClasses.contains(name))
            // .map(name -> innerClasses.getOrDefault(name, name))
            .collect(Collectors.toSet());
        // allSubclasses.retainAll(concreteClasses);
        // return allSubclasses;
    }

    public Map<String, String> getInnerClasses() {
        return innerClasses;
    }

    public Map<String, String> allFoundSubclasses(Map<String, String> root) {
        return findSubclasses(root);
    }

    // map of class to its superclass/interface
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
                foundClasses.put(subclass, e.getValue());
                toCheckDescendants.addLast(Map.entry(subclass, e.getValue()));
            }
            processed.add(classname);
        }
        return foundClasses;
    }

}
