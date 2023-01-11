/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.scanner;

import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassReader;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

public class ClassScanner {
    private final Map<String, String> foundClasses;
    private final AnnotatedHierarchyVisitor annotatedHierarchyVisitor;

    public ClassScanner(String targetAnnotation, BiFunction<String, Map<String, String>, AnnotationVisitor> biConsumer) {
        this.foundClasses = new HashMap<>();
        this.annotatedHierarchyVisitor = new AnnotatedHierarchyVisitor(
            targetAnnotation,
            classname -> biConsumer.apply(classname, foundClasses)
        );
    }

    public void visit(List<ClassReader> classReaders) {
        classReaders.forEach(classReader -> classReader.accept(annotatedHierarchyVisitor, ClassReader.SKIP_CODE));
        addExtensibleDescendants(annotatedHierarchyVisitor.getClassHierarchy());
    }

    public void addExtensibleDescendants(Map<String, Set<String>> classToSubclasses) {
        Deque<Map.Entry<String, String>> toCheckDescendants = new ArrayDeque<>(foundClasses.entrySet());
        Set<String> processed = new HashSet<>();
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
    }

    public Map<String, String> getFoundClasses() {
        return foundClasses;
    }

}
