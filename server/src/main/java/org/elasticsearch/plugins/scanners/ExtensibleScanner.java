/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.scanners;

import org.elasticsearch.plugin.api.Extensible;
import org.objectweb.asm.ClassReader;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class ExtensibleScanner {
    /**
     *
     * @return list of class, interfaces names annotated with @Extensible
     */
    public Map<String, String> getExtensibles() {

        Map<String, String> extensibleClasses = new HashMap<>();
        var visitor = new AnnotatedHierarchyVisitor(Extensible.class, classname -> {
            extensibleClasses.put(classname, classname);
            return null;
        });

        // scan for classes with the extensible annotation, also capturing the class hierarchy
        String classpath = System.getProperty("jdk.module.path");
//        String[] pathelements = classpath.split(":");
        try {
            Stream<Path> list = Files.list(Path.of(classpath));
            list.forEach(p -> {
//            Path p = Paths.get(pathelement);
                if (Files.exists(p) == false) {
                    return;
                }
                try {
                    forEachClass(p, classReader -> {
                        classReader.accept(visitor, ClassReader.SKIP_CODE);
                    });
                } catch (IOException e) {
                    e.printStackTrace();
                }

            });
        } catch (IOException e) {
            e.printStackTrace();
        }
        addExtensibleDescendants(extensibleClasses, visitor.getClassHierarchy());
        return extensibleClasses;
    }


    private static void addExtensibleDescendants(Map<String, String> extensible, Map<String, Set<String>> classToSubclasses) {
        Deque<Map.Entry<String, String>> toCheckDescendants = new ArrayDeque<>(extensible.entrySet());
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
                extensible.put(subclass, e.getValue());
                toCheckDescendants.addLast(Map.entry(subclass, e.getValue()));
            }
            processed.add(classname);
        }
    }
    private static void forEachClassInJar(Path jar, Consumer<ClassReader> classConsumer) throws IOException {
        try (FileSystem jarFs = FileSystems.newFileSystem(jar)) {
            Path root = jarFs.getPath("/");
            forEachClassInPath(root, classConsumer);
        }
    }

    private static void forEachClassInPath(Path root, Consumer<ClassReader> classConsumer) throws IOException {
        try (Stream<Path> stream = Files.walk(root)) {
            stream.filter(p -> p.toString().endsWith(".class")).forEach(p -> {
                try (InputStream is = Files.newInputStream(p)) {
                    byte[] classBytes = is.readAllBytes();
                    ClassReader classReader = new ClassReader(classBytes);
                    classConsumer.accept(classReader);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }
    }

    private static void forEachClass(Path p, Consumer<ClassReader> classConsumer) throws IOException {
        if (p.toString().endsWith(".jar")) {
            forEachClassInJar(p, classConsumer);
        } else {
            forEachClassInPath(p, classConsumer);
        }
    }
}
