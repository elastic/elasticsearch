/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.scanners;

import org.elasticsearch.core.PathUtils;
import org.elasticsearch.plugin.api.NamedComponent;
import org.elasticsearch.plugins.PluginBundle;
import org.elasticsearch.plugins.PluginsService;
import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.Opcodes;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class NamedComponentScanner {
    public Map<String, NamedPlugins> findNamedComponents(PluginBundle bundle,
                                                           ClassLoader pluginClassLoader,
                                                           Map<String,String> extensibleInterfaces) {
        Map<String, String> namedComponents = new HashMap<>();
        var visitor = new AnnotatedHierarchyVisitor(NamedComponent.class, classname ->
            new AnnotationVisitor(Opcodes.ASM9) {
                @Override
                public void visit(String name, Object value) {
                    assert name.equals("name");
                    assert value instanceof String;
                    namedComponents.put(value.toString(), classname);
                }
            });

        for (Path jar : urlsToPaths(bundle.allUrls)) {
            try {
                forEachClassInJar(jar, classReader -> {
                    classReader.accept(visitor, ClassReader.SKIP_CODE);
                });
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        var localExtensible = new HashMap<>(extensibleInterfaces); // copy extensible so we can add local extensible classes
        addExtensibleDescendants(localExtensible, visitor.getClassHierarchy());

        Map<String, NamedPlugins> componentInfo = new HashMap<>();

        for (var e : namedComponents.entrySet()) {
            String name = e.getKey();
            String classname = e.getValue();
            String extensibleClassname = localExtensible.get(classname);
            if (extensibleClassname == null) {
                throw new RuntimeException("Named component " + name + "(" + classname + ") does not extend from an extensible class");
            }
            var named = componentInfo.computeIfAbsent(extensibleClassname, k -> new NamedPlugins());
            named.put(name, new NamedPluginInfo(name, classname, pluginClassLoader));
        }
        return componentInfo;
    }

    /**
     * Iterate through the existing extensible classes, and add all descendents as extensible.
     *
     * @param extensible A map from class name, to the original class that has the ExtensibleComponent annotation
     */
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
    //from plugin service
    static final Path[] urlsToPaths(Set<URL> urls) {
        return urls.stream().map(PluginsService::uncheckedToURI).map(PathUtils::get).toArray(Path[]::new);
    }
}
