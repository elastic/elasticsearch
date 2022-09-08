/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.scanner.impl;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.elasticsearch.plugin.api.NamedComponent;
import org.elasticsearch.plugin.scanner.NameToPluginInfo;
import org.elasticsearch.plugin.scanner.PluginInfo;
import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.Opcodes;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class NamedComponentScanner {
    ObjectMapper mapper = new ObjectMapper();

    private static final String NAMED_COMPONENTS_FILE_NAME = "named_components.json";
    private final ExtensiblesRegistry extensiblesRegistry;

    NamedComponentScanner(ExtensiblesRegistry extensiblesRegistry) {
        this.extensiblesRegistry = extensiblesRegistry;
    }

    public Map<String, NameToPluginInfo> findNamedComponents(Set<URL> bundle, ClassLoader pluginClassLoader) {
        try (Stream<ClassReader> classReaderStream = ClassReaders.ofBundle(bundle)) {
            return scanForNamedClasses(classReaderStream, pluginClassLoader);
        }
    }

    // scope for testing
    Map<String, NameToPluginInfo> findNamedComponents(Stream<ClassReader> classReaderStream, ClassLoader pluginClassLoader) {
        // readFromFile()
        return scanForNamedClasses(classReaderStream, pluginClassLoader);
    }

    private Map<String, NameToPluginInfo> scanForNamedClasses(Stream<ClassReader> classReaderStream, ClassLoader pluginClassLoader) {
        ClassScanner namedComponentsScanner = new ClassScanner(
            NamedComponent.class,
            (classname, map) -> new AnnotationVisitor(Opcodes.ASM9) {
                @Override
                public void visit(String name, Object value) {
                    assert name.equals("name");
                    assert value instanceof String;
                    map.put(value.toString(), classname);
                }
            }
        );

        namedComponentsScanner.visit(classReaderStream);

        ClassScanner localExtensible = new ClassScanner(extensiblesRegistry.getExtensibleClassScanner());
        localExtensible.addExtensibleDescendants(namedComponentsScanner.getClassHierarchy());

        Map<String, NameToPluginInfo> componentInfo = new HashMap<>();
        for (var e : namedComponentsScanner.getFoundClasses().entrySet()) {
            String name = e.getKey();
            String classnameWithSlashes = e.getValue();
            String extensibleClassnameWithSlashes = localExtensible.getFoundClasses().get(classnameWithSlashes);
            if (extensibleClassnameWithSlashes == null) {
                throw new RuntimeException(
                    "Named component " + name + "(" + pathToClassName(classnameWithSlashes) + ") does not extend from an extensible class"
                );
            }
            var named = componentInfo.computeIfAbsent(pathToClassName(extensibleClassnameWithSlashes), k -> new NameToPluginInfo());
            named.put(name, new PluginInfo(name, pathToClassName(classnameWithSlashes), pluginClassLoader));
        }
        return componentInfo;
    }

    private String pathToClassName(String classWithSlashes) {
        return classWithSlashes.replace('/', '.');
    }

    @SuppressWarnings("unchecked")
    Map<String, NameToPluginInfo> readFromFile(InputStream resource, ClassLoader pluginClassLoader) throws IOException {
        Map<String, NameToPluginInfo> res = new HashMap<>();

        try (var in = new BufferedInputStream(resource)) {

            Map<String, Object> fileAsMap = mapper.readValue(in, Map.class);
            for (Map.Entry<String, Object> extensibleEntry : fileAsMap.entrySet()) {
                String extensibleInterface = extensibleEntry.getKey();
                Map<String, Object> components = (Map<String, Object>) extensibleEntry.getValue();
                for (Map.Entry<String, Object> nameToComponent : components.entrySet()) {
                    String name = nameToComponent.getKey();
                    String value = (String) nameToComponent.getValue();

                    res.computeIfAbsent(extensibleInterface, k -> new NameToPluginInfo())
                        .put(name, new PluginInfo(name, value, pluginClassLoader));
                }
            }
        }
        return res;
    }
}
