/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.scanners;

import org.elasticsearch.plugin.api.NamedComponent;
import org.elasticsearch.plugins.PluginBundle;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.Opcodes;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.elasticsearch.xcontent.XContentType.JSON;

public class NamedComponentScanner {

    private static final String NAMED_COMPONENTS_FILE_NAME = "named_components.json";
    private final ExtensiblesRegistry extensiblesRegistry;

    public NamedComponentScanner() {
        this.extensiblesRegistry = ExtensiblesRegistry.INSTANCE;
    }

    NamedComponentScanner(ExtensiblesRegistry extensiblesRegistry) {
        this.extensiblesRegistry = extensiblesRegistry;
    }

    public Map<String, NameToPluginInfo> findNamedComponents(PluginBundle bundle, ClassLoader pluginClassLoader) {
        return findNamedComponents(ClassReaders.ofBundle(bundle), pluginClassLoader);
    }

    // scope for testing
    Map<String, NameToPluginInfo> findNamedComponents(Stream<ClassReader> classReaderStream, ClassLoader pluginClassLoader) {
//        readFromFile()
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
            named.put(name, new NamedPluginInfo(name, pathToClassName(classnameWithSlashes), pluginClassLoader));
        }
        return componentInfo;
    }

    private String pathToClassName(String classWithSlashes) {
        return classWithSlashes.replace('/', '.');
    }

    @SuppressWarnings("unchecked")
    Map<String, NameToPluginInfo> readFromFile(InputStream resource, ClassLoader pluginClassLoader) throws IOException {
        Map<String, NameToPluginInfo> res = new HashMap<>();

        try (var json = new BufferedInputStream(resource)) {
            try (XContentParser parser = JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
                Map<String, Object> map = parser.map();
                for (Map.Entry<String, Object> fileAsMap : map.entrySet()) {
                    String extensibleInterface = fileAsMap.getKey();

                    Map<String, Object> components = (Map<String, Object>) fileAsMap.getValue();
                    for (Map.Entry<String, Object> nameToComponent : components.entrySet()) {
                        String name = nameToComponent.getKey();
                        String value = (String) nameToComponent.getValue();

                        res.computeIfAbsent(extensibleInterface, k -> new NameToPluginInfo())
                            .put(name, new NamedPluginInfo(name, value, pluginClassLoader));
                    }
                }
            }
        }
        return res;
    }
}
