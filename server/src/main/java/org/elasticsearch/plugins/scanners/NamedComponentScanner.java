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
import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.Opcodes;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class NamedComponentScanner {

    public Map<String, NameToPluginInfo> findNamedComponents(PluginBundle bundle, ClassLoader pluginClassLoader) {
        return findNamedComponents(ClassReaders.ofBundle(bundle), pluginClassLoader);
    }

    // scope for testing
    Map<String, NameToPluginInfo> findNamedComponents(Stream<ClassReader> classReaderStream, ClassLoader pluginClassLoader) {
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

        ClassScanner localExtensible = new ClassScanner(ExtensiblesRegistry.INSTANCE);
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
}
