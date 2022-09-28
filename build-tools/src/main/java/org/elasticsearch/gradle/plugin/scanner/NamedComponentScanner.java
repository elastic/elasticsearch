/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.plugin.scanner;


import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.Opcodes;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class NamedComponentScanner {


    // Map<String, Map<String,String> - extensible interface -> map{ namedName -> className }
    public Map<String, Map<String, String>> scanForNamedClasses(Supplier<Stream<ClassReader>> classReaderStream) {
        ClassScanner extensibleClassScanner = new ClassScanner("Lorg/elasticsearch/plugin/api/Extensible;", (classname, map) -> {
            map.put(classname, classname);
            return null;
        });
        extensibleClassScanner.visit(classReaderStream.get());

        ClassScanner namedComponentsScanner = new ClassScanner(
            "Lorg/elasticsearch/plugin/api/NamedComponent;"/*NamedComponent.class*/,
            (classname, map) -> new AnnotationVisitor(Opcodes.ASM9) {
                @Override
                public void visit(String name, Object value) {
                    assert name.equals("name");
                    assert value instanceof String;
                    map.put(value.toString(), classname);
                }
            }
        );

        namedComponentsScanner.visit(classReaderStream.get());

        Map<String, Map<String, String>> componentInfo = new HashMap<>();
        for (var e : namedComponentsScanner.getFoundClasses().entrySet()) {
            String name = e.getKey();
            String classnameWithSlashes = e.getValue();
            String extensibleClassnameWithSlashes = extensibleClassScanner.getFoundClasses().get(classnameWithSlashes);
            if (extensibleClassnameWithSlashes == null) {
                throw new RuntimeException(
                    "Named component " + name + "(" + pathToClassName(classnameWithSlashes) + ") does not extend from an extensible class"
                );
            }
            var named = componentInfo.computeIfAbsent(pathToClassName(extensibleClassnameWithSlashes), k -> new HashMap<>());
            named.put(name, pathToClassName(classnameWithSlashes));
        }
        return componentInfo;
    }

    private String pathToClassName(String classWithSlashes) {
        return classWithSlashes.replace('/', '.');
    }


}
