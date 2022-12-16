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
import org.objectweb.asm.Opcodes;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class NamedComponentScanner {
    // private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    // main method to be used by gradle build plugin
    public static void main(String[] args) throws IOException {
        List<ClassReader> classReaders = ClassReaders.ofClassPath().collect(Collectors.toList());

        NamedComponentScanner scanner = new NamedComponentScanner();
        Map<String, Map<String, String>> namedComponentsMap = scanner.scanForNamedClasses(classReaders);
        scanner.writeToFile(namedComponentsMap,args[0]);
    }

    private void writeToFile(Map<String, Map<String, String>> namedComponentsMap, String arg) {
         try {
        // String json = OBJECT_MAPPER.writeValueAsString(namedComponentsMap);
         File file = new File(arg);
         Path of = Path.of(file.getAbsolutePath());
         Files.writeString(of, namedComponentsMap.toString());
         } catch (Exception e) {
         e.printStackTrace();
         }
    }

    // returns a Map<String, Map<String,String> - extensible interface -> map{ namedName -> className }
    public Map<String, Map<String, String>> scanForNamedClasses(Collection<ClassReader> classReaderStream) {
        // TODO I don't have access to stable-plugin-api here so I have to hardcode class descriptors
        ClassScanner extensibleClassScanner = new ClassScanner("Lorg/elasticsearch/plugin/api/Extensible;", (classname, map) -> {
            map.put(classname, classname);
            return null;
        });
        extensibleClassScanner.visit(classReaderStream.stream());

        ClassScanner namedComponentsScanner = new ClassScanner(
            "Lorg/elasticsearch/plugin/api/NamedComponent;"/*NamedComponent.class*/,
            (classname, map) -> new AnnotationVisitor(Opcodes.ASM9) {
                @Override
                public void visit(String key, Object value) {
                    assert key.equals("value");
                    assert value instanceof String;
                    map.put(value.toString(), classname);
                }
            }
        );

        namedComponentsScanner.visit(classReaderStream.stream());

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
