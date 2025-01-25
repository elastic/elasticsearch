/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.hdfs.patch;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.function.Function;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class HdfsClassPatcher {
    static final Map<String, Function<ClassWriter, ClassVisitor>> patchers = Map.of(
        "org/apache/hadoop/util/ShutdownHookManager.class",
        ShutdownHookManagerPatcher::new,
        "org/apache/hadoop/util/Shell.class",
        ShellPatcher::new,
        "org/apache/hadoop/security/UserGroupInformation.class",
        SubjectGetSubjectPatcher::new,
        "org/apache/hadoop/security/authentication/client/KerberosAuthenticator.class",
        SubjectGetSubjectPatcher::new
    );

    public static void main(String[] args) throws Exception {
        String jarPath = args[0];
        Path outputDir = Paths.get(args[1]);

        try (JarFile jarFile = new JarFile(new File(jarPath))) {
            for (var patcher : patchers.entrySet()) {
                JarEntry jarEntry = jarFile.getJarEntry(patcher.getKey());
                if (jarEntry == null) {
                    throw new IllegalArgumentException("path [" + patcher.getKey() + "] not found in [" + jarPath + "]");
                }
                byte[] classToPatch = jarFile.getInputStream(jarEntry).readAllBytes();

                ClassReader classReader = new ClassReader(classToPatch);
                ClassWriter classWriter = new ClassWriter(classReader, 0);
                classReader.accept(patcher.getValue().apply(classWriter), 0);

                Path outputFile = outputDir.resolve(patcher.getKey());
                Files.createDirectories(outputFile.getParent());
                Files.write(outputFile, classWriter.toByteArray());
            }
        }
    }
}
