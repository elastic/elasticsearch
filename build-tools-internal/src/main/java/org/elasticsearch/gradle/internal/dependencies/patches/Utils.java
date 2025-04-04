/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.dependencies.patches;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;

import static org.objectweb.asm.ClassWriter.COMPUTE_FRAMES;
import static org.objectweb.asm.ClassWriter.COMPUTE_MAXS;

public class Utils {
    public static void patchJar(File inputFile, File outputFile, Map<String, Function<ClassWriter, ClassVisitor>> patchers) {
        var classPatchers = new HashMap<>(patchers);
        try (JarFile jarFile = new JarFile(inputFile); JarOutputStream jos = new JarOutputStream(new FileOutputStream(outputFile))) {
            Enumeration<JarEntry> entries = jarFile.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                String entryName = entry.getName();
                // Add the entry to the new JAR file
                jos.putNextEntry(new JarEntry(entryName));

                Function<ClassWriter, ClassVisitor> classPatcher = classPatchers.remove(entryName);
                if (classPatcher != null) {
                    byte[] classToPatch = jarFile.getInputStream(entry).readAllBytes();

                    ClassReader classReader = new ClassReader(classToPatch);
                    ClassWriter classWriter = new ClassWriter(classReader, COMPUTE_MAXS | COMPUTE_FRAMES);
                    classReader.accept(classPatcher.apply(classWriter), 0);
                    jos.write(classWriter.toByteArray());
                } else {
                    // Read the entry's data and write it to the new JAR
                    try (InputStream is = jarFile.getInputStream(entry)) {
                        is.transferTo(jos);
                    }
                }
                jos.closeEntry();
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        if (classPatchers.isEmpty() == false) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "error patching [%s]: the jar does not contain [%s]",
                    inputFile.getName(),
                    String.join(", ", patchers.keySet())
                )
            );
        }
    }
}
