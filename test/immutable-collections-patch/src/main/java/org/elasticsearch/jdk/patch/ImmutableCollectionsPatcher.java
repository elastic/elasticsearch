/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.jdk.patch;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.FieldNode;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Loads ImmutableCollections.class from the current jdk and writes it out
 * as a public class with SALT32L and REVERSE as public, non-final static fields.
 *
 * By exposing ImmutableCollections, tests run with this patched version can
 * hook in the existing test seed to ensure consistent iteration of immutable collections.
 * Note that the consistency is for <i>reproducing</i> dependencies on iteration
 * order, so that the code can be fixed.
 */
public class ImmutableCollectionsPatcher {
    private static final String CLASSFILE = "java.base/java/util/ImmutableCollections.class";
    private static final String STABLE_DESC = "Ljdk/internal/vm/annotation/Stable;";

    public static void main(String[] args) throws Exception {
        Path outputDir = Paths.get(args[0]);
        byte[] originalClassFile = Files.readAllBytes(Paths.get(URI.create("jrt:/" + CLASSFILE)));

        ClassReader classReader = new ClassReader(originalClassFile);
        ClassNode classNode = new ClassNode();
        classReader.accept(classNode, 0);
        classNode.access = Opcodes.ACC_PUBLIC;
        for (FieldNode field : classNode.fields) {
            if (field.name.equals("SALT32L") || field.name.equals("REVERSE")) {
                // Since JDK 27, these fields are non-final but @Stable. Modifying fields without removing @Stable
                // causes unpredictable behaviors from inconsistencies by different iterators using different values
                // for SALT32L and REVERSE.
                field.access = Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC;
                if (field.visibleAnnotations != null) {
                    field.visibleAnnotations.removeIf(annotation -> annotation.desc.equals(STABLE_DESC));
                }
            }
        }
        ClassWriter classWriter = new ClassWriter(0);
        classNode.accept(classWriter);

        Path outputFile = outputDir.resolve(CLASSFILE);
        Files.createDirectories(outputFile.getParent());
        Files.write(outputFile, classWriter.toByteArray());
    }
}
