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
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Opcodes;

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

    public static void main(String[] args) throws Exception {
        Path outputDir = Paths.get(args[0]);
        byte[] originalClassFile = Files.readAllBytes(Paths.get(URI.create("jrt:/" + CLASSFILE)));

        ClassReader classReader = new ClassReader(originalClassFile);
        ClassWriter classWriter = new ClassWriter(classReader, 0);
        classReader.accept(new ClassVisitor(Opcodes.ASM9, classWriter) {
            @Override
            public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
                super.visit(version, Opcodes.ACC_PUBLIC, name, signature, superName, interfaces);
            }

            @Override
            public FieldVisitor visitField(int access, String name, String descriptor, String signature, Object value) {
                if (name.equals("SALT32L") || name.equals("REVERSE")) {
                    access = Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC;
                }
                return super.visitField(access, name, descriptor, signature, value);
            }
        }, 0);
        Path outputFile = outputDir.resolve(CLASSFILE);
        Files.createDirectories(outputFile.getParent());
        Files.write(outputFile, classWriter.toByteArray());
    }
}
