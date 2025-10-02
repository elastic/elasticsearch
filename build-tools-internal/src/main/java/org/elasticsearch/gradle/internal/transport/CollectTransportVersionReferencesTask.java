/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.transport;

import org.gradle.api.DefaultTask;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.Classpath;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.LdcInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashSet;
import java.util.Set;

/**
 * This task locates all method invocations of org.elasticsearch.TransportVersion#fromName(java.lang.String) in the
 * provided directory, and then records the value of string literals passed as arguments. It then records each
 * string on a newline along with path and line number in the provided output file.
 */
@CacheableTask
public abstract class CollectTransportVersionReferencesTask extends DefaultTask {
    public static final String TRANSPORT_VERSION_SET_CLASS = "org/elasticsearch/TransportVersion";
    public static final String TRANSPORT_VERSION_SET_METHOD_NAME = "fromName";
    public static final String CLASS_EXTENSION = ".class";
    public static final String MODULE_INFO = "module-info.class";

    /**
     * The directory to scan for method invocations.
     */
    @Classpath
    public abstract ConfigurableFileCollection getClassPath();

    /**
     * The output file, with each newline containing the string literal argument of each method
     * invocation.
     */
    @OutputFile
    public abstract RegularFileProperty getOutputFile();

    @TaskAction
    public void checkTransportVersion() throws IOException {
        var results = new HashSet<TransportVersionReference>();

        for (var cpElement : getClassPath()) {
            Path file = cpElement.toPath();
            if (Files.isDirectory(file)) {
                addNamesFromClassesDirectory(results, file);
            }
        }

        Path outputFile = getOutputFile().get().getAsFile().toPath();
        Files.writeString(outputFile, String.join("\n", results.stream().map(Object::toString).sorted().toList()));
    }

    private void addNamesFromClassesDirectory(Set<TransportVersionReference> results, Path basePath) throws IOException {
        Files.walkFileTree(basePath, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                String filename = file.getFileName().toString();
                if (filename.endsWith(CLASS_EXTENSION) && filename.endsWith(MODULE_INFO) == false) {
                    try (var inputStream = Files.newInputStream(file)) {
                        addNamesFromClass(results, inputStream, classname(basePath.relativize(file).toString()));
                    }
                }
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private void addNamesFromClass(Set<TransportVersionReference> results, InputStream classBytes, String classname) throws IOException {
        ClassVisitor classVisitor = new ClassVisitor(Opcodes.ASM9) {
            @Override
            public MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions) {
                return new MethodNode(Opcodes.ASM9, access, name, descriptor, signature, exceptions) {
                    int lineNumber = -1;

                    @Override
                    public void visitLineNumber(int line, Label start) {
                        lineNumber = line;
                    }

                    @Override
                    public void visitLabel(Label label) {
                        // asm uses many debug labels that we do not want to consider
                        // so we ignore labels so they do not become part of the instructions list
                    }

                    @Override
                    public void visitMethodInsn(int opcode, String owner, String name, String descriptor, boolean isInterface) {
                        if (owner.equals(TRANSPORT_VERSION_SET_CLASS) && name.equals(TRANSPORT_VERSION_SET_METHOD_NAME)) {
                            var abstractInstruction = this.instructions.getLast();
                            String location = classname + " line " + lineNumber;
                            if (abstractInstruction instanceof LdcInsnNode ldcInsnNode
                                && ldcInsnNode.cst instanceof String tvName
                                && tvName.isEmpty() == false) {
                                results.add(new TransportVersionReference(tvName, location));
                            } else {
                                // The instruction is not a LDC with a String constant (or an empty String), which is not allowed.
                                throw new RuntimeException(
                                    "TransportVersion.fromName must be called with a non-empty String literal. " + "See " + location + "."
                                );
                            }
                        }
                        super.visitMethodInsn(opcode, owner, name, descriptor, isInterface);
                    }
                };
            }
        };
        ClassReader classReader = new ClassReader(classBytes);
        classReader.accept(classVisitor, 0);
    }

    private static String classname(String filename) {
        return filename.substring(0, filename.length() - CLASS_EXTENSION.length()).replaceAll("[/\\\\]", ".");
    }
}
