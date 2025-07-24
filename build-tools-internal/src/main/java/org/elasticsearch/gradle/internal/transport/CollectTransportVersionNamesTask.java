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
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.LdcInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This task locates all method invocations of org.elasticsearch.TransportVersion#getName(java.lang.String) in the
 * provided directory, and then records the value of string literals passed as arguments. It then records each
 * string on a newline in the provided output file.
 */
public abstract class CollectTransportVersionNamesTask extends DefaultTask {
    public static final String TRANSPORT_VERSION_SET_CLASS = "org/elasticsearch/TransportVersion";
    public static final String TRANSPORT_VERSION_SET_METHOD_NAME = "fromName";
    public static final String CLASS_EXTENSION = ".class";
    public static final String MODULE_INFO = "module-info.class";

    /**
     * The directory to scan for method invocations.
     */
    @InputFiles
    public abstract Property<FileCollection> getClassDirs();

    /**
     * The output file, with each newline containing the string literal argument of each method
     * invocation.
     */
    @OutputFile
    public abstract RegularFileProperty getOutputFile();

    @TaskAction
    public void checkTransportVersion() {
        var classFiles = findJavaClassFiles(getClassDirs().get().getFiles());
        var tvNames = getTVDeclarationNames(classFiles);

        File file = getOutputFile().get().getAsFile();
        try (FileWriter writer = new FileWriter(file)) {
            for (String tvName : tvNames) {
                writer.write(tvName + "\n");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Set<String> getTVDeclarationNames(Collection<File> classfiles) {
        var results = new HashSet<String>();
        for (File javaFile : classfiles) {
            try (InputStream inputStream = new FileInputStream(javaFile)) {
                ClassVisitor classVisitor = new ClassVisitor(Opcodes.ASM9) {
                    @Override
                    public MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions) {
                        return new MethodNode(Opcodes.ASM9, access, name, descriptor, signature, exceptions) {
                            @Override
                            public void visitMethodInsn(int opcode, String owner, String name, String descriptor, boolean isInterface) {
                                if (owner.equals(TRANSPORT_VERSION_SET_CLASS) && name.equals(TRANSPORT_VERSION_SET_METHOD_NAME)) {
                                    var abstractInstruction = this.instructions.getLast();
                                    if (abstractInstruction instanceof LdcInsnNode ldcInsnNode
                                        && ldcInsnNode.cst instanceof String tvName
                                        && tvName.isEmpty() == false) {
                                        results.add(tvName);
                                    } else {
                                        // The instruction is not a LDC with a String constant (or an empty String),
                                        // which is not allowed.
                                        throw new RuntimeException(
                                            "Transport Versions must be declared with a constant and non-empty String. "
                                                + "file: "
                                                + javaFile.getPath()
                                        );
                                    }
                                }
                                super.visitMethodInsn(opcode, owner, name, descriptor, isInterface);
                            }
                        };
                    }
                };
                ClassReader classReader = new ClassReader(inputStream);
                classReader.accept(classVisitor, 0);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return results;
    }

    private static List<File> findJavaClassFiles(Collection<File> files) {
        List<File> classFiles = new ArrayList<>();
        for (File file : files) {
            if (file.isDirectory()) {
                File[] subFiles = file.listFiles();
                if (subFiles != null) {
                    classFiles.addAll(findJavaClassFiles(Arrays.asList(subFiles)));
                }
            } else if (file.getName().endsWith(CLASS_EXTENSION) && file.getName().endsWith(MODULE_INFO) == false) {
                classFiles.add(file);
            }
        }
        return classFiles;
    }
}
