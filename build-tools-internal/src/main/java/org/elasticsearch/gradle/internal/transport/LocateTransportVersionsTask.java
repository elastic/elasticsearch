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
 * This task locates all method invocations of org.elasticsearch.TransportVersionSet#get(java.lang.String) in the
 * provided directory, and then records the value of string literals passed as arguments. It then records each
 * String on a newline in the provided output file.
 */
public abstract class LocateTransportVersionsTask extends DefaultTask {
    public static final String TRANSPORT_VERSION_SET_CLASS = "org/elasticsearch/TransportVersionSet";
    public static final String TRANSPORT_VERSION_SET_METHOD_NAME = "get";

    /**
     * The directory to scan for TransportVersionSet#get invocations.
     */
    @InputFiles
    public abstract Property<FileCollection> getClassDirs();

    /**
     * The output file, with each newline containing the string literal argument of each TransportVersionSet#get
     * invocation.
     */
    @OutputFile
    public abstract RegularFileProperty getOutputFile();

    @TaskAction
    public void checkTransportVersion() {
        var tvNames = getTVDeclarationNames(getClassDirs().get().getFiles());
        File file = new File(getOutputFile().get().getAsFile().getAbsolutePath());
        try (FileWriter writer = new FileWriter(file)) {
            for (String tvName : tvNames) {
                writer.write(tvName + "\n");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Set<String> getTVDeclarationNames(Collection<File> classDirs) {

        var javaFiles = findJavaFiles(classDirs);

        var results = new HashSet<String>();
        for (File javaFile : javaFiles) {
            // Print the path of each Java file found
            // System.out.println("Found Java file: " + javaFile.getAbsolutePath());
            try (InputStream inputStream = new FileInputStream(javaFile)) {

                ClassVisitor classVisitor = new ClassVisitor(Opcodes.ASM9) {
                    @Override
                    public MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions) {
                        // System.out.println("name: " + name + " descriptor: " + descriptor + " signature: " + signature);

                        return new MethodNode(Opcodes.ASM9, access, name, descriptor, signature, exceptions) {

                            @Override
                            public void visitMethodInsn(int opcode, String owner, String name, String descriptor, boolean isInterface) {
                                if (owner.equals(TRANSPORT_VERSION_SET_CLASS) && name.equals(TRANSPORT_VERSION_SET_METHOD_NAME)) {
                                    // System.out.println("Potato: opcode: " + opcode + " owner: " + owner + " name: " + name);
                                    var abstractInstruction = this.instructions.getLast();
                                    if (abstractInstruction instanceof LdcInsnNode ldcInsnNode) {
                                        if (ldcInsnNode.cst instanceof String tvName && tvName.isEmpty() == false) {
                                            System.out.println("constant: " + tvName);
                                            results.add(tvName);
                                        } else {
                                            System.out.println(
                                                "Transport Versions must be declared with a constant string. "
                                                    + "file: "
                                                    + javaFile.getPath()
                                            );
                                        }
                                    }
                                    System.out.println(
                                        "visitMethodInsn: opcode: "
                                            + opcode
                                            + " owner: "
                                            + owner
                                            + " name: "
                                            + name
                                            + " descriptor: "
                                            + descriptor
                                            + " file: "
                                            + javaFile.getPath()
                                    );
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

    private static List<File> findJavaFiles(Collection<File> files) {
        final String JAVA_EXTENSION = ".class";
        final String MODULE_INFO = "module-info.class";
        List<File> javaFiles = new ArrayList<>();

        for (File file : files) {
            if (file.isDirectory()) {
                File[] subFiles = file.listFiles();
                if (subFiles != null) {
                    javaFiles.addAll(findJavaFiles(Arrays.asList(subFiles)));
                }
            } else if (file.getName().endsWith(JAVA_EXTENSION) && file.getName().endsWith(MODULE_INFO) == false) {
                javaFiles.add(file);
            }
        }
        return javaFiles;
    }

}
