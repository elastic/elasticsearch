/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.gradle.api.DefaultTask;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.TaskAction;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.InnerClassNode;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.List;
import java.util.function.Consumer;

import static org.objectweb.asm.Opcodes.ACC_PRIVATE;
import static org.objectweb.asm.Opcodes.ACC_PUBLIC;

/**
 * A task to manipulate an existing class file.
 */
public class JavaClassPublicifier extends DefaultTask {

    private List<String> classFiles;
    private DirectoryProperty inputDir;
    private DirectoryProperty outputDir;

    public JavaClassPublicifier() {
        this.inputDir = getProject().getObjects().directoryProperty();
        this.outputDir = getProject().getObjects().directoryProperty();
    }

    @Input
    public List<String> getClassFiles() {
        return classFiles;
    }

    public void setClassFiles(List<String> classFiles) {
        this.classFiles = classFiles;
    }

    @InputDirectory
    public DirectoryProperty getInputDir() {
        return inputDir;
    }

    @OutputDirectory
    public DirectoryProperty getOutputDir() {
        return outputDir;
    }

    @TaskAction
    public void adapt() throws IOException {

        for (String classFile : classFiles) {
            adjustClass(classFile, classNode -> {
                classNode.access &= ~ACC_PRIVATE;
                classNode.access |= ACC_PUBLIC;

                if (classFile.contains("$")) {
                    // java inexplicably has an inner class contain itself as an inner class...
                    makeInnerClassPublic(classNode, classNode.name.split("\\$")[1]);
                }
            });

            if (classFile.contains("$")) {
                // for inner classes, also need to adjust the parent
                String[] parts = classFile.split("\\$");
                String parentClassFile = parts[0] + ".class";
                String innerClass = parts[1].split("\\.")[0];
                adjustClass(parentClassFile, classNode -> makeInnerClassPublic(classNode, innerClass));
            }
        }
    }

    private static void makeInnerClassPublic(ClassNode classNode, String innerClass) {
        InnerClassNode innerClassNode = classNode.innerClasses.stream().filter(node -> node.innerName.equals(innerClass)).findFirst().get();
        innerClassNode.access &= ~ACC_PRIVATE;
        innerClassNode.access |= ACC_PUBLIC;
    }

    private void writeClass(String classFile, ClassNode classNode) throws IOException {
        ClassWriter classWriter = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
        classNode.accept(classWriter);

        File outputFile = outputDir.get().file(classFile).getAsFile();
        outputFile.getParentFile().mkdirs();
        Files.write(outputFile.toPath(), classWriter.toByteArray());
    }

    private void adjustClass(String classFile, Consumer<ClassNode> adjustor) throws IOException {
        try (InputStream is = Files.newInputStream(inputDir.get().file(classFile).getAsFile().toPath())) {
            ClassReader classReader = new ClassReader(is);
            ClassNode classNode = new ClassNode();
            classReader.accept(classNode, ClassReader.EXPAND_FRAMES);
            adjustor.accept(classNode);
            writeClass(classFile, classNode);
        }
    }
}
