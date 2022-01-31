/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.modules;

import org.gradle.api.DefaultTask;
import org.gradle.api.file.FileCollection;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.SkipWhenEmpty;
import org.gradle.api.tasks.TaskAction;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.ClassNode;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Set;

public class ReadModuleExports extends DefaultTask {

    private FileCollection classFiles;

    Set<String> exports = new HashSet<>();

    @TaskAction
    public void readModuleFile() throws IOException {
        File moduleInfoFile = classFiles.getFiles()
            .stream()
            .map(f -> new File(f, "module-info.class"))
            .filter(f -> f.exists())
            .findFirst()
            .get();

        ClassNode classNode = new ClassNode(Opcodes.ASM8);
        try (InputStream is = Files.newInputStream(moduleInfoFile.toPath())) {
            ClassReader classReader = new ClassReader(is);
            classReader.accept(classNode, 0);
        }
        classNode.module.exports.forEach(n -> {
            System.out.println(n.packaze);
            exports.add(n.packaze);
        });
    }

    @Internal
    public Set<String> getExports() {
        return exports;
    }

    public void setClassFiles(FileCollection classFiles) {
        this.classFiles = classFiles;
    }

    @InputFiles
    @SkipWhenEmpty
    public FileCollection getModuleClassFile() {
        return classFiles;
    }
}
