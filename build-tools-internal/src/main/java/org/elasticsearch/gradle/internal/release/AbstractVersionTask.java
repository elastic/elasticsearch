/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.release;

import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.printer.lexicalpreservation.LexicalPreservingPrinter;

import org.gradle.api.DefaultTask;
import org.gradle.initialization.layout.BuildLayout;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public abstract class AbstractVersionTask extends DefaultTask {
    static final String SERVER_MODULE_PATH = "server/src/main/java/";
    static final String VERSION_FILE_PATH = SERVER_MODULE_PATH + "org/elasticsearch/Version.java";

    final Path rootDir;

    protected AbstractVersionTask(BuildLayout layout) {
        rootDir = layout.getRootDirectory().toPath();
    }

    static void writeOutNewContents(Path file, CompilationUnit unit) throws IOException {
        if (unit.containsData(LexicalPreservingPrinter.NODE_TEXT_DATA) == false) {
            throw new IllegalArgumentException("CompilationUnit has no lexical information for output");
        }
        Files.writeString(file, LexicalPreservingPrinter.print(unit), StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
    }
}
