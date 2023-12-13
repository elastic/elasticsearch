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

import org.elasticsearch.gradle.Version;
import org.gradle.api.DefaultTask;
import org.gradle.initialization.layout.BuildLayout;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class AbstractVersionTask extends DefaultTask {
    static final String SERVER_MODULE_PATH = "server/src/main/java/";
    static final String VERSION_FILE_PATH = SERVER_MODULE_PATH + "org/elasticsearch/Version.java";

    static final Pattern VERSION_FIELD = Pattern.compile("V_(\\d+)_(\\d+)_(\\d+)(?:_(\\w+))?");

    final Path rootDir;

    protected AbstractVersionTask(BuildLayout layout) {
        rootDir = layout.getRootDirectory().toPath();
    }

    static Optional<Version> parseVersionField(CharSequence field) {
        Matcher m = VERSION_FIELD.matcher(field);
        if (m.find() == false) return Optional.empty();

        return Optional.of(
            new Version(Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2)), Integer.parseInt(m.group(3)), m.group(4))
        );
    }

    static void writeOutNewContents(Path file, CompilationUnit unit) throws IOException {
        if (unit.containsData(LexicalPreservingPrinter.NODE_TEXT_DATA) == false) {
            throw new IllegalArgumentException("CompilationUnit has no lexical information for output");
        }
        Files.writeString(file, LexicalPreservingPrinter.print(unit), StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
    }
}
