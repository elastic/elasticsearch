/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.release;

import com.github.javaparser.StaticJavaParser;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.body.ClassOrInterfaceDeclaration;
import com.github.javaparser.printer.lexicalpreservation.LexicalPreservingPrinter;
import com.google.common.annotations.VisibleForTesting;

import org.elasticsearch.gradle.Version;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.options.Option;
import org.gradle.initialization.layout.BuildLayout;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

import javax.inject.Inject;

public class RemoveVersionTask extends AbstractVersionTask {
    private static final Logger LOGGER = Logging.getLogger(RemoveVersionTask.class);

    private Version version;

    @Inject
    public RemoveVersionTask(BuildLayout layout) {
        super(layout);
    }

    @Option(option = "remove-version", description = "Specifies the version to remove")
    public void version(String version) {
        this.version = Version.fromString(version);
    }

    @TaskAction
    public void executeTask() throws IOException {
        if (version == null) {
            throw new IllegalArgumentException("version has not been specified");
        }

        Path versionJava = rootDir.resolve(VERSION_FILE_PATH);
        LOGGER.lifecycle("Removing version [{}] from [{}]", version, versionJava);
        CompilationUnit file = LexicalPreservingPrinter.setup(StaticJavaParser.parse(versionJava));
        var newFile = removeVersionConstant(file, version);
        if (newFile.isPresent()) {
            writeOutNewContents(versionJava, newFile.get());
        }
    }

    @VisibleForTesting
    static Optional<CompilationUnit> removeVersionConstant(CompilationUnit versionJava, Version version) {
        String removeFieldName = toVersionField(version);

        ClassOrInterfaceDeclaration versionClass = versionJava.getClassByName("Version").get();
        var declaration = versionClass.getFieldByName(removeFieldName);
        if (declaration.isEmpty()) {
            LOGGER.lifecycle("Version constant [{}] not found, skipping", removeFieldName);
            return Optional.empty();
        }

        // check if this is referenced by CURRENT
        String currentReference = versionClass.getFieldByName("CURRENT")
            .orElseThrow(() -> new IllegalArgumentException("Could not find CURRENT constant"))
            .getVariable(0)
            .getInitializer()
            .get()
            .asNameExpr()
            .getNameAsString();
        if (currentReference.equals(removeFieldName)) {
            throw new IllegalArgumentException(String.format("Cannot remove version [%s], it is referenced by CURRENT", version));
        }

        declaration.get().remove();

        return Optional.of(versionJava);
    }
}
