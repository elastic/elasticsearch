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
import com.github.javaparser.ast.NodeList;
import com.github.javaparser.ast.body.ClassOrInterfaceDeclaration;
import com.github.javaparser.ast.body.FieldDeclaration;
import com.github.javaparser.ast.body.VariableDeclarator;
import com.github.javaparser.ast.expr.NameExpr;
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
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.inject.Inject;

public class AddVersionTask extends AbstractVersionTask {
    private static final Logger LOGGER = Logging.getLogger(AddVersionTask.class);

    private Version version;
    private boolean setCurrent;

    @Inject
    public AddVersionTask(BuildLayout layout) {
        super(layout);
    }

    @Option(option = "version", description = "Specifies the version to add")
    public void version(String version) {
        this.version = Version.fromString(version);
    }

    @Option(option = "update-current", description = "Update the 'current' constant to the new version")
    public void setCurrent(boolean setCurrent) {
        this.setCurrent = setCurrent;
    }

    @TaskAction
    public void executeTask() throws IOException {
        if (version == null) {
            throw new IllegalArgumentException("version has not been specified");
        }

        Path versionJava = rootDir.resolve(VERSION_FILE_PATH);
        LOGGER.lifecycle("Adding new version [{}] to [{}]", version, versionJava);
        CompilationUnit file = LexicalPreservingPrinter.setup(StaticJavaParser.parse(versionJava));
        var newFile = addVersionConstant(file, version, setCurrent);
        if (newFile.isPresent()) {
            writeOutNewContents(versionJava, newFile.get());
        }
    }

    private static final Pattern VERSION_FIELD = Pattern.compile("V_(\\d+)_(\\d+)_(\\d+)(?:_(\\w+))?");

    @VisibleForTesting
    static Optional<CompilationUnit> addVersionConstant(CompilationUnit versionJava, Version version, boolean updateCurrent) {
        String newFieldName = String.format("V_%d_%d_%d", version.getMajor(), version.getMinor(), version.getRevision());

        ClassOrInterfaceDeclaration versionClass = versionJava.getClassByName("Version").get();
        if (versionClass.getFieldByName(newFieldName).isPresent()) {
            LOGGER.lifecycle("New version constant [{}] already present, skipping", newFieldName);
            return Optional.empty();
        }

        NavigableMap<Version, FieldDeclaration> versions = versionClass.getFields()
            .stream()
            .map(f -> Map.entry(f, VERSION_FIELD.matcher(f.getVariable(0).getNameAsString())))
            .filter(e -> e.getValue().find())
            .collect(
                Collectors.toMap(
                    e -> new Version(
                        Integer.parseInt(e.getValue().group(1)),
                        Integer.parseInt(e.getValue().group(2)),
                        Integer.parseInt(e.getValue().group(3)),
                        e.getValue().group(4)
                    ),
                    Map.Entry::getKey,
                    (v1, v2) -> {
                        throw new IllegalArgumentException("Duplicate version constants");
                    },
                    TreeMap::new
                )
            );

        // find the version this should be inserted after
        var previousVersion = versions.lowerEntry(version);
        if (previousVersion == null) {
            throw new IllegalStateException(String.format("Could not find previous version to [%s]", version));
        }
        FieldDeclaration newVersion = createNewVersionConstant(
            previousVersion.getValue(),
            newFieldName,
            String.format("%d_%02d_%02d_99", version.getMajor(), version.getMinor(), version.getRevision())
        );
        versionClass.getMembers().addAfter(newVersion, previousVersion.getValue());

        if (updateCurrent) {
            versionClass.getFieldByName("CURRENT")
                .orElseThrow(() -> new IllegalArgumentException("Could not find CURRENT constant"))
                .getVariable(0)
                .setInitializer(new NameExpr(newFieldName));
        }

        return Optional.of(versionJava);
    }

    private static FieldDeclaration createNewVersionConstant(FieldDeclaration lastVersion, String newName, String newExpr) {
        return new FieldDeclaration(
            new NodeList<>(lastVersion.getModifiers()),
            new VariableDeclarator(
                lastVersion.getCommonType(),
                newName,
                StaticJavaParser.parseExpression(String.format("new Version(%s)", newExpr))
            )
        );
    }
}
