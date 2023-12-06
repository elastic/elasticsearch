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
import org.gradle.api.DefaultTask;
import org.gradle.api.Project;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.options.Option;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.inject.Inject;

public class FinalizeVersionTask extends DefaultTask {
    private static final Logger LOGGER = Logging.getLogger(FinalizeVersionTask.class);

    static final String SERVER_PATH = "server/src/main/java/";
    static final String VERSION_PATH = SERVER_PATH + "org/elasticsearch/Version.java";

    private final Path rootDir;

    private Version releaseVersion;
    private Version nextVersion;
    private String luceneVersion;
    private boolean updateCurrent;
    private boolean removeLastMinorFinalPatch;

    @Inject
    public FinalizeVersionTask(Project project) {
        rootDir = project.getRootDir().toPath();
    }

    public void releaseVersion(Version releaseVersion) {
        this.releaseVersion = releaseVersion;
    }

    @Option(option = "next-version", description = "Specifies the next version after the release version")
    public void nextVersion(String nextVersion) {
        this.nextVersion = Version.fromString(nextVersion);
    }

    public void luceneVersion(String luceneVersion) {
        this.luceneVersion = luceneVersion;
    }

    @Option(option = "update-current", description = "True if CURRENT should be updated")
    public void updateCurrent(boolean update) {
        updateCurrent = update;
    }

    @Option(option = "remove-last-minor-patch", description = "True if the last version of the previous minor should be removed")
    public void removeLastMinorFinalPatch(boolean remove) {
        removeLastMinorFinalPatch = remove;
    }

    @TaskAction
    public void executeTask() throws IOException {
        if (nextVersion == null) {
            throw new IllegalStateException("next-version has not been specified");
        }

        Path versionJava = rootDir.resolve(VERSION_PATH);
        LOGGER.lifecycle("    > Updating [{}]", versionJava);
        CompilationUnit file = LexicalPreservingPrinter.setup(StaticJavaParser.parse(versionJava));
        CompilationUnit newFile = updateVersionJava(file, releaseVersion, nextVersion, updateCurrent, removeLastMinorFinalPatch);
        writeOutNewContents(versionJava, newFile);
    }

    private static final Pattern VERSION_FIELD = Pattern.compile("V_(\\d+)_(\\d+)_(\\d+)(?:_(\\w+))?");

    @VisibleForTesting
    static CompilationUnit updateVersionJava(
        CompilationUnit versionJava,
        Version releaseVersion,
        Version nextVersion,
        boolean updateCurrent,
        boolean removeLastMinorFinalPatch
    ) {
        String newFieldName = String.format("V_%d_%d_%d", nextVersion.getMajor(), nextVersion.getMinor(), nextVersion.getRevision());

        ClassOrInterfaceDeclaration version = versionJava.getClassByName("Version").get();
        if (version.getFieldByName(newFieldName).isPresent()) {
            LOGGER.lifecycle("    > New version constant [{}] already present, skipping", newFieldName);
            return versionJava;
        }

        String newFieldConstant = String.format(
            "%d_%02d_%02d_99",
            nextVersion.getMajor(),
            nextVersion.getMinor(),
            nextVersion.getRevision()
        );

        NavigableMap<Version, FieldDeclaration> versions = version.getFields()
            .stream()
            .map(f -> Map.entry(f, VERSION_FIELD.matcher(f.getVariable(0).getName().getIdentifier())))
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
                        throw new IllegalArgumentException();
                    },
                    TreeMap::new
                )
            );

        if (removeLastMinorFinalPatch) {
            var remove = versions.lowerEntry(releaseVersion);
            remove.getValue().remove();
            versions.remove(remove.getKey());
        }

        // find the version this should be inserted after
        var releaseField = versions.get(releaseVersion);
        FieldDeclaration newVersion = createNewConstant(releaseField, newFieldName, newFieldConstant);
        version.getMembers().addAfter(newVersion, releaseField);

        if (updateCurrent) {
            version.getFieldByName("CURRENT")
                .orElseThrow(() -> new IllegalArgumentException("Could not find CURRENT constant in Version.java"))
                .getVariable(0)
                .setInitializer(new NameExpr(newFieldName));
        }

        return versionJava;
    }

    private static FieldDeclaration createNewConstant(FieldDeclaration lastVersion, String newName, String newExpr) {
        return new FieldDeclaration(
            new NodeList<>(lastVersion.getModifiers()),
            new VariableDeclarator(
                lastVersion.getCommonType(),
                newName,
                StaticJavaParser.parseExpression(String.format("new Version(%s)", newExpr))
            )
        );
    }

    private static void writeOutNewContents(Path file, CompilationUnit unit) throws IOException {
        if (unit.containsData(LexicalPreservingPrinter.NODE_TEXT_DATA) == false) {
            throw new IllegalArgumentException("CompilationUnit has no lexical information for output");
        }
        Files.writeString(file, LexicalPreservingPrinter.print(unit), StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
    }
}
