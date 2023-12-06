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

import javax.inject.Inject;

public class FreezeVersionTask extends DefaultTask {
    private static final Logger LOGGER = Logging.getLogger(FreezeVersionTask.class);

    static final String SERVER_PATH = "server/src/main/java/";
    static final String VERSION_PATH = SERVER_PATH + "org/elasticsearch/Version.java";

    private final Path rootDir;

    private Version nextVersion;
    private String luceneVersion;

    @Inject
    public FreezeVersionTask(Project project) {
        rootDir = project.getRootDir().toPath();
    }

    @Option(option = "next-version", description = "Specifies the next version after the release version")
    public void nextVersion(String nextVersion) {
        this.nextVersion = Version.fromString(nextVersion);
    }

    public void luceneVersion(String luceneVersion) {
        this.luceneVersion = luceneVersion;
    }

    @TaskAction
    public void executeTask() throws IOException {
        if (nextVersion == null) {
            throw new IllegalStateException("next-version has not been specified");
        }

        Path versionJava = rootDir.resolve(VERSION_PATH);
        LOGGER.lifecycle("Updating [{}]", versionJava);
        CompilationUnit file = LexicalPreservingPrinter.setup(StaticJavaParser.parse(versionJava));
        CompilationUnit newFile = updateVersionJava(file, nextVersion);
        writeOutNewContents(versionJava, newFile);
    }

    @VisibleForTesting
    static CompilationUnit updateVersionJava(CompilationUnit versionJava, Version nextVersion) {
        String newFieldName = String.format("V_%d_%d_%d", nextVersion.getMajor(), nextVersion.getMinor(), nextVersion.getRevision());

        ClassOrInterfaceDeclaration version = versionJava.getClassByName("Version").get();
        if (version.getFieldByName(newFieldName).isPresent()) {
            LOGGER.lifecycle("Version.java already contains constant {}, skipping", newFieldName);
            return versionJava;
        }

        String newFieldConstant = String.format(
            "%d_%02d_%02d_99",
            nextVersion.getMajor(),
            nextVersion.getMinor(),
            nextVersion.getRevision()
        );

        for (int i = 0; i < version.getMembers().size(); i++) {
            var member = version.getMember(i);
            if (member.isFieldDeclaration() && member.asFieldDeclaration().getVariable(0).getName().getIdentifier().equals("CURRENT")) {
                // found CURRENT - copy the previous variable
                var current = member.asFieldDeclaration();
                // copy the last version field constant
                var lastVersion = version.getMember(i - 1).asFieldDeclaration();
                FieldDeclaration newVersion = createNewConstant(lastVersion, newFieldName, newFieldConstant);

                // add a new field
                version.getMembers().addAfter(newVersion, lastVersion);

                // and update CURRENT to point to it
                current.getVariable(0).setInitializer(new NameExpr(newFieldName));
                return versionJava;
            }
        }

        throw new IllegalArgumentException("Could not find CURRENT constant in Version.java");
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
