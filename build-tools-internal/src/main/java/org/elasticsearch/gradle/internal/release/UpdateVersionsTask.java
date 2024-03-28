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
import com.github.javaparser.ast.body.FieldDeclaration;
import com.github.javaparser.ast.body.TypeDeclaration;
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
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.inject.Inject;

public class UpdateVersionsTask extends AbstractVersionsTask {

    private static final Logger LOGGER = Logging.getLogger(UpdateVersionsTask.class);

    static final Pattern VERSION_FIELD = Pattern.compile("V_(\\d+)_(\\d+)_(\\d+)(?:_(\\w+))?");

    @Nullable
    private Version addVersion;
    private boolean setCurrent;
    @Nullable
    private Version removeVersion;

    private boolean incrementVersionIds;

    @Inject
    public UpdateVersionsTask(BuildLayout layout) {
        super(layout);
    }

    @Option(option = "add-version", description = "Specifies the version to add")
    public void addVersion(String version) {
        this.addVersion = Version.fromString(version);
    }

    @Option(option = "set-current", description = "Set the 'current' constant to the new version")
    public void setCurrent(boolean setCurrent) {
        this.setCurrent = setCurrent;
    }

    @Option(option = "remove-version", description = "Specifies the version to remove")
    public void removeVersion(String version) {
        this.removeVersion = Version.fromString(version);
    }

    @Option(option = "increment-version-ids", description = "Increment other version ids in the project")
    public void incrementVersionIds(boolean increment) {
        this.incrementVersionIds = true;
    }

    static String toVersionField(Version version) {
        return String.format("V_%d_%d_%d", version.getMajor(), version.getMinor(), version.getRevision());
    }

    static Optional<Version> parseVersionField(CharSequence field) {
        Matcher m = VERSION_FIELD.matcher(field);
        if (m.find() == false) return Optional.empty();

        return Optional.of(
            new Version(Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2)), Integer.parseInt(m.group(3)), m.group(4))
        );
    }

    @TaskAction
    public void executeTask() throws IOException {
        if (addVersion == null && removeVersion == null) {
            throw new IllegalArgumentException("No versions to add or remove specified");
        }
        if (setCurrent && addVersion == null) {
            throw new IllegalArgumentException("No new version added to set as the current version");
        }
        if (Objects.equals(addVersion, removeVersion)) {
            throw new IllegalArgumentException("Same version specified to add and remove");
        }

        updateVersionJava();

        if (incrementVersionIds) {
            Path transportVersionJava = rootDir.resolve(TRANSPORT_VERSIONS_FILE_PATH);
            CompilationUnit tvFile = LexicalPreservingPrinter.setup(StaticJavaParser.parse(transportVersionJava));
            LOGGER.lifecycle("Adding new version constant to [{}]", transportVersionJava);
            var modifiedTv = addVersionId(tvFile, addVersion);
            if (modifiedTv.isPresent()) {
                writeOutNewContents(transportVersionJava, modifiedTv.get());
            }

            Path indexVersionJava = rootDir.resolve(INDEX_VERSIONS_FILE_PATH);
            CompilationUnit ivFile = LexicalPreservingPrinter.setup(StaticJavaParser.parse(indexVersionJava));
            LOGGER.lifecycle("Adding new version constant to [{}]", indexVersionJava);
            var modifiedIv = addVersionId(ivFile, addVersion);
            if (modifiedIv.isPresent()) {
                writeOutNewContents(indexVersionJava, modifiedIv.get());
            }
        }
    }

    private void updateVersionJava() throws IOException {
        Path versionJava = rootDir.resolve(VERSION_FILE_PATH);
        CompilationUnit file = LexicalPreservingPrinter.setup(StaticJavaParser.parse(versionJava));

        Optional<CompilationUnit> modifiedFile = Optional.empty();
        if (addVersion != null) {
            LOGGER.lifecycle("Adding new version [{}] to [{}]", addVersion, versionJava);
            var added = addVersionConstant(modifiedFile.orElse(file), addVersion, setCurrent);
            if (added.isPresent()) {
                modifiedFile = added;
            }
        }
        if (removeVersion != null) {
            LOGGER.lifecycle("Removing version [{}] from [{}]", removeVersion, versionJava);
            var removed = removeVersionConstant(modifiedFile.orElse(file), removeVersion);
            if (removed.isPresent()) {
                modifiedFile = removed;
            }
        }

        if (modifiedFile.isPresent()) {
            writeOutNewContents(versionJava, modifiedFile.get());
        }
    }

    @VisibleForTesting
    static Optional<CompilationUnit> addVersionConstant(CompilationUnit versionJava, Version version, boolean updateCurrent) {
        String newFieldName = toVersionField(version);

        TypeDeclaration<?> versionClass = versionJava.getType(0);
        if (versionClass.getFieldByName(newFieldName).isPresent()) {
            LOGGER.lifecycle("New version constant [{}] already present, skipping", newFieldName);
            return Optional.empty();
        }

        NavigableMap<Version, FieldDeclaration> versions = versionClass.getFields()
            .stream()
            .map(f -> Map.entry(f, parseVersionField(f.getVariable(0).getNameAsString())))
            .filter(e -> e.getValue().isPresent())
            .collect(Collectors.toMap(e -> e.getValue().get(), Map.Entry::getKey, (v1, v2) -> {
                throw new IllegalArgumentException("Duplicate version constants " + v1);
            }, TreeMap::new));

        // find the version this should be inserted after
        var previousVersion = versions.lowerEntry(version);
        if (previousVersion == null) {
            throw new IllegalStateException(String.format("Could not find previous version to [%s]", version));
        }
        FieldDeclaration newVersion = createNewVersionConstant(
            previousVersion.getValue(),
            newFieldName,
            String.format("new Version(%d_%02d_%02d_99)", version.getMajor(), version.getMinor(), version.getRevision())
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
            new VariableDeclarator(lastVersion.getCommonType(), newName, StaticJavaParser.parseExpression(newExpr))
        );
    }

    @VisibleForTesting
    static Optional<CompilationUnit> removeVersionConstant(CompilationUnit versionJava, Version version) {
        String removeFieldName = toVersionField(version);

        TypeDeclaration<?> versionClass = versionJava.getType(0);
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

    private static final int VERSION_ADDITION = 1000;

    @VisibleForTesting
    static Optional<CompilationUnit> addVersionId(CompilationUnit java, Version releaseVersion) {
        TypeDeclaration<?> type = java.getType(0);

        String newVersionField = "RELEASE_" + releaseVersion.toString().replace('.', '_');
        if (type.getFieldByName(newVersionField).isPresent()) {
            LOGGER.lifecycle("New version constant [{}] already present, skipping", newVersionField);
            return Optional.empty();
        }

        // find the most recent version id
        Map.Entry<Integer, FieldDeclaration> versions = type.getFields()
            .stream()
            .map(f -> Map.entry(f, findSingleIntegerExpr(f)))
            .filter(e -> e.getValue().isPresent())
            .map(e -> Map.entry(e.getValue().getAsInt(), e.getKey()))
            .max(Map.Entry.comparingByKey())
            .orElseThrow(() -> new IllegalStateException("Could not find any version constants"));

        int newVersionId = versions.getKey() + VERSION_ADDITION - (versions.getKey() % VERSION_ADDITION);
        // transform the id into the text format M_NNN_SS_P
        StringBuilder versionIdText = new StringBuilder(Integer.toString(newVersionId));
        int len = versionIdText.length();
        versionIdText.insert(len - 1, '_').insert(len - 3, '_').insert(len - 6, '_');

        FieldDeclaration newVersion = createNewVersionConstant(
            versions.getValue(),
            newVersionField,
            String.format("def(%s)", versionIdText)
        );
        type.getMembers().addAfter(newVersion, versions.getValue());

        return Optional.of(java);
    }
}
