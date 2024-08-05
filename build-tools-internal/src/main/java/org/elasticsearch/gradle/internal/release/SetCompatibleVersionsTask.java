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
import com.github.javaparser.ast.expr.NameExpr;
import com.github.javaparser.printer.lexicalpreservation.LexicalPreservingPrinter;

import org.elasticsearch.gradle.Version;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.options.Option;
import org.gradle.initialization.layout.BuildLayout;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.inject.Inject;

public class SetCompatibleVersionsTask extends AbstractVersionsTask {

    private Version thisVersion;
    private Version releaseVersion;
    private Map<String, Integer> versionIds = Map.of();

    @Inject
    public SetCompatibleVersionsTask(BuildLayout layout) {
        super(layout);
    }

    public void setThisVersion(Version version) {
        thisVersion = version;
    }

    @Option(option = "version-id", description = "Version id used for the release. Of the form <VersionType>:<id>.")
    public void versionIds(List<String> version) {
        this.versionIds = splitVersionIds(version);
    }

    @Option(option = "release", description = "The version being released")
    public void releaseVersion(String version) {
        releaseVersion = Version.fromString(version);
    }

    @TaskAction
    public void executeTask() throws IOException {
        if (versionIds.isEmpty()) {
            throw new IllegalArgumentException("No version ids specified");
        }

        if (releaseVersion.getMajor() < thisVersion.getMajor()) {
            // don't need to update CCS version - this is for a different major
            return;
        }

        Integer transportVersion = versionIds.get(TRANSPORT_VERSION_TYPE);
        if (transportVersion == null) {
            throw new IllegalArgumentException("TransportVersion id not specified");
        }
        Path versionJava = rootDir.resolve(TRANSPORT_VERSIONS_FILE_PATH);
        CompilationUnit file = LexicalPreservingPrinter.setup(StaticJavaParser.parse(versionJava));

        Optional<CompilationUnit> modifiedFile;

        modifiedFile = setMinimumCcsTransportVersion(file, transportVersion);

        if (modifiedFile.isPresent()) {
            writeOutNewContents(versionJava, modifiedFile.get());
        }
    }

    static Optional<CompilationUnit> setMinimumCcsTransportVersion(CompilationUnit unit, int transportVersion) {
        ClassOrInterfaceDeclaration transportVersions = unit.getClassByName("TransportVersions").get();

        String tvConstantName = transportVersions.getFields().stream().filter(f -> {
            var i = findSingleIntegerExpr(f);
            return i.isPresent() && i.getAsInt() == transportVersion;
        })
            .map(f -> f.getVariable(0).getNameAsString())
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("Could not find constant for id " + transportVersion));

        transportVersions.getFieldByName("MINIMUM_CCS_VERSION")
            .orElseThrow(() -> new IllegalStateException("Could not find MINIMUM_CCS_VERSION constant"))
            .getVariable(0)
            .setInitializer(new NameExpr(tvConstantName));

        return Optional.of(unit);
    }
}
