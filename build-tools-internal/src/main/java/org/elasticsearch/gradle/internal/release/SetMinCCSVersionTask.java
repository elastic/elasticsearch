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
import com.github.javaparser.ast.body.FieldDeclaration;
import com.github.javaparser.ast.expr.IntegerLiteralExpr;
import com.github.javaparser.printer.lexicalpreservation.LexicalPreservingPrinter;

import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.options.Option;
import org.gradle.initialization.layout.BuildLayout;

import java.io.IOException;
import java.nio.file.Path;

import javax.inject.Inject;

public class SetMinCCSVersionTask extends AbstractVersionsTask {

    private static final String MINIMUM_CCS_VERSION_FIELD = "MINIMUM_CCS_VERSION";

    private int versionToSet = -1;

    @Inject
    public SetMinCCSVersionTask(BuildLayout layout) {
        super(layout);
    }

    @Option(option = "min-ccs-version", description = "The version id to set as the minimum CCS version")
    public void minCcsVersion(String versionId) {
        versionToSet = Integer.parseInt(versionId);
    }

    @TaskAction
    public void executeTask() throws IOException {
        if (versionToSet == -1) {
            throw new IllegalArgumentException("Version not specified");
        }

        Path tvJava = rootDir.resolve(TRANSPORT_VERSION_FILE_PATH);
        CompilationUnit file = LexicalPreservingPrinter.setup(StaticJavaParser.parse(tvJava));

        setMinCcsVersionField(file, versionToSet);

        writeOutNewContents(tvJava, file);
    }

    static void setMinCcsVersionField(CompilationUnit file, int version) {
        ClassOrInterfaceDeclaration tvs = file.getClassByName("TransportVersions").get();

        FieldDeclaration minCcsField = tvs.getFieldByName(MINIMUM_CCS_VERSION_FIELD)
            .orElseThrow(() -> new IllegalArgumentException("Could not find " + MINIMUM_CCS_VERSION_FIELD + " constant"));

        if (minCcsField.getVariable(0).getInitializer().get().isNameExpr() == false) {
            throw new IllegalArgumentException(MINIMUM_CCS_VERSION_FIELD + " initializer is not a single field, cannot update");
        }

        var versionField = tvs.findFirst(FieldDeclaration.class, f -> {
            var ints = f.findAll(IntegerLiteralExpr.class);
            return ints.size() == 1 && ints.get(0).asNumber().intValue() == version;
        }).orElseThrow(() -> new IllegalArgumentException("Could not find constant for version id " + version));

        minCcsField.getVariable(0).setInitializer(versionField.getVariable(0).getNameAsExpression());
    }
}
