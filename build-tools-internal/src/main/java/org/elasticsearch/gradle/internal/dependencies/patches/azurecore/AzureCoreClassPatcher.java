/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.dependencies.patches.azurecore;

import org.elasticsearch.gradle.internal.dependencies.patches.PatcherInfo;
import org.elasticsearch.gradle.internal.dependencies.patches.Utils;
import org.gradle.api.artifacts.transform.InputArtifact;
import org.gradle.api.artifacts.transform.TransformAction;
import org.gradle.api.artifacts.transform.TransformOutputs;
import org.gradle.api.artifacts.transform.TransformParameters;
import org.gradle.api.file.FileSystemLocation;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.Classpath;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.List;
import java.util.regex.Pattern;

import static org.elasticsearch.gradle.internal.dependencies.patches.PatcherInfo.classPatcher;

public abstract class AzureCoreClassPatcher implements TransformAction<TransformParameters.None> {

    private static final String JAR_FILE_TO_PATCH = "azure-core-[\\d.]*\\.jar";

    private static final List<PatcherInfo> CLASS_PATCHERS = List.of(
        classPatcher(
            "com/azure/core/implementation/ImplUtils.class",
            "7beda5bdff5ea460cfc08721a188cf07d16e0c987dae45401fca7abf4e6e6c0e",
            ImplUtilsPatcher::new
        )
    );

    @Classpath
    @InputArtifact
    public abstract Provider<FileSystemLocation> getInputArtifact();

    @Override
    public void transform(@NotNull TransformOutputs outputs) {
        File inputFile = getInputArtifact().get().getAsFile();

        if (Pattern.matches(JAR_FILE_TO_PATCH, inputFile.getName())) {
            System.out.println("Patching " + inputFile.getName());
            File outputFile = outputs.file(inputFile.getName().replace(".jar", "-patched.jar"));
            Utils.patchJar(inputFile, outputFile, CLASS_PATCHERS, true);
        } else {
            System.out.println("Skipping " + inputFile.getName());
            outputs.file(getInputArtifact());
        }
    }

}
