/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.dependencies.patches.awsv2sdk;

import org.elasticsearch.gradle.internal.dependencies.patches.PatcherInfo;
import org.elasticsearch.gradle.internal.dependencies.patches.Utils;
import org.gradle.api.artifacts.transform.CacheableTransform;
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

import static org.elasticsearch.gradle.internal.dependencies.patches.PatcherInfo.classPatcher;

@CacheableTransform
public abstract class Awsv2ClassPatcher implements TransformAction<TransformParameters.None> {

    private static final String JAR_FILE_TO_PATCH = "aws-query-protocol";

    private static final List<PatcherInfo> CLASS_PATCHERS = List.of(
        // This patcher is needed because of this AWS bug: https://github.com/aws/aws-sdk-java-v2/issues/5968
        // As soon as the bug is resolved and we upgrade our AWS SDK v2 libraries, we can remove this.
        classPatcher(
            "software/amazon/awssdk/protocols/query/internal/marshall/ListQueryMarshaller.class",
            "213e84d9a745bdae4b844334d17aecdd6499b36df32aa73f82dc114b35043009",
            StringFormatInPathResolverPatcher::new
        )
    );

    @Classpath
    @InputArtifact
    public abstract Provider<FileSystemLocation> getInputArtifact();

    @Override
    public void transform(@NotNull TransformOutputs outputs) {
        File inputFile = getInputArtifact().get().getAsFile();

        if (inputFile.getName().startsWith(JAR_FILE_TO_PATCH)) {
            System.out.println("Patching " + inputFile.getName());
            File outputFile = outputs.file(inputFile.getName().replace(".jar", "-patched.jar"));
            Utils.patchJar(inputFile, outputFile, CLASS_PATCHERS);
        } else {
            System.out.println("Skipping " + inputFile.getName());
            outputs.file(getInputArtifact());
        }
    }
}
