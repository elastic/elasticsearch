/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.dependencies.patches.awsv2sdk;

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
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;

import java.io.File;
import java.util.Map;
import java.util.function.Function;

import static java.util.Map.entry;

@CacheableTransform
public abstract class Awsv2ClassPatcher implements TransformAction<TransformParameters.None> {

    private static final String JAR_FILE_TO_PATCH = "aws-query-protocol";

    private static final Map<String, Function<ClassWriter, ClassVisitor>> CLASS_PATCHERS = Map.ofEntries(
        entry("software/amazon/awssdk/protocols/query/internal/marshall/ListQueryMarshaller.class", StringFormatInPathResolverPatcher::new)
    );

    @Classpath
    @InputArtifact
    public abstract Provider<FileSystemLocation> getInputArtifact();

    @Override
    public void transform(@NotNull TransformOutputs outputs) {
        File inputFile = getInputArtifact().get().getAsFile();

        if (inputFile.getName().startsWith(JAR_FILE_TO_PATCH) == false) {
            System.out.println("Skipping " + inputFile.getName());
            outputs.file(getInputArtifact());
        } else {
            System.out.println("Patching " + inputFile.getName());
            File outputFile = outputs.file(inputFile.getName().replace(".jar", "-patched.jar"));
            Utils.patchJar(inputFile, outputFile, CLASS_PATCHERS);
        }
    }
}
