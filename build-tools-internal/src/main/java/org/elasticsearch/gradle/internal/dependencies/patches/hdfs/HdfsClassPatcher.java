/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.dependencies.patches.hdfs;

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
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.Optional;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.List;
import java.util.regex.Pattern;

import static org.elasticsearch.gradle.internal.dependencies.patches.PatcherInfo.classPatcher;

@CacheableTransform
public abstract class HdfsClassPatcher implements TransformAction<HdfsClassPatcher.Parameters> {

    record JarPatchers(String artifactTag, Pattern artifactPattern, List<PatcherInfo> jarPatchers) {}

    static final List<JarPatchers> allPatchers = List.of(
        new JarPatchers(
            "hadoop-common",
            Pattern.compile("hadoop-common-(?!.*tests)"),
            List.of(
                classPatcher(
                    "org/apache/hadoop/util/ShutdownHookManager.class",
                    "90641e0726fc9372479728ef9b7ae2be20fb7ab4cddd4938e55ffecadddd4d94",
                    ShutdownHookManagerPatcher::new
                ),
                classPatcher(
                    "org/apache/hadoop/util/Shell.class",
                    "8837c7f3eeda3f658fc3d6595f18e77a4558220ff0becdf3e175fa4397a6fd0c",
                    ShellPatcher::new
                ),
                classPatcher(
                    "org/apache/hadoop/security/UserGroupInformation.class",
                    "3c34bbc2716a6c8f4e356e78550599b0a4f01882712b4f7787d032fb10527212",
                    SubjectGetSubjectPatcher::new
                )
            )
        ),
        new JarPatchers(
            "hadoop-client-api",
            Pattern.compile("hadoop-client-api.*"),
            List.of(
                classPatcher(
                    "org/apache/hadoop/util/ShutdownHookManager.class",
                    "90641e0726fc9372479728ef9b7ae2be20fb7ab4cddd4938e55ffecadddd4d94",
                    ShutdownHookManagerPatcher::new
                ),
                classPatcher(
                    "org/apache/hadoop/util/Shell.class",
                    "8837c7f3eeda3f658fc3d6595f18e77a4558220ff0becdf3e175fa4397a6fd0c",
                    ShellPatcher::new
                ),
                classPatcher(
                    "org/apache/hadoop/security/UserGroupInformation.class",
                    "3c34bbc2716a6c8f4e356e78550599b0a4f01882712b4f7787d032fb10527212",
                    SubjectGetSubjectPatcher::new
                ),
                classPatcher(
                    "org/apache/hadoop/security/authentication/client/KerberosAuthenticator.class",
                    "6bab26c1032a38621c20050ec92067226d1d67972d0d370e412ca25f1df96b76",
                    SubjectGetSubjectPatcher::new
                )
            )
        )
    );

    interface Parameters extends TransformParameters {
        @Input
        @Optional
        List<String> getMatchingArtifacts();

        void setMatchingArtifacts(List<String> matchingArtifacts);
    }

    @Classpath
    @InputArtifact
    public abstract Provider<FileSystemLocation> getInputArtifact();

    @Override
    public void transform(@NotNull TransformOutputs outputs) {
        File inputFile = getInputArtifact().get().getAsFile();

        List<String> matchingArtifacts = getParameters().getMatchingArtifacts();
        List<JarPatchers> patchersToApply = allPatchers.stream()
            .filter(jp -> matchingArtifacts.contains(jp.artifactTag()) && jp.artifactPattern().matcher(inputFile.getName()).find())
            .toList();
        if (patchersToApply.isEmpty()) {
            outputs.file(getInputArtifact());
        } else {
            patchersToApply.forEach(patchers -> {
                System.out.println("Patching " + inputFile.getName());
                File outputFile = outputs.file(inputFile.getName().replace(".jar", "-patched.jar"));
                Utils.patchJar(inputFile, outputFile, patchers.jarPatchers());
            });
        }
    }
}
