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
                classPatcher("org/apache/hadoop/util/ShutdownHookManager.class", ShutdownHookManagerPatcher::new),
                classPatcher("org/apache/hadoop/util/Shell.class", ShellPatcher::new),
                classPatcher("org/apache/hadoop/security/UserGroupInformation.class", SubjectGetSubjectPatcher::new)
            )
        ),
        new JarPatchers(
            "hadoop-client-api",
            Pattern.compile("hadoop-client-api.*"),
            List.of(
                classPatcher("org/apache/hadoop/util/ShutdownHookManager.class", ShutdownHookManagerPatcher::new),
                classPatcher("org/apache/hadoop/util/Shell.class", ShellPatcher::new),
                classPatcher("org/apache/hadoop/security/UserGroupInformation.class", SubjectGetSubjectPatcher::new),
                classPatcher("org/apache/hadoop/security/authentication/client/KerberosAuthenticator.class", SubjectGetSubjectPatcher::new)
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
