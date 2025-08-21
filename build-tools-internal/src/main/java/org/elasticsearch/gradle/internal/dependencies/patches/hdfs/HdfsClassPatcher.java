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
            "hadoop2-common",
            Pattern.compile("hadoop-common-2(?!.*tests)"),
            List.of(
                classPatcher(
                    "org/apache/hadoop/util/ShutdownHookManager.class",
                    "3912451f02da9199dae7dba3f1420e0d951067addabbb235e7551de52234a0ef",
                    ShutdownHookManagerPatcher::new
                ),
                classPatcher(
                    "org/apache/hadoop/util/Shell.class",
                    "60400dc800e7c3e1a5fc499793033d877f5319bbd7633fee05d5a1d96b947bbd",
                    ShellPatcher::new
                ),
                classPatcher(
                    "org/apache/hadoop/security/UserGroupInformation.class",
                    "218078b8c77838f93d015c843775985a71f3c7a8128e2a9394410f0cd1da5f53",
                    SubjectGetSubjectPatcher::new
                )
            )
        ),
        new JarPatchers(
            "hadoop3-common",
            Pattern.compile("hadoop-common-3(?!.*tests)"),
            List.of(
                classPatcher(
                    "org/apache/hadoop/util/ShutdownHookManager.class",
                    "7720e8545a02de6fd03f4170f0e471d1301ef73d7d6a09097bad361f9e31f819",
                    ShutdownHookManagerPatcher::new
                ),
                classPatcher(
                    "org/apache/hadoop/util/Shell.class",
                    "856d0b829cf550df826387af15fa1c772bc7d26d6461535b17b9d5114d308dc4",
                    ShellPatcher::new
                ),
                classPatcher(
                    "org/apache/hadoop/security/UserGroupInformation.class",
                    "52f5973f35a282908d48a573a03c04f240a22c9f6007d7c5e7852aff1c641420",
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
