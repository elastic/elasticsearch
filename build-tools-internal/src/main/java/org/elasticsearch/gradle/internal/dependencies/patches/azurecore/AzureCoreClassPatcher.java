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
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;

import static org.elasticsearch.gradle.internal.dependencies.patches.PatcherInfo.classPatcher;

public abstract class AzureCoreClassPatcher implements TransformAction<TransformParameters.None> {

    private static final String JAR_FILE_TO_PATCH = "azure-core-1.55.3.jar";

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

        // TODO could use a regex here so it matches regardless of JAR version
        if (inputFile.getName().equals(JAR_FILE_TO_PATCH)) {
            System.out.println("Patching " + inputFile.getName());
            // This would be cleaner if gradle artifact transformers supported temp files https://github.com/gradle/gradle/issues/30440.
            // We need to patch (or delete) the Manifest file as it contains signatures for the class files - which obviously get
            // invalidated when we patch the bytecode - but the existing patch tools only work on class files, so we unfortunately need to
            // duplicate the loop here. We _also_ need to create a "temporary" jar to write (or not) the patched manifest and the rest of
            // the jar contents.
            // TODO we could possibly update `Utils.patchJar` to enable patching manifests etc and most of this would be significantly
            // simplified
            File firstOutputFile = outputs.file(inputFile.getName().replace(".jar", "-half-patched"));

            try (
                JarFile jarFile = new JarFile(inputFile);
                JarOutputStream jos = new JarOutputStream(new FileOutputStream(firstOutputFile))
            ) {
                Enumeration<JarEntry> entries = jarFile.entries();
                while (entries.hasMoreElements()) {
                    JarEntry entry = entries.nextElement();
                    String entryName = entry.getName();

                    if (entryName.endsWith("MANIFEST.MF") == false) {
                        jos.putNextEntry(new JarEntry(entryName));
                        try (InputStream is = jarFile.getInputStream(entry)) {
                            is.transferTo(jos);
                        }
                        jos.closeEntry();
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            File secondOutputFile = outputs.file(inputFile.getName().replace(".jar", "-patched.jar"));
            Utils.patchJar(firstOutputFile, secondOutputFile, CLASS_PATCHERS);
        } else {
            System.out.println("Skipping " + inputFile.getName());
            outputs.file(getInputArtifact());
        }
    }

}
