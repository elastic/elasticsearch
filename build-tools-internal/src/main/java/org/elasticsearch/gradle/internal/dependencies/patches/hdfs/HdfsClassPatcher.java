/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.dependencies.patches.hdfs;

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
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.regex.Pattern;

import static java.util.Map.entry;
import static org.objectweb.asm.ClassWriter.COMPUTE_FRAMES;
import static org.objectweb.asm.ClassWriter.COMPUTE_MAXS;

@CacheableTransform
public abstract class HdfsClassPatcher implements TransformAction<HdfsClassPatcher.Parameters> {

    record JarPatchers(String artifactTag, Pattern artifactPattern, Map<String, Function<ClassWriter, ClassVisitor>> jarPatchers) {}

    static final List<JarPatchers> allPatchers = List.of(
        new JarPatchers(
            "hadoop-common",
            Pattern.compile("hadoop-common-(?!.*tests)"),
            Map.ofEntries(
                entry("org/apache/hadoop/util/ShutdownHookManager.class", ShutdownHookManagerPatcher::new),
                entry("org/apache/hadoop/util/Shell.class", ShellPatcher::new),
                entry("org/apache/hadoop/security/UserGroupInformation.class", SubjectGetSubjectPatcher::new)
            )
        ),
        new JarPatchers(
            "hadoop-client-api",
            Pattern.compile("hadoop-client-api.*"),
            Map.ofEntries(
                entry("org/apache/hadoop/util/ShutdownHookManager.class", ShutdownHookManagerPatcher::new),
                entry("org/apache/hadoop/util/Shell.class", ShellPatcher::new),
                entry("org/apache/hadoop/security/UserGroupInformation.class", SubjectGetSubjectPatcher::new),
                entry("org/apache/hadoop/security/authentication/client/KerberosAuthenticator.class", SubjectGetSubjectPatcher::new)
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

                Map<String, Function<ClassWriter, ClassVisitor>> jarPatchers = new HashMap<>(patchers.jarPatchers());
                File outputFile = outputs.file(inputFile.getName().replace(".jar", "-patched.jar"));

                patchJar(inputFile, outputFile, jarPatchers);

                if (jarPatchers.isEmpty() == false) {
                    throw new IllegalArgumentException(
                        String.format(
                            Locale.ROOT,
                            "error patching [%s] with [%s]: the jar does not contain [%s]",
                            inputFile.getName(),
                            patchers.artifactPattern().toString(),
                            String.join(", ", jarPatchers.keySet())
                        )
                    );
                }
            });
        }
    }

    private static void patchJar(File inputFile, File outputFile, Map<String, Function<ClassWriter, ClassVisitor>> jarPatchers) {
        try (JarFile jarFile = new JarFile(inputFile); JarOutputStream jos = new JarOutputStream(new FileOutputStream(outputFile))) {
            Enumeration<JarEntry> entries = jarFile.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                String entryName = entry.getName();
                // Add the entry to the new JAR file
                jos.putNextEntry(new JarEntry(entryName));

                Function<ClassWriter, ClassVisitor> classPatcher = jarPatchers.remove(entryName);
                if (classPatcher != null) {
                    byte[] classToPatch = jarFile.getInputStream(entry).readAllBytes();

                    ClassReader classReader = new ClassReader(classToPatch);
                    ClassWriter classWriter = new ClassWriter(classReader, COMPUTE_FRAMES | COMPUTE_MAXS);
                    classReader.accept(classPatcher.apply(classWriter), 0);

                    jos.write(classWriter.toByteArray());
                } else {
                    // Read the entry's data and write it to the new JAR
                    try (InputStream is = jarFile.getInputStream(entry)) {
                        is.transferTo(jos);
                    }
                }
                jos.closeEntry();
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
