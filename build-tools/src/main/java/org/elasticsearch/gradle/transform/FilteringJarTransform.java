/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.transform;

import org.gradle.api.Action;
import org.gradle.api.artifacts.dsl.DependencyHandler;
import org.gradle.api.artifacts.transform.InputArtifact;
import org.gradle.api.artifacts.transform.TransformAction;
import org.gradle.api.artifacts.transform.TransformOutputs;
import org.gradle.api.artifacts.transform.TransformParameters;
import org.gradle.api.artifacts.type.ArtifactTypeDefinition;
import org.gradle.api.file.FileSystemLocation;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.Input;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

public abstract class FilteringJarTransform implements TransformAction<FilteringJarTransform.Parameters> {
    public static final String FILTERED_JAR_TYPE = "filtered-jar";

    @InputArtifact
    public abstract Provider<FileSystemLocation> getInputArtifact();

    @Override
    public void transform(TransformOutputs outputs) {
        File original = getInputArtifact().get().getAsFile();
        File transformed = outputs.file(original.getName());
        List<PathMatcher> excludes = createMatchers(getParameters().getExcludes());

        try (
            ZipFile input = new ZipFile(original);
            ZipOutputStream output = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(transformed)))
        ) {
            Enumeration<? extends ZipEntry> entries = input.entries();
            while (entries.hasMoreElements()) {
                ZipEntry entry = entries.nextElement();
                if (excludes.stream().noneMatch(e -> e.matches(Path.of(entry.getName())))) {
                    output.putNextEntry(entry);
                    input.getInputStream(entry).transferTo(output);
                    output.closeEntry();
                }
            }

            output.flush();
            output.finish();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to patch archive", e);
        }
    }

    private List<PathMatcher> createMatchers(List<String> patterns) {
        return patterns.stream().map(p -> FileSystems.getDefault().getPathMatcher("glob:" + p)).toList();
    }

    public static void registerTransform(DependencyHandler dependencyHandler, Action<Parameters> config) {
        dependencyHandler.registerTransform(FilteringJarTransform.class, spec -> {
            spec.getFrom().attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.JAR_TYPE);
            spec.getTo().attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, FILTERED_JAR_TYPE);
            config.execute(spec.getParameters());
        });
    }

    public abstract static class Parameters implements TransformParameters, Serializable {
        private List<String> excludes = new ArrayList<>();

        @Input
        public List<String> getExcludes() {
            return excludes;
        }

        public void exclude(String exclude) {
            excludes.add(exclude);
        }
    }
}
