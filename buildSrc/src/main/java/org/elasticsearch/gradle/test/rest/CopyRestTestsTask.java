/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.gradle.test.rest;

import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.info.BuildParams;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.DefaultTask;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.FileTree;
import org.gradle.api.plugins.JavaPluginConvention;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.SkipWhenEmpty;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.util.PatternFilterable;
import org.gradle.api.tasks.util.PatternSet;
import org.gradle.internal.Factory;

import javax.inject.Inject;
import java.io.File;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Copies the Rest YAML test to the current projects test resources output directory.
 * This is intended to be be used from {@link RestResourcesPlugin} since the plugin wires up the needed
 * configurations and custom extensions.
 * @see RestResourcesPlugin
 */
public class CopyRestTestsTask extends DefaultTask {
    private static final String REST_TEST_PREFIX = "rest-api-spec/test";
    final ListProperty<String> includeCore = getProject().getObjects().listProperty(String.class);
    final ListProperty<String> includeXpack = getProject().getObjects().listProperty(String.class);

    String sourceSetName;
    Configuration coreConfig;
    Configuration xpackConfig;

    private final PatternFilterable corePatternSet;
    private final PatternFilterable xpackPatternSet;

    public CopyRestTestsTask() {
        corePatternSet = getPatternSetFactory().create();
        xpackPatternSet = getPatternSetFactory().create();
    }

    @Inject
    protected Factory<PatternSet> getPatternSetFactory() {
        throw new UnsupportedOperationException();
    }

    @Input
    public ListProperty<String> getIncludeCore() {
        return includeCore;
    }

    @Input
    public ListProperty<String> getIncludeXpack() {
        return includeXpack;
    }

    @org.gradle.api.tasks.Optional
    @Input
    String getSourceSetName() {
        return sourceSetName;
    }

    @SkipWhenEmpty
    @InputFiles
    public FileTree getInputDir() {
        FileTree coreFileTree = null;
        FileTree xpackFileTree = null;
        if (includeXpack.get().isEmpty() == false) {
            xpackPatternSet.setIncludes(includeXpack.get().stream().map(prefix -> prefix + "*/**").collect(Collectors.toList()));
            xpackFileTree = xpackConfig.getAsFileTree().matching(xpackPatternSet);
        }
        if (includeCore.get().isEmpty() == false) {
            if (BuildParams.isInternal()) {
                corePatternSet.setIncludes(includeCore.get().stream().map(prefix -> prefix + "*/**").collect(Collectors.toList()));
                coreFileTree = coreConfig.getAsFileTree().matching(corePatternSet); // directory on disk
            } else {
                coreFileTree = coreConfig.getAsFileTree(); // jar file
            }
        }
        ConfigurableFileCollection fileCollection = getProject().files(coreFileTree, xpackFileTree);

        // copy tests only if explicitly requested
        return includeCore.get().isEmpty() == false || includeXpack.get().isEmpty() == false ? fileCollection.getAsFileTree() : null;
    }

    @OutputDirectory
    public File getOutputDir() {
        return new File(
            getSourceSet().orElseThrow(() -> new IllegalArgumentException("could not find source set [" + sourceSetName + "]"))
                .getOutput()
                .getResourcesDir(),
            REST_TEST_PREFIX
        );
    }

    @TaskAction
    void copy() {
        Project project = getProject();
        // only copy core tests if explicitly instructed
        if (includeCore.get().isEmpty() == false) {
            if (BuildParams.isInternal()) {
                getLogger().debug("Rest tests for project [{}] will be copied to the test resources.", project.getPath());
                project.copy(c -> {
                    c.from(coreConfig.getAsFileTree());
                    c.into(getOutputDir());
                    c.include(corePatternSet.getIncludes());
                });

            } else {
                getLogger().debug(
                    "Rest tests for project [{}] will be copied to the test resources from the published jar (version: [{}]).",
                    project.getPath(),
                    VersionProperties.getElasticsearch()
                );
                project.copy(c -> {
                    c.from(project.zipTree(coreConfig.getSingleFile()));
                    // this ends up as the same dir as outputDir
                    c.into(Objects.requireNonNull(getSourceSet().orElseThrow().getOutput().getResourcesDir()));
                    c.include(
                        includeCore.get().stream().map(prefix -> REST_TEST_PREFIX + "/" + prefix + "*/**").collect(Collectors.toList())
                    );
                });
            }
        }
        // only copy x-pack tests if explicitly instructed
        if (includeXpack.get().isEmpty() == false) {
            getLogger().debug("X-pack rest tests for project [{}] will be copied to the test resources.", project.getPath());
            project.copy(c -> {
                c.from(xpackConfig.getAsFileTree());
                c.into(getOutputDir());
                c.include(xpackPatternSet.getIncludes());
            });
        }
    }

    private Optional<SourceSet> getSourceSet() {
        Project project = getProject();
        return project.getConvention().findPlugin(JavaPluginConvention.class) == null
            ? Optional.empty()
            : Optional.ofNullable(GradleUtils.getJavaSourceSets(project).findByName(getSourceSetName()));
    }
}
