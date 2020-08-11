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
import java.io.IOException;
import java.nio.file.Files;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Copies the files needed for the Rest YAML specs to the current projects test resources output directory.
 * This is intended to be be used from {@link RestResourcesPlugin} since the plugin wires up the needed
 * configurations and custom extensions.
 * @see RestResourcesPlugin
 */
public class CopyRestApiTask extends DefaultTask {
    private static final String REST_API_PREFIX = "rest-api-spec/api";
    final ListProperty<String> includeCore = getProject().getObjects().listProperty(String.class);
    final ListProperty<String> includeXpack = getProject().getObjects().listProperty(String.class);
    String sourceSetName;
    Configuration coreConfig;
    Configuration xpackConfig;

    private final PatternFilterable corePatternSet;
    private final PatternFilterable xpackPatternSet;

    public CopyRestApiTask() {
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
        boolean projectHasYamlRestTests = projectHasYamlRestTests();
        if (includeCore.get().isEmpty() == false || projectHasYamlRestTests) {
            if (BuildParams.isInternal()) {
                corePatternSet.setIncludes(includeCore.get().stream().map(prefix -> prefix + "*/**").collect(Collectors.toList()));
                coreFileTree = coreConfig.getAsFileTree().matching(corePatternSet); // directory on disk
            } else {
                coreFileTree = coreConfig.getAsFileTree(); // jar file
            }
        }
        ConfigurableFileCollection fileCollection = getProject().files(coreFileTree, xpackFileTree);

        // if project has rest tests or the includes are explicitly configured execute the task, else NO-SOURCE due to the null input
        return projectHasYamlRestTests || includeCore.get().isEmpty() == false || includeXpack.get().isEmpty() == false
            ? fileCollection.getAsFileTree()
            : null;
    }

    @OutputDirectory
    public File getOutputDir() {
        return new File(
            getSourceSet().orElseThrow(() -> new IllegalArgumentException("could not find source set [" + sourceSetName + "]"))
                .getOutput()
                .getResourcesDir(),
            REST_API_PREFIX
        );
    }

    @TaskAction
    void copy() {
        Project project = getProject();
        // always copy the core specs if the task executes
        if (BuildParams.isInternal()) {
            getLogger().debug("Rest specs for project [{}] will be copied to the test resources.", project.getPath());
            project.copy(c -> {
                c.from(coreConfig.getSingleFile());
                c.into(getOutputDir());
                c.include(corePatternSet.getIncludes());
            });
        } else {
            getLogger().debug(
                "Rest specs for project [{}] will be copied to the test resources from the published jar (version: [{}]).",
                project.getPath(),
                VersionProperties.getElasticsearch()
            );
            project.copy(c -> {
                c.from(project.zipTree(coreConfig.getSingleFile()));
                // this ends up as the same dir as outputDir
                c.into(Objects.requireNonNull(getSourceSet().orElseThrow().getOutput().getResourcesDir()));
                if (includeCore.get().isEmpty()) {
                    c.include(REST_API_PREFIX + "/**");
                } else {
                    c.include(
                        includeCore.get().stream().map(prefix -> REST_API_PREFIX + "/" + prefix + "*/**").collect(Collectors.toList())
                    );
                }
            });
        }
        // only copy x-pack specs if explicitly instructed
        if (includeXpack.get().isEmpty() == false) {
            getLogger().debug("X-pack rest specs for project [{}] will be copied to the test resources.", project.getPath());
            project.copy(c -> {
                c.from(xpackConfig.getSingleFile());
                c.into(getOutputDir());
                c.include(xpackPatternSet.getIncludes());
            });
        }
    }

    /**
     * Returns true if any files with a .yml extension exist the test resources rest-api-spec/test directory (from source or output dir)
     */
    private boolean projectHasYamlRestTests() {
        File testSourceResourceDir = getTestSourceResourceDir();
        File testOutputResourceDir = getTestOutputResourceDir(); // check output for cases where tests are copied programmatically

        if (testSourceResourceDir == null && testOutputResourceDir == null) {
            return false;
        }
        try {
            if (testSourceResourceDir != null && new File(testSourceResourceDir, "rest-api-spec/test").exists()) {
                return Files.walk(testSourceResourceDir.toPath().resolve("rest-api-spec/test"))
                    .anyMatch(p -> p.getFileName().toString().endsWith("yml"));
            }
            if (testOutputResourceDir != null && new File(testOutputResourceDir, "rest-api-spec/test").exists()) {
                return Files.walk(testOutputResourceDir.toPath().resolve("rest-api-spec/test"))
                    .anyMatch(p -> p.getFileName().toString().endsWith("yml"));
            }
        } catch (IOException e) {
            throw new IllegalStateException(String.format("Error determining if this project [%s] has rest tests.", getProject()), e);
        }
        return false;
    }

    private File getTestSourceResourceDir() {
        Optional<SourceSet> testSourceSet = getSourceSet();
        if (testSourceSet.isPresent()) {
            SourceSet testSources = testSourceSet.get();
            Set<File> resourceDir = testSources.getResources()
                .getSrcDirs()
                .stream()
                .filter(f -> f.isDirectory() && f.getParentFile().getName().equals(getSourceSetName()) && f.getName().equals("resources"))
                .collect(Collectors.toSet());
            assert resourceDir.size() <= 1;
            if (resourceDir.size() == 0) {
                return null;
            }
            return resourceDir.iterator().next();
        } else {
            return null;
        }
    }

    private File getTestOutputResourceDir() {
        Optional<SourceSet> testSourceSet = getSourceSet();
        return testSourceSet.map(sourceSet -> sourceSet.getOutput().getResourcesDir()).orElse(null);
    }

    private Optional<SourceSet> getSourceSet() {
        Project project = getProject();
        return project.getConvention().findPlugin(JavaPluginConvention.class) == null
            ? Optional.empty()
            : Optional.ofNullable(GradleUtils.getJavaSourceSets(project).findByName(getSourceSetName()));
    }
}
