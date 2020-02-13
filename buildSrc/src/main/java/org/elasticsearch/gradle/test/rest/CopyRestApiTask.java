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
import org.elasticsearch.gradle.tool.Boilerplate;
import org.gradle.api.DefaultTask;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.FileTree;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
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
import java.util.Set;
import java.util.stream.Collectors;

public class CopyRestApiTask extends DefaultTask {
    private static final Logger logger = Logging.getLogger(CopyRestApiTask.class);
    final ListProperty<String> includeCore = getProject().getObjects().listProperty(String.class);
    final ListProperty<String> includeXpack = getProject().getObjects().listProperty(String.class);

    Configuration coreConfig;
    Configuration xpackConfig;
    String copyTo;

    private final PatternFilterable corePatternSet;
    private final PatternFilterable xpackPatternSet;

    public CopyRestApiTask() {
        // TODO: blow up if internal and requested x-pack
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

    @SkipWhenEmpty
    @InputFiles
    public FileTree getInputDir() {
        xpackPatternSet.setIncludes(includeXpack.get().stream().map(prefix -> prefix + "*/**").collect(Collectors.toList()));
        ConfigurableFileCollection fileCollection = getProject().files(xpackConfig.getAsFileTree().matching(xpackPatternSet));
        if (BuildParams.isInternal()) {
            corePatternSet.setIncludes(includeCore.get().stream().map(prefix -> prefix + "*/**").collect(Collectors.toList()));
            fileCollection.plus(coreConfig.getAsFileTree().matching(corePatternSet));
        } else {
            fileCollection.plus(coreConfig);
        }
        // if project has rest tests or the includes are explicitly configured execute the task, else NO-SOURCE due to the null input
        return projectHasYamlRestTests() || includeCore.get().isEmpty() == false || includeXpack.get().isEmpty() == false
            ? fileCollection.getAsFileTree()
            : null;
    }

    @OutputDirectory
    public File getOutputDir() {
        return new File(getTestSourceSet().getOutput().getResourcesDir(), copyTo);
    }

    @TaskAction
    void copy() {
        Project project = getProject();
        // always copy the core specs if the task executes
        if (BuildParams.isInternal()) {
            logger.info("Rest specs for project [{}] will be copied to the test resources.", project.getPath());
            project.copy(c -> {
                c.from(coreConfig.getSingleFile());
                c.into(getOutputDir());
                c.include(corePatternSet.getIncludes());
            });

        } else {
            logger.info(
                "Rest specs for project [{}] will be copied to the test resources from the published jar (version: [{}]).",
                project.getPath(),
                VersionProperties.getElasticsearch()
            );
            project.copy(c -> {
                c.from(project.zipTree(coreConfig.getSingleFile()));
                c.into(getTestSourceSet().getOutput().getResourcesDir()); // this ends up as the same dir as outputDir
                c.include(includeCore.get().stream().map(prefix -> copyTo + "/" + prefix + "*/**").collect(Collectors.toList()));
            });
        }
        // only copy x-pack specs if explicitly instructed
        if (includeXpack.get().isEmpty() == false) {
            logger.info("X-pack rest specs for project [{}] will be copied to the test resources.", project.getPath());
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
            if (testSourceResourceDir != null) {
                return new File(testSourceResourceDir, "rest-api-spec/test").exists() == false
                    || Files.walk(testSourceResourceDir.toPath().resolve("rest-api-spec/test"))
                        .anyMatch(p -> p.getFileName().toString().endsWith("yml"));
            }
            if (testOutputResourceDir != null) {
                return new File(testOutputResourceDir, "rest-api-spec/test").exists() == false
                    || Files.walk(testOutputResourceDir.toPath().resolve("rest-api-spec/test"))
                        .anyMatch(p -> p.getFileName().toString().endsWith("yml"));
            }
        } catch (IOException e) {
            throw new IllegalStateException(String.format("Error determining if this project [%s] has rest tests.", getProject()), e);
        }
        return false;
    }

    private File getTestSourceResourceDir() {
        SourceSet testSources = getTestSourceSet();
        if (testSources == null) {
            return null;
        }
        Set<File> resourceDir = testSources.getResources()
            .getSrcDirs()
            .stream()
            .filter(f -> f.isDirectory() && f.getParentFile().getName().equals("test") && f.getName().equals("resources"))
            .collect(Collectors.toSet());
        assert resourceDir.size() <= 1;
        if (resourceDir.size() == 0) {
            return null;
        }
        return resourceDir.iterator().next();
    }

    private File getTestOutputResourceDir() {
        SourceSet testSources = getTestSourceSet();
        if (testSources == null) {
            return null;
        }
        return testSources.getOutput().getResourcesDir();
    }

    private SourceSet getTestSourceSet() {
        return Boilerplate.getJavaSourceSets(getProject()).findByName("test");
    }
}
