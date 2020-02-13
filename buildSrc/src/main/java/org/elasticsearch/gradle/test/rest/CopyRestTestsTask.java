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
import java.util.stream.Collectors;

/**
 * <p>Copies the files needed for the Rest YAML tests to the current projects test resources output directory.
 * This is intended to be be used from {@link CopyRestApiPlugin} since the plugin wires up the needed
 * configurations and custom extensions.
 * </p>
 * <p>This task supports copying either the Rest YAML tests (.yml), or the Rest API specification (.json).</p>
 * <br>
 * <strong>Rest API specification:</strong> <br>
 * When the {@link CopyRestApiPlugin} has been applied this task will automatically copy the Rest API specification
 * if there are any Rest YAML tests present (either in source, or output) or if `restApi.includeCore` or `restApi.includeXpack` has been
 * explicitly declared through the 'restResources' extension. <br>
 * This task supports copying only a subset of the Rest API specification through the use of the custom extension.<br>
 * <i>For example:</i>
 * <pre>
 * restResources {
 *   restApi {
 *     includeXpack 'enrich'
 *   }
 * }
 * </pre>
 * Will copy any of the the x-pack specs that start with enrich*. The core API specs will also be copied iff the project also has
 * Rest YAML tests. To help optimize the build cache, it is recommended to explicitly declare which specs your project depends on.
 * <i>For example:</i>
 * <pre>
 * restResources {
 *   restApi {
 *     includeCore 'index', 'cat'
 *     includeXpack 'enrich'
 *   }
 * }
 * </pre>
 * <br>
 * <strong>Rest YAML tests :</strong> <br>
 * When the {@link CopyRestApiPlugin} has been applied this task can copy the Rest YAML tests iff explicitly configured with
 * `includeCore` or `includeXpack` through the `restResources.restTests` extension.
 * <i>For example:</i>
 * <pre>
 * restResources {
 *   restTests {
 *     includeXpack 'graph'
 *   }
 * }
 * </pre>
 * Will copy any of the the x-pack tests that start with graph.
 */
public class CopyRestTestsTask extends DefaultTask {
    private static final Logger logger = Logging.getLogger(CopyRestTestsTask.class);
    final ListProperty<String> includeCore = getProject().getObjects().listProperty(String.class);
    final ListProperty<String> includeXpack = getProject().getObjects().listProperty(String.class);

    Configuration coreConfig;
    Configuration xpackConfig;
    String copyTo;

    private final PatternFilterable corePatternSet;
    private final PatternFilterable xpackPatternSet;

    public CopyRestTestsTask() {
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
        // copy tests only if explicitly requested
        return includeCore.get().isEmpty() == false || includeXpack.get().isEmpty() == false ? fileCollection.getAsFileTree() : null;
    }

    @OutputDirectory
    public File getOutputDir() {
        return new File(getTestSourceSet().getOutput().getResourcesDir(), copyTo);
    }

    @TaskAction
    void copy() {
        Project project = getProject();
        // only copy core tests if explicitly instructed
        if (includeCore.get().isEmpty() == false) {
            if (BuildParams.isInternal()) {
                logger.info("Rest tests for project [{}] will be copied to the test resources.", project.getPath());
                project.copy(c -> {
                    c.from(coreConfig.getSingleFile());
                    c.into(getOutputDir());
                    c.include(corePatternSet.getIncludes());
                });

            } else {
                logger.info(
                    "Rest tests for project [{}] will be copied to the test resources from the published jar (version: [{}]).",
                    project.getPath(),
                    VersionProperties.getElasticsearch()
                );
                project.copy(c -> {
                    c.from(project.zipTree(coreConfig.getSingleFile()));
                    c.into(getTestSourceSet().getOutput().getResourcesDir()); // this ends up as the same dir as outputDir
                    c.include(includeCore.get().stream().map(prefix -> copyTo + "/" + prefix + "*/**").collect(Collectors.toList()));
                });
            }
        }
        // only copy x-pack tests if explicitly instructed
        if (includeXpack.get().isEmpty() == false) {
            logger.info("X-pack rest tests for project [{}] will be copied to the test resources.", project.getPath());
            project.copy(c -> {
                c.from(xpackConfig.getSingleFile());
                c.into(getOutputDir());
                c.include(xpackPatternSet.getIncludes());
            });
        }
    }

    private SourceSet getTestSourceSet() {
        return Boilerplate.getJavaSourceSets(getProject()).findByName("test");
    }

}
