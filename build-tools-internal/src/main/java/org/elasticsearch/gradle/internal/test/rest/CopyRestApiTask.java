/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gradle.internal.test.rest;

import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.internal.info.BuildParams;
import org.gradle.api.DefaultTask;
import org.gradle.api.file.ArchiveOperations;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.FileSystemOperations;
import org.gradle.api.file.FileTree;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.SkipWhenEmpty;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.util.PatternFilterable;
import org.gradle.api.tasks.util.PatternSet;
import org.gradle.internal.Factory;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.gradle.util.GradleUtils.getProjectPathFromTask;

/**
 * Copies the files needed for the Rest YAML specs to the current projects test resources output directory.
 * This is intended to be be used from {@link RestResourcesPlugin} since the plugin wires up the needed
 * configurations and custom extensions.
 *
 * @see RestResourcesPlugin
 */
public class CopyRestApiTask extends DefaultTask {
    private static final String REST_API_PREFIX = "rest-api-spec/api";
    private static final String REST_TEST_PREFIX = "rest-api-spec/test";
    private final ListProperty<String> include;
    private final DirectoryProperty outputResourceDir;
    private final DirectoryProperty additionalYamlTestsDir;

    private File sourceResourceDir;
    private boolean skipHasRestTestCheck;
    private FileCollection config;
    private FileCollection additionalConfig;
    private Function<FileCollection, FileTree> configToFileTree = FileCollection::getAsFileTree;
    private Function<FileCollection, FileTree> additionalConfigToFileTree = FileCollection::getAsFileTree;

    private final PatternFilterable patternSet;
    private final ProjectLayout projectLayout;
    private final FileSystemOperations fileSystemOperations;

    @Inject
    public CopyRestApiTask(
        ProjectLayout projectLayout,
        Factory<PatternSet> patternSetFactory,
        FileSystemOperations fileSystemOperations,
        ObjectFactory objectFactory
    ) {
        this.include = objectFactory.listProperty(String.class);
        this.outputResourceDir = objectFactory.directoryProperty();
        this.additionalYamlTestsDir = objectFactory.directoryProperty();
        this.patternSet = patternSetFactory.create();
        this.projectLayout = projectLayout;
        this.fileSystemOperations = fileSystemOperations;
    }

    @Input
    public ListProperty<String> getInclude() {
        return include;
    }

    @Input
    public boolean isSkipHasRestTestCheck() {
        return skipHasRestTestCheck;
    }

    @SkipWhenEmpty
    @InputFiles
    public FileTree getInputDir() {
        FileTree coreFileTree = null;
        boolean projectHasYamlRestTests = skipHasRestTestCheck || projectHasYamlRestTests();
        if (include.get().isEmpty() == false || projectHasYamlRestTests) {
            patternSet.setIncludes(include.get().stream().map(prefix -> prefix + "*/**").collect(Collectors.toList()));
            coreFileTree = configToFileTree.apply(config).matching(patternSet); // directory on disk
        }

        FileCollection fileCollection = additionalConfig == null
            ? coreFileTree
            : projectLayout.files(coreFileTree, additionalConfigToFileTree.apply(additionalConfig));

        // if project has rest tests or the includes are explicitly configured execute the task, else NO-SOURCE due to the null input
        return projectHasYamlRestTests || include.get().isEmpty() == false ? fileCollection.getAsFileTree() : null;
    }

    @OutputDirectory
    public DirectoryProperty getOutputResourceDir() {
        return outputResourceDir;
    }

    @Internal
    public DirectoryProperty getAdditionalYamlTestsDir() {
        return additionalYamlTestsDir;
    }

    @TaskAction
    void copy() {
        // clean the output directory to ensure no stale files persist
        fileSystemOperations.delete(d -> d.delete(outputResourceDir));

        // always copy the core specs if the task executes
        String projectPath = getProjectPathFromTask(getPath());
        File restSpecOutputDir = new File(outputResourceDir.get().getAsFile(), REST_API_PREFIX);

        getLogger().debug("Rest specs for project [{}] will be copied to the test resources.", projectPath);
        fileSystemOperations.copy(c -> {
            c.from(configToFileTree.apply(config));
            c.into(restSpecOutputDir);
            c.include(patternSet.getIncludes());
        });
        // copy any additional config
        if (additionalConfig != null) {
            fileSystemOperations.copy(c -> {
                c.from(additionalConfigToFileTree.apply(additionalConfig));
                c.into(restSpecOutputDir);
            });
        }
    }

    /**
     * Returns true if any files with a .yml extension exist the test resources rest-api-spec/test directory (from source or output dir)
     */
    private boolean projectHasYamlRestTests() {
        try {
            // check source folder for tests
            if (sourceResourceDir != null && new File(sourceResourceDir, REST_TEST_PREFIX).exists()) {
                return Files.walk(sourceResourceDir.toPath().resolve(REST_TEST_PREFIX))
                    .anyMatch(p -> p.getFileName().toString().endsWith("yml"));
            }
            // check output for cases where tests are copied programmatically
            File yamlTestOutputDir = new File(additionalYamlTestsDir.get().getAsFile(), REST_TEST_PREFIX);
            if (yamlTestOutputDir.exists()) {
                return Files.walk(yamlTestOutputDir.toPath()).anyMatch(p -> p.getFileName().toString().endsWith("yml"));
            }
        } catch (IOException e) {
            throw new IllegalStateException(String.format("Error determining if this project [%s] has rest tests.", getProject()), e);
        }
        return false;
    }

    public void setSourceResourceDir(File sourceResourceDir) {
        this.sourceResourceDir = sourceResourceDir;
    }

    public void setSkipHasRestTestCheck(boolean skipHasRestTestCheck) {
        this.skipHasRestTestCheck = skipHasRestTestCheck;
    }

    public void setConfig(FileCollection config) {
        this.config = config;
    }

    public void setAdditionalConfig(FileCollection additionalConfig) {
        this.additionalConfig = additionalConfig;
    }

    public void setConfigToFileTree(Function<FileCollection, FileTree> configToFileTree) {
        this.configToFileTree = configToFileTree;
    }

    public void setAdditionalConfigToFileTree(Function<FileCollection, FileTree> additionalConfigToFileTree) {
        this.additionalConfigToFileTree = additionalConfigToFileTree;
    }

}
