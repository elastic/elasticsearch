/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gradle.test.rest;

import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.info.BuildParams;
import org.gradle.api.DefaultTask;
import org.gradle.api.file.ArchiveOperations;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.FileSystemOperations;
import org.gradle.api.file.FileTree;
import org.gradle.api.file.ProjectLayout;
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
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.gradle.util.GradleUtils.getProjectPathFromTask;

/**
 * Copies the Rest YAML test to the current projects test resources output directory.
 * This is intended to be be used from {@link RestResourcesPlugin} since the plugin wires up the needed
 * configurations and custom extensions.
 *
 * @see RestResourcesPlugin
 */
public class CopyRestTestsTask extends DefaultTask {
    private static final String REST_TEST_PREFIX = "rest-api-spec/test";
    private final ListProperty<String> includeCore = getProject().getObjects().listProperty(String.class);
    private final ListProperty<String> includeXpack = getProject().getObjects().listProperty(String.class);

    private File outputResourceDir;
    private FileCollection coreConfig;
    private FileCollection xpackConfig;
    private FileCollection additionalConfig;
    private Function<FileCollection, FileTree> coreConfigToFileTree = FileCollection::getAsFileTree;
    private Function<FileCollection, FileTree> xpackConfigToFileTree = FileCollection::getAsFileTree;
    private Function<FileCollection, FileTree> additionalConfigToFileTree = FileCollection::getAsFileTree;

    private final PatternFilterable corePatternSet;
    private final PatternFilterable xpackPatternSet;
    private final ProjectLayout projectLayout;
    private final FileSystemOperations fileSystemOperations;
    private final ArchiveOperations archiveOperations;

    @Inject
    public CopyRestTestsTask(
        ProjectLayout projectLayout,
        Factory<PatternSet> patternSetFactory,
        FileSystemOperations fileSystemOperations,
        ArchiveOperations archiveOperations
    ) {
        corePatternSet = patternSetFactory.create();
        xpackPatternSet = patternSetFactory.create();
        this.projectLayout = projectLayout;
        this.fileSystemOperations = fileSystemOperations;
        this.archiveOperations = archiveOperations;
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
        FileTree coreFileTree = null;
        FileTree xpackFileTree = null;
        if (includeXpack.get().isEmpty() == false) {
            xpackPatternSet.setIncludes(includeXpack.get().stream().map(prefix -> prefix + "*/**").collect(Collectors.toList()));
            xpackFileTree = xpackConfigToFileTree.apply(xpackConfig).matching(xpackPatternSet);
        }
        if (includeCore.get().isEmpty() == false) {
            if (BuildParams.isInternal()) {
                corePatternSet.setIncludes(includeCore.get().stream().map(prefix -> prefix + "*/**").collect(Collectors.toList()));
                coreFileTree = coreConfigToFileTree.apply(coreConfig).matching(corePatternSet); // directory on disk
            } else {
                coreFileTree = coreConfig.getAsFileTree(); // jar file
            }
        }
        FileCollection fileCollection = additionalConfig == null
            ? projectLayout.files(coreFileTree, xpackFileTree)
            : projectLayout.files(coreFileTree, xpackFileTree, additionalConfigToFileTree.apply(additionalConfig));

        // copy tests only if explicitly requested
        return includeCore.get().isEmpty() == false || includeXpack.get().isEmpty() == false || additionalConfig != null
            ? fileCollection.getAsFileTree()
            : null;
    }

    @OutputDirectory
    public File getOutputDir() {
        return new File(outputResourceDir, REST_TEST_PREFIX);
    }

    @TaskAction
    void copy() {
        String projectPath = getProjectPathFromTask(getPath());
        // only copy core tests if explicitly instructed
        if (includeCore.get().isEmpty() == false) {
            if (BuildParams.isInternal()) {
                getLogger().debug("Rest tests for project [{}] will be copied to the test resources.", projectPath);
                fileSystemOperations.copy(c -> {
                    c.from(coreConfigToFileTree.apply(coreConfig));
                    c.into(getOutputDir());
                    c.include(corePatternSet.getIncludes());
                });
            } else {
                getLogger().debug(
                    "Rest tests for project [{}] will be copied to the test resources from the published jar (version: [{}]).",
                    projectPath,
                    VersionProperties.getElasticsearch()
                );
                fileSystemOperations.copy(c -> {
                    c.from(archiveOperations.zipTree(coreConfig.getSingleFile())); // jar file
                    c.into(Objects.requireNonNull(outputResourceDir));
                    c.include(
                        includeCore.get().stream().map(prefix -> REST_TEST_PREFIX + "/" + prefix + "*/**").collect(Collectors.toList())
                    );
                });
            }
        }
        // only copy x-pack tests if explicitly instructed
        if (includeXpack.get().isEmpty() == false) {
            getLogger().debug("X-pack rest tests for project [{}] will be copied to the test resources.", projectPath);
            fileSystemOperations.copy(c -> {
                c.from(xpackConfigToFileTree.apply(xpackConfig));
                c.into(getOutputDir());
                c.include(xpackPatternSet.getIncludes());
            });
        }
        // copy any additional config
        if (additionalConfig != null) {
            fileSystemOperations.copy(c -> {
                c.from(additionalConfigToFileTree.apply(additionalConfig));
                c.into(getOutputDir());
            });
        }
    }

    public void setOutputResourceDir(File outputResourceDir) {
        this.outputResourceDir = outputResourceDir;
    }

    public void setCoreConfig(FileCollection coreConfig) {
        this.coreConfig = coreConfig;
    }

    public void setXpackConfig(FileCollection xpackConfig) {
        this.xpackConfig = xpackConfig;
    }

    public void setAdditionalConfig(FileCollection additionalConfig) {
        this.additionalConfig = additionalConfig;
    }

    public void setCoreConfigToFileTree(Function<FileCollection, FileTree> coreConfigToFileTree) {
        this.coreConfigToFileTree = coreConfigToFileTree;
    }

    public void setXpackConfigToFileTree(Function<FileCollection, FileTree> xpackConfigToFileTree) {
        this.xpackConfigToFileTree = xpackConfigToFileTree;
    }

    public void setAdditionalConfigToFileTree(Function<FileCollection, FileTree> additionalConfigToFileTree) {
        this.additionalConfigToFileTree = additionalConfigToFileTree;
    }

    @Internal
    public FileCollection getCoreConfig() {
        return coreConfig;
    }

    @Internal
    public FileCollection getXpackConfig() {
        return xpackConfig;
    }
}
