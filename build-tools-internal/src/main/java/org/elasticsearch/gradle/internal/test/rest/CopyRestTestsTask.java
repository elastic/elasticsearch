/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.internal.test.rest;

import org.apache.tools.ant.filters.ReplaceTokens;
import org.elasticsearch.gradle.internal.util.SerializableFunction;
import org.gradle.api.DefaultTask;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.FileSystemOperations;
import org.gradle.api.file.FileTree;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.internal.file.FileOperations;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.tasks.IgnoreEmptyDirectories;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.SkipWhenEmpty;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.util.PatternFilterable;
import org.gradle.api.tasks.util.PatternSet;
import org.gradle.internal.Factory;

import java.io.File;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import static org.elasticsearch.gradle.util.GradleUtils.getProjectPathFromTask;

/**
 * Copies the Rest YAML test to the current projects test resources output directory.
 * This is intended to be be used from {@link RestResourcesPlugin} since the plugin wires up the needed
 * configurations and custom extensions.
 *
 * @see RestResourcesPlugin
 */
public abstract class CopyRestTestsTask extends DefaultTask {
    private static final String REST_TEST_PREFIX = "rest-api-spec/test";
    private final ListProperty<String> includeCore;
    private final ListProperty<String> includeXpack;
    private Map<String, String> substitutions;
    private final DirectoryProperty outputResourceDir;

    private FileCollection coreConfig;
    private FileCollection xpackConfig;
    private FileCollection additionalConfig;
    private SerializableFunction<FileCollection, FileTree> coreConfigToFileTree = FileCollection::getAsFileTree;
    private SerializableFunction<FileCollection, FileTree> xpackConfigToFileTree = FileCollection::getAsFileTree;
    private SerializableFunction<FileCollection, FileTree> additionalConfigToFileTree = FileCollection::getAsFileTree;

    private final PatternFilterable corePatternSet;
    private final PatternFilterable xpackPatternSet;
    private final ProjectLayout projectLayout;
    private final FileSystemOperations fileSystemOperations;

    @Inject
    public abstract FileOperations getFileOperations();

    @Inject
    public CopyRestTestsTask(
        ProjectLayout projectLayout,
        Factory<PatternSet> patternSetFactory,
        FileSystemOperations fileSystemOperations,
        ObjectFactory objectFactory
    ) {
        this.includeCore = objectFactory.listProperty(String.class);
        this.includeXpack = objectFactory.listProperty(String.class);
        this.outputResourceDir = objectFactory.directoryProperty();
        this.corePatternSet = patternSetFactory.create();
        this.xpackPatternSet = patternSetFactory.create();
        this.projectLayout = projectLayout;
        this.fileSystemOperations = fileSystemOperations;
    }

    @Input
    public ListProperty<String> getIncludeCore() {
        return includeCore;
    }

    @Input
    public ListProperty<String> getIncludeXpack() {
        return includeXpack;
    }

    public void setSubstitutions(Map<String, String> substitutions) {
        this.substitutions = substitutions;
    }

    @Input
    @Optional
    public Map<String, String> getSubstitutions() {
        return substitutions;
    }

    @SkipWhenEmpty
    @IgnoreEmptyDirectories
    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    public FileTree getInputDir() {
        FileTree coreFileTree = null;
        FileTree xpackFileTree = null;
        if (includeXpack.get().isEmpty() == false) {
            xpackPatternSet.setIncludes(includeXpack.get().stream().map(prefix -> prefix + "*/**").collect(Collectors.toList()));
            xpackFileTree = xpackConfigToFileTree.apply(xpackConfig).matching(xpackPatternSet);
        }
        if (includeCore.get().isEmpty() == false) {
            corePatternSet.setIncludes(includeCore.get().stream().map(prefix -> prefix + "*/**").collect(Collectors.toList()));
            coreFileTree = coreConfigToFileTree.apply(coreConfig).matching(corePatternSet); // directory on disk
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
    public DirectoryProperty getOutputResourceDir() {
        return outputResourceDir;
    }

    @TaskAction
    void copy() {
        // clean the output directory to ensure no stale files persist
        fileSystemOperations.delete(d -> d.delete(outputResourceDir));

        String projectPath = getProjectPathFromTask(getPath());
        File restTestOutputDir = new File(outputResourceDir.get().getAsFile(), REST_TEST_PREFIX);

        // only copy core tests if explicitly instructed
        if (includeCore.get().isEmpty() == false) {
            getLogger().debug("Rest tests for project [{}] will be copied to the test resources.", projectPath);
            fileSystemOperations.copy(c -> {
                c.from(coreConfigToFileTree.apply(coreConfig));
                c.into(restTestOutputDir);
                c.include(corePatternSet.getIncludes());
                if (substitutions != null) {
                    c.filter(Map.of("tokens", substitutions), ReplaceTokens.class);
                }
            });
        }
        // only copy x-pack tests if explicitly instructed
        if (includeXpack.get().isEmpty() == false) {
            getLogger().debug("X-pack rest tests for project [{}] will be copied to the test resources.", projectPath);
            fileSystemOperations.copy(c -> {
                c.from(xpackConfigToFileTree.apply(xpackConfig));
                c.into(restTestOutputDir);
                c.include(xpackPatternSet.getIncludes());
                if (substitutions != null) {
                    c.filter(Map.of("tokens", substitutions), ReplaceTokens.class);
                }
            });
        }
        // copy any additional config
        if (additionalConfig != null) {
            fileSystemOperations.copy(c -> {
                c.from(additionalConfigToFileTree.apply(additionalConfig));
                c.into(restTestOutputDir);
                if (substitutions != null) {
                    c.filter(Map.of("tokens", substitutions), ReplaceTokens.class);
                }
            });
        }
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

    public void setCoreConfigToFileTree(SerializableFunction<FileCollection, FileTree> coreConfigToFileTree) {
        this.coreConfigToFileTree = coreConfigToFileTree;
    }

    public void setXpackConfigToFileTree(SerializableFunction<FileCollection, FileTree> xpackConfigToFileTree) {
        this.xpackConfigToFileTree = xpackConfigToFileTree;
    }

    public void setAdditionalConfigToFileTree(SerializableFunction<FileCollection, FileTree> additionalConfigToFileTree) {
        this.additionalConfigToFileTree = additionalConfigToFileTree;
    }

}
