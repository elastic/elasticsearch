/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gradle.internal.precommit;

import org.elasticsearch.gradle.OS;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.FileTree;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.tasks.IgnoreEmptyDirectories;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.SkipWhenEmpty;
import org.gradle.api.tasks.StopExecutionException;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.util.PatternFilterable;
import org.gradle.api.tasks.util.PatternSet;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

/**
 * Checks source files for correct file permissions.
 */
public abstract class FilePermissionsTask extends DefaultTask {

    private final ProjectLayout projectLayout;

    /**
     * A pattern set of which files should be checked.
     */
    private final PatternFilterable filesFilter = new PatternSet()
        // we always include all source files, and exclude what should not be checked
        .include("**")
        // exclude sh files that might have the executable bit set
        .exclude("**/*.sh");

    private File outputMarker = new File(getProject().getBuildDir(), "markers/filePermissions");

    @Inject
    public FilePermissionsTask(ProjectLayout projectLayout) {
        this.projectLayout = projectLayout;
        setDescription("Checks java source files for correct file permissions");
    }

    private static boolean isExecutableFile(File file) {
        try {
            Set<PosixFilePermission> permissions = Files.getFileAttributeView(file.toPath(), PosixFileAttributeView.class)
                .readAttributes()
                .permissions();
            return permissions.contains(PosixFilePermission.OTHERS_EXECUTE)
                || permissions.contains(PosixFilePermission.OWNER_EXECUTE)
                || permissions.contains(PosixFilePermission.GROUP_EXECUTE);
        } catch (IOException e) {
            throw new IllegalStateException("unable to read the file " + file + " attributes", e);
        }
    }

    /**
     * Returns the files this task will check
     */
    @InputFiles
    @IgnoreEmptyDirectories
    @SkipWhenEmpty
    public FileCollection getFiles() {
        return getSources().get()
            .stream()
            .map(sourceTree -> sourceTree.matching(filesFilter))
            .reduce(FileTree::plus)
            .orElse(projectLayout.files().getAsFileTree());
    }

    @TaskAction
    public void checkInvalidPermissions() throws IOException {
        if (OS.current() == OS.WINDOWS) {
            throw new StopExecutionException();
        }
        List<String> failures = getFiles().getFiles()
            .stream()
            .filter(FilePermissionsTask::isExecutableFile)
            .map(file -> "Source file is executable: " + file)
            .collect(Collectors.toList());

        if (failures.isEmpty() == false) {
            throw new GradleException("Found invalid file permissions:\n" + String.join("\n", failures));
        }

        outputMarker.getParentFile().mkdirs();
        Files.write(outputMarker.toPath(), "done".getBytes("UTF-8"));
    }

    @OutputFile
    public File getOutputMarker() {
        return outputMarker;
    }

    @Internal
    public abstract ListProperty<FileTree> getSources();
}
