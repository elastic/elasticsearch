/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.release;

import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.TaskAction;

import javax.inject.Inject;
import java.net.URI;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Performs additional checks on changelog files, beyond whether they confirm to the schema.
 */
public class ValidateYamlTask extends DefaultTask {
    private final ConfigurableFileCollection changelogs;
    private final ProjectLayout projectLayout;

    @Inject
    public ValidateYamlTask(ObjectFactory objectFactory, ProjectLayout projectLayout) {
        this.changelogs = objectFactory.fileCollection();
        this.projectLayout = projectLayout;
    }

    @TaskAction
    public void executeTask() {
        final URI rootDir = projectLayout.getProjectDirectory().getAsFile().toURI();
        final Map<String, ChangelogEntry> changelogs = this.changelogs.getFiles()
            .stream()
            .collect(Collectors.toMap(file -> rootDir.relativize(file.toURI()).toString(), ChangelogEntry::parse));

        // We don't try to find all such errors, because we expect them to be rare e.g. only
        // when a new file is added.
        changelogs.forEach((path, entry) -> {
            if ((entry.getType().equals("breaking") || entry.getType().equals("breaking-java")) && entry.getBreaking() == null) {
                throw new GradleException(
                    "[" + path + "] has type [breaking] and must supply a [breaking] section with further information"
                );
            }

            if (entry.getType().equals("deprecation") && entry.getDeprecation() == null) {
                throw new GradleException(
                    "[" + path + "] has type [deprecation] and must supply a [deprecation] section with further information"
                );
            }
        });
    }

    @InputFiles
    public FileCollection getChangelogs() {
        return changelogs;
    }

    public void setChangelogs(FileCollection files) {
        this.changelogs.setFrom(files);
    }
}
