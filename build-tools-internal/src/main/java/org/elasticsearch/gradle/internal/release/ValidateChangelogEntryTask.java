/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.release;

import com.google.common.annotations.VisibleForTesting;

import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.TaskAction;

import java.net.URI;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

/**
 * Performs additional checks on changelog files, beyond whether they conform to the schema.
 */
public class ValidateChangelogEntryTask extends DefaultTask {
    private final ConfigurableFileCollection changelogs;
    private final ProjectLayout projectLayout;

    public static final String TRIPLE_BACKTICK = "```";
    private static final String CODE_BLOCK_ERROR = """
        [%s] uses a triple-backtick in the [%s] section, but it must be
        formatted as a Asciidoc code block. For example:

            [source,yaml]
            ----
            {
              "metrics.time" : 10,
              "metrics.time.min" : 1,
              "metrics.time.max" : 500
            }
            ----
        """;

    @Inject
    public ValidateChangelogEntryTask(ObjectFactory objectFactory, ProjectLayout projectLayout) {
        this.changelogs = objectFactory.fileCollection();
        this.projectLayout = projectLayout;
    }

    @TaskAction
    public void executeTask() {
        final URI rootDir = projectLayout.getProjectDirectory().getAsFile().toURI();
        final Map<String, ChangelogEntry> changelogs = this.changelogs.getFiles()
            .stream()
            .collect(Collectors.toMap(file -> rootDir.relativize(file.toURI()).toString(), ChangelogEntry::parse));

        changelogs.forEach(ValidateChangelogEntryTask::validate);
    }

    @VisibleForTesting
    static void validate(String path, ChangelogEntry entry) {
        // We don't try to find all such errors, because we expect them to be rare e.g. only
        // when a new file is added.
        final String type = entry.getType();

        if (type.equals("known-issue") == false && type.equals("security") == false) {
            if (entry.getPr() == null) {
                throw new GradleException(
                    "[" + path + "] must provide a [pr] number (only 'known-issue' and 'security' entries can omit this"
                );
            }

            if (entry.getArea() == null) {
                throw new GradleException("[" + path + "] must provide an [area] (only 'known-issue' and 'security' entries can omit this");
            }
        }

        if (type.equals("breaking") || type.equals("breaking-java")) {
            if (entry.getBreaking() == null) {
                throw new GradleException(
                    "[" + path + "] has type [" + type + "] and must supply a [breaking] section with further information"
                );
            }

            if (entry.getBreaking().getDetails().contains(TRIPLE_BACKTICK)) {
                throw new GradleException(CODE_BLOCK_ERROR.formatted(path, "breaking.details"));
            }
            if (entry.getBreaking().getImpact().contains(TRIPLE_BACKTICK)) {
                throw new GradleException(CODE_BLOCK_ERROR.formatted(path, "breaking.impact"));
            }
        }

        if (type.equals("deprecation")) {
            if (entry.getDeprecation() == null) {
                throw new GradleException(
                    "[" + path + "] has type [deprecation] and must supply a [deprecation] section with further information"
                );
            }

            if (entry.getDeprecation().getDetails().contains(TRIPLE_BACKTICK)) {
                throw new GradleException(CODE_BLOCK_ERROR.formatted(path, "deprecation.details"));
            }
            if (entry.getDeprecation().getImpact().contains(TRIPLE_BACKTICK)) {
                throw new GradleException(CODE_BLOCK_ERROR.formatted(path, "deprecation.impact"));
            }
        }

        if (entry.getHighlight() != null && entry.getHighlight().getBody().contains(TRIPLE_BACKTICK)) {
            throw new GradleException(CODE_BLOCK_ERROR.formatted(path, "highlight.body"));
        }
    }

    @InputFiles
    public FileCollection getChangelogs() {
        return changelogs;
    }

    public void setChangelogs(FileCollection files) {
        this.changelogs.setFrom(files);
    }
}
