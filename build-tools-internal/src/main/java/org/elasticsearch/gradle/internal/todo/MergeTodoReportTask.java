/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License, v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.todo;

import org.gradle.api.DefaultTask;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.options.Option;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Merges per-module to-do reports into one JSON file and one CSV file for dashboards.
 */
public abstract class MergeTodoReportTask extends DefaultTask {

    private static final Logger logger = Logging.getLogger(MergeTodoReportTask.class);

    /** JSON reports written by each module's {@code todoReport} task. */
    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract ConfigurableFileCollection getReportFiles();

    /** Merged JSON report, a {@code { "items": [ ... ] }} document. */
    @OutputFile
    public abstract RegularFileProperty getOutputJson();

    /** Merged CSV report with the same rows as {@link #getOutputJson()}. */
    @OutputFile
    public abstract RegularFileProperty getOutputCsv();

    /**
     * Accepted on the root task so {@code :todoReport --include-untracked} is a valid command.
     * Module tasks use this value unless they were given the option themselves.
     */
    @Input
    @Option(option = "include-untracked", description = "Include comments in files git does not track")
    public abstract Property<Boolean> getIncludeUntracked();

    @TaskAction
    public void merge() {
        List<Path> reports = new ArrayList<>();
        for (File report : getReportFiles()) {
            reports.add(report.toPath());
        }
        List<TodoEntry> items = TodoReportWriter.merge(reports);
        Path json = getOutputJson().get().getAsFile().toPath();
        TodoReportWriter.writeJson(json, items);
        TodoReportWriter.writeCsv(getOutputCsv().get().getAsFile().toPath(), items);
        long todos = items.stream().filter(item -> item.marker().equals("TODO")).count();
        long fixmes = items.stream().filter(item -> item.marker().equals("FIXME")).count();
        logger.lifecycle("Found {} TODO and {} FIXME comments. Report: {}", todos, fixmes, json);
    }
}
