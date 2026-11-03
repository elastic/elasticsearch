/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.ci;

import org.gradle.api.DefaultTask;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Locale;

/**
 * Regenerates the checked-in CI part assignment from the planner config and the duration snapshot. The
 * output is source-controlled, so the task is deliberately not cacheable.
 */
public abstract class GenerateCiPartAssignmentTask extends DefaultTask {

    @InputFile
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract RegularFileProperty getConfigFile();

    @InputFile
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract RegularFileProperty getDurationsFile();

    @OutputFile
    public abstract RegularFileProperty getAssignmentFile();

    @TaskAction
    public void generate() throws IOException {
        CiPartPlanner.Config config = CiPartPlanner.parseConfig(Files.readString(getConfigFile().get().getAsFile().toPath()));
        CiPartPlanner.Durations durations = CiPartPlanner.parseDurations(Files.readString(getDurationsFile().get().getAsFile().toPath()));
        CiPartAssignment assignment = CiPartPlanner.plan(config, durations);
        Files.writeString(getAssignmentFile().get().getAsFile().toPath(), assignment.toJson());
        double[] minutes = CiPartPlanner.estimatedMinutes(assignment, config, durations);
        for (int i = 0; i < minutes.length; i++) {
            getLogger().lifecycle(String.format(Locale.ROOT, "part %d: ~%.1f min of tests (planner model)", i + 1, minutes[i]));
        }
    }
}
