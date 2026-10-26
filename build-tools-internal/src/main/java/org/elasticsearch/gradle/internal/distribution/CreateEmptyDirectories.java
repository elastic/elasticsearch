/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.distribution;

import org.gradle.api.Action;
import org.gradle.api.GradleException;
import org.gradle.api.Task;
import org.gradle.api.tasks.Copy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * A {@link Copy} task action that creates empty directories in the task's destination directory, e.g. the directories
 * an OS package owns but ships without content, such as {@code /var/log/elasticsearch}.
 *
 * <p>A {@code Copy} task cannot produce empty directories without a source for them. Build scripts used to create them
 * in a {@code doLast} closure calling the script's {@code mkdir()}, which references the build script at execution time
 * and therefore fails when the task runs from a reused configuration cache entry. This action only holds the relative
 * paths, so it is safe to serialize into the configuration cache.
 */
public final class CreateEmptyDirectories implements Action<Task> {

    private final List<String> relativePaths;

    /**
     * @param relativePaths the directories to create, relative to the destination directory of the {@link Copy} task
     */
    public CreateEmptyDirectories(List<String> relativePaths) {
        this.relativePaths = List.copyOf(relativePaths);
    }

    @Override
    public void execute(Task task) {
        if (task instanceof Copy copy) {
            Path destination = copy.getDestinationDir().toPath();
            for (String relativePath : relativePaths) {
                try {
                    Files.createDirectories(destination.resolve(relativePath));
                } catch (IOException e) {
                    throw new UncheckedIOException("Failed to create directory [" + relativePath + "] in [" + destination + "]", e);
                }
            }
            return;
        }
        throw new GradleException("Task [" + task.getPath() + "] must be a Copy task to create empty directories in");
    }
}
