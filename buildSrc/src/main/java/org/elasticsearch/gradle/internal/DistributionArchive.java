/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.gradle.api.Named;
import org.gradle.api.file.CopySpec;
import org.gradle.api.tasks.Sync;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.bundling.AbstractArchiveTask;

import java.util.function.Supplier;

public class DistributionArchive implements Named {

    private TaskProvider<? extends AbstractArchiveTask> archiveTask;
    private TaskProvider<Sync> expandedDistTask;
    private final String name;

    public DistributionArchive(TaskProvider<? extends AbstractArchiveTask> archiveTask, TaskProvider<Sync> expandedDistTask, String name) {
        this.archiveTask = archiveTask;
        this.expandedDistTask = expandedDistTask;
        this.name = name;
    }

    public void setArchiveClassifier(String classifier) {
        this.archiveTask.configure(abstractArchiveTask -> abstractArchiveTask.getArchiveClassifier().set(classifier));
    }

    public void content(Supplier<CopySpec> p) {
        this.archiveTask.configure(t -> t.with(p.get()));
        this.expandedDistTask.configure(t -> t.with(p.get()));
    }

    @Override
    public String getName() {
        return name;
    }

    public TaskProvider<? extends AbstractArchiveTask> getArchiveTask() {
        return archiveTask;
    }

    public TaskProvider<Sync> getExpandedDistTask() {
        return expandedDistTask;
    }
}
