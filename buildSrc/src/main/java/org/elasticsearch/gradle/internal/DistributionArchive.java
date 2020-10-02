/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gradle.internal;

import org.gradle.api.Named;
import org.gradle.api.file.CopySpec;
import org.gradle.api.tasks.Copy;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.bundling.AbstractArchiveTask;

import java.util.function.Supplier;

public class DistributionArchive implements Named {

    private TaskProvider<? extends AbstractArchiveTask> archiveTask;
    private TaskProvider<Copy> expandedDistTask;
    private final String name;

    public DistributionArchive(TaskProvider<? extends AbstractArchiveTask> archiveTask, TaskProvider<Copy> expandedDistTask, String name) {
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

    public TaskProvider<Copy> getExpandedDistTask() {
        return expandedDistTask;
    }
}
