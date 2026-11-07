/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.run;

import org.gradle.api.DefaultTask;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.UntrackedTask;
import org.gradle.api.tasks.options.Option;

/**
 * Entry point for running several clusters at once (e.g. {@code run-ccs}). It depends on one {@link RunTask} per cluster, which
 * run concurrently. Member tasks pick up this task's command line options via {@link RunTask#inheritOptionsFrom(TaskProvider)}.
 */
@UntrackedTask(because = "Lifecycle task for running clusters")
public abstract class RunLifecycleTask extends DefaultTask {

    public RunLifecycleTask() {
        getDebugJvm().convention(false);
        getDebugCliJvm().convention(false);
        getPreserveData().convention(false);
    }

    @Option(option = "debug-jvm", description = "Connect each node to a listening debugger")
    @Internal
    public abstract Property<Boolean> getDebugJvm();

    @Option(option = "debug-cli-jvm", description = "Connect each node's CLI launcher to a listening debugger")
    @Internal
    public abstract Property<Boolean> getDebugCliJvm();

    @Option(option = "preserve-data", description = "Preserves data directory contents between runs")
    @Internal
    public abstract Property<Boolean> getPreserveData();
}
