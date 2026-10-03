/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.internal.nativelibs;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.testing.Test;
import org.gradle.language.base.plugins.LifecycleBasePlugin;

/**
 * Registers {@link VerifyNativeLibraryLinuxAbiTask} on {@code :check} for native library projects.
 *
 * <p>Apply plugin id {@code elasticsearch.native-libraries-linux-abi} and point the task at the
 * extracted {@code build/platform/} tree (see {@code :libs:native:libraries}).
 *
 * <p>Verification runs only on Linux build hosts where {@code objdump} is meaningful; the task is
 * skipped on macOS and Windows.
 */
public class NativeLibrariesLinuxAbiPlugin implements Plugin<Project> {

    /** Default maximum glibc (RHEL 8). */
    public static final String DEFAULT_MAX_GLIBC_VERSION = "2.28";
    /** Default maximum libstdc++ {@code GLIBCXX} (RHEL 8). */
    public static final String DEFAULT_MAX_GLIBCXX_VERSION = "3.4.25";

    /** Name of the verification task registered by this plugin. */
    public static final String VERIFY_TASK = "verifyNativeLibrariesLinuxAbi";

    /** Registers {@link #VERIFY_TASK} and attaches it to the project {@code check} lifecycle. */
    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(LifecycleBasePlugin.class);

        TaskProvider<VerifyNativeLibraryLinuxAbiTask> verifyTask = project.getTasks()
            .register(VERIFY_TASK, VerifyNativeLibraryLinuxAbiTask.class, task -> {
                task.setGroup(LifecycleBasePlugin.VERIFICATION_GROUP);
                task.setDescription("Verifies Linux native libraries meet the minimum supported ABI (RHEL 8)");
                task.getMaxGlibcVersion().set(DEFAULT_MAX_GLIBC_VERSION);
                task.getMaxGlibcxxVersion().set(DEFAULT_MAX_GLIBCXX_VERSION);
                task.getResultMarker().set(project.getLayout().getBuildDirectory().file("markers/verify-native-libraries-linux-abi.ok"));
                task.onlyIf("Linux host OS required for native ABI verification", t -> LinuxBuildHost.isLinux());
            });

        project.getTasks().named(LifecycleBasePlugin.CHECK_TASK_NAME).configure(check -> check.dependsOn(verifyTask));
        project.getTasks().withType(Test.class).configureEach(test -> test.mustRunAfter(verifyTask));
    }
}
