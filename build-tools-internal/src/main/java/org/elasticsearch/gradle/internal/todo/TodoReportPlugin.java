/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License, v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.todo;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.file.FileCollection;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.specs.Spec;
import org.gradle.api.tasks.TaskProvider;

import java.io.File;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.List;

/**
 * Registers {@code todoReport} on a Java project. The task scans that project's Java sources for
 * to-do and fix-me comments and writes a JSON report the root build merges for dashboards.
 */
public class TodoReportPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        TodoReportExtension extension = project.getExtensions().create(TodoReportExtension.NAME, TodoReportExtension.class);
        extension.getMarkers().convention(List.of("TODO", "FIXME"));
        project.getPluginManager().withPlugin("java-base", applied -> register(project, extension));
    }

    private static void register(Project project, TodoReportExtension extension) {
        TaskProvider<TodoReportTask> task = project.getTasks().register("todoReport", TodoReportTask.class, t -> {
            t.setGroup("reporting");
            t.setDescription("Write this module's TODO and FIXME comments to a JSON report");
            t.getModulePath().set(project.getPath());
            t.getProjectDirectory().set(project.getLayout().getProjectDirectory().getAsFile().getAbsolutePath());
            t.getSettingsDirectory().set(project.getLayout().getSettingsDirectory().getAsFile().getAbsolutePath());
            t.getMarkers().set(extension.getMarkers());
            t.getOutputFile().set(project.getLayout().getBuildDirectory().file("reports/todo-report/todos.json"));
        });
        File buildDirectory = project.getLayout().getBuildDirectory().get().getAsFile();
        OutsideBuildDirectory included = new OutsideBuildDirectory(buildDirectory);
        JavaPluginExtension java = project.getExtensions().getByType(JavaPluginExtension.class);
        java.getSourceSets().configureEach(sourceSet -> task.configure(t -> {
            FileCollection sources = sourceSet.getAllJava().filter(included);
            t.getSourceFiles().from(sources);
        }));
    }

    /**
     * Drops generated sources under the project build directory. A concrete spec serializes with the
     * configuration cache, which a lambda capturing the project would not.
     */
    private static final class OutsideBuildDirectory implements Spec<File>, Serializable {

        private final String buildDirectory;

        OutsideBuildDirectory(File buildDirectory) {
            this.buildDirectory = buildDirectory.getAbsolutePath();
        }

        @Override
        public boolean isSatisfiedBy(File file) {
            Path root = Path.of(buildDirectory).toAbsolutePath().normalize();
            Path candidate = file.toPath().toAbsolutePath().normalize();
            return candidate.startsWith(root) == false;
        }
    }
}
