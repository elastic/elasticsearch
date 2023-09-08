/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.vagrant;

import org.elasticsearch.gradle.ReaperPlugin;
import org.elasticsearch.gradle.internal.InternalReaperPlugin;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.execution.TaskActionListener;
import org.gradle.api.execution.TaskExecutionListener;
import org.gradle.api.tasks.TaskState;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class VagrantBasePlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        project.getRootProject().getPluginManager().apply(VagrantSetupCheckerPlugin.class);
        project.getRootProject().getPluginManager().apply(VagrantManagerPlugin.class);
        project.getRootProject().getPluginManager().apply(InternalReaperPlugin.class);

        var reaperServiceProvider = GradleUtils.getBuildService(project.getGradle().getSharedServices(), ReaperPlugin.REAPER_SERVICE_NAME);
        var extension = project.getExtensions().create("vagrant", VagrantExtension.class, project);
        var service = project.getExtensions().create("vagrantService", VagrantMachine.class, extension, reaperServiceProvider);

        project.getGradle()
            .getTaskGraph()
            .whenReady(
                graph -> service.refs = graph.getAllTasks()
                    .stream()
                    .filter(t -> t instanceof VagrantShellTask)
                    .filter(t -> t.getProject() == project)
                    .count()
            );
    }

    /**
     * Check vagrant and virtualbox versions, if any vagrant test tasks will be run.
     */
    static class VagrantSetupCheckerPlugin implements Plugin<Project> {

        private static final Pattern VAGRANT_VERSION = Pattern.compile("Vagrant (\\d+\\.\\d+\\.\\d+)");
        private static final Pattern VIRTUAL_BOX_VERSION = Pattern.compile("(\\d+\\.\\d+)");

        @Override
        public void apply(Project project) {
            if (project != project.getRootProject()) {
                throw new IllegalArgumentException("VagrantSetupCheckerPlugin can only be applied to the root project of a build");
            }

            project.getGradle().getTaskGraph().whenReady(graph -> {
                boolean needsVagrant = graph.getAllTasks().stream().anyMatch(t -> t instanceof VagrantShellTask);
                if (needsVagrant) {
                    checkVersion(project, "vagrant", VAGRANT_VERSION, 1, 8, 6);
                    checkVersion(project, "vboxmanage", VIRTUAL_BOX_VERSION, 5, 1);
                }
            });
        }

        void checkVersion(Project project, String tool, Pattern versionRegex, int... minVersion) {
            ByteArrayOutputStream pipe = new ByteArrayOutputStream();
            project.exec(spec -> {
                spec.setCommandLine(tool, "--version");
                spec.setStandardOutput(pipe);
            });
            String output = pipe.toString(StandardCharsets.UTF_8).trim();
            Matcher matcher = versionRegex.matcher(output);
            if (matcher.find() == false) {
                throw new IllegalStateException(
                    tool + " version output [" + output + "] did not match regex [" + versionRegex.pattern() + "]"
                );
            }

            String version = matcher.group(1);
            List<Integer> versionParts = Stream.of(version.split("\\.")).map(Integer::parseInt).toList();
            for (int i = 0; i < minVersion.length; ++i) {
                int found = versionParts.get(i);
                if (found > minVersion[i]) {
                    break; // most significant version is good
                } else if (found < minVersion[i]) {
                    final String exceptionMessage = String.format(
                        Locale.ROOT,
                        "Unsupported version of %s. Found [%s], expected [%s+]",
                        tool,
                        version,
                        Stream.of(minVersion).map(String::valueOf).collect(Collectors.joining("."))
                    );

                    throw new IllegalStateException(exceptionMessage);
                } // else equal, so check next element
            }
        }
    }

    /**
     * Adds global hooks to manage destroying, starting and updating VMs.
     */
    static class VagrantManagerPlugin implements Plugin<Project>, TaskActionListener, TaskExecutionListener {

        @Override
        public void apply(Project project) {
            if (project != project.getRootProject()) {
                throw new IllegalArgumentException("VagrantManagerPlugin can only be applied to the root project of a build");
            }
            project.getGradle().addListener(this);
        }

        private void callIfVagrantTask(Task task, Consumer<VagrantMachine> method) {
            if (task instanceof VagrantShellTask) {
                VagrantMachine service = task.getProject().getExtensions().getByType(VagrantMachine.class);
                method.accept(service);
            }
        }

        @Override
        public void beforeExecute(Task task) { /* nothing to do */}

        @Override
        public void afterActions(Task task) { /* nothing to do */ }

        @Override
        public void beforeActions(Task task) {
            callIfVagrantTask(task, VagrantMachine::maybeStartVM);
        }

        @Override
        public void afterExecute(Task task, TaskState state) {
            callIfVagrantTask(task, service -> service.maybeStopVM(state.getFailure() != null));
        }
    }

}
