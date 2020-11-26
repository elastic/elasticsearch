/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gradle.vagrant;

import org.elasticsearch.gradle.ReaperPlugin;
import org.elasticsearch.gradle.ReaperService;
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
        project.getRootProject().getPluginManager().apply(ReaperPlugin.class);

        ReaperService reaper = project.getRootProject().getExtensions().getByType(ReaperService.class);
        VagrantExtension extension = project.getExtensions().create("vagrant", VagrantExtension.class, project);
        VagrantMachine service = project.getExtensions().create("vagrantService", VagrantMachine.class, project, extension, reaper);

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
            List<Integer> versionParts = Stream.of(version.split("\\.")).map(Integer::parseInt).collect(Collectors.toList());
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
