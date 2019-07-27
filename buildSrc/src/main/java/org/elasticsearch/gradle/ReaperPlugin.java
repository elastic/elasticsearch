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

package org.elasticsearch.gradle;

import org.gradle.BuildListener;
import org.gradle.BuildResult;
import org.gradle.api.GradleException;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.initialization.Settings;
import org.gradle.api.invocation.Gradle;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * A plugin to handle reaping external services spawned by a build if Gradle dies.
 */
public class ReaperPlugin implements Plugin<Project> {

    private Project project;
    private Path baseDirectory;
    private volatile Process reaperProcess;

    @Override
    public void apply(Project project) {
        this.project = project;
        this.baseDirectory = project.getRootDir().toPath().resolve(".gradle")
            .resolve("reaper").resolve("build-" + ProcessHandle.current().pid());
        project.getGradle().addListener(new BuildListener() {
            @Override
            public void buildStarted(Gradle gradle) {}

            @Override
            public void settingsEvaluated(Settings settings) {}

            @Override
            public void projectsLoaded(Gradle gradle) {}

            @Override
            public void projectsEvaluated(Gradle gradle) {}

            @Override
            public void buildFinished(BuildResult result) {
                if (reaperProcess != null) {
                    try {
                        reaperProcess.getOutputStream().close();
                        project.getLogger().info("Waiting for reaper to exit normally");
                        if (reaperProcess.waitFor() != 0) {
                            throw new GradleException("Reaper process failed. Check log at "
                                + baseDirectory.resolve("error.log") + " for details");
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }

                }
            }
        });
    }

    /**
     * Register a pid that will be killed by the reaper.
     */
    public void registerPid(String serviceId, long pid) {
        String[] killPidCommand = OS.<String[]>conditional()
            .onWindows(() -> new String[]{"Taskill", "/F", "/PID", String.valueOf(pid)})
            .onUnix(() -> new String[]{"kill", "-9", String.valueOf(pid)}).supply();
        registerCommand(serviceId, killPidCommand);
    }

    /**
     * Register a system command that will be run by the reaper.
     */
    public void registerCommand(String serviceId, String... command) {
        ensureReaperStarted();

        try {
            Files.writeString(baseDirectory.resolve(serviceId), String.join(" ", command));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void unregister(String serviceId) {
        try {
            Files.deleteIfExists(baseDirectory.resolve(serviceId));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private synchronized void ensureReaperStarted() {
        if (reaperProcess == null) {
            try {
                // copy the reaper jar
                Path jarPath = project.getBuildDir().toPath().resolve("reaper").resolve("reaper.jar");
                Files.createDirectories(jarPath.getParent());
                InputStream jarInput = ReaperPlugin.class.getResourceAsStream("/META-INF/reaper.jar");
                try (OutputStream out = Files.newOutputStream(jarPath)) {
                    jarInput.transferTo(out);
                }

                // ensure the input directory exists
                Files.createDirectories(baseDirectory);

                // start the reaper
                ProcessBuilder builder = new ProcessBuilder(
                    "java", "-cp", jarPath.toString(), "org.elasticsearch.gradle.reaper.Reaper", baseDirectory.toString());
                File logFile = baseDirectory.resolve("reaper.log").toFile();
                builder.redirectOutput(logFile);
                builder.redirectError(logFile);
                reaperProcess = builder.start();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
