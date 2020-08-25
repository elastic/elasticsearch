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

import org.elasticsearch.gradle.LoggedExec;
import org.elasticsearch.gradle.VersionProperties;
import org.gradle.api.Action;
import org.gradle.api.GradleException;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.plugins.BasePlugin;
import org.gradle.api.specs.Spec;
import org.gradle.api.tasks.TaskProvider;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

public class InternalDistributionSanityCheckPlugin implements Plugin<Project> {
    @Override
    public void apply(Project project) {
        project.getPlugins().apply(BasePlugin.class);
        String buildTaskName = calculateBuildTask(project.getName());
        Task buildDistTask = project.getParent().getTasks().getByName(buildTaskName);
        File archiveExtractionDir;
        if (project.getName().contains("tar")) {
            archiveExtractionDir = new File(project.getBuildDir(), "tar-extracted");
        } else {
            if (!project.getName().contains("zip")) {
                throw new GradleException("Expecting project name containing 'zip' or 'tar'.");
            }
            archiveExtractionDir = new File(project.getBuildDir(), "zip-extracted");
        }
        Spec<Task> toolAvailableSpec = task -> toolExists(project);

        // sanity checks if archives can be extracted
        TaskProvider<LoggedExec> checkExtraction = registerCheckExtractionTask(
            project,
            buildDistTask,
            archiveExtractionDir,
            toolAvailableSpec
        );
        TaskProvider<Task> checkLicense = registerCheckLicenseTask(
            project,
            buildDistTask,
            archiveExtractionDir,
            toolAvailableSpec,
            checkExtraction
        );

        TaskProvider<Task> checkNotice = registerCheckNoticeTask(
            project,
            buildDistTask,
            archiveExtractionDir,
            toolAvailableSpec,
            checkExtraction
        );
        TaskProvider<Task> checkTask = project.getTasks().named("check");
        checkTask.configure(task -> {
            task.dependsOn(checkExtraction);
            task.dependsOn(checkLicense);
            task.dependsOn(checkNotice);
        });

        if (project.getName().contains("zip") || project.getName().contains("tar")) {
            project.getExtensions().add("licenseName", "Elastic License");
            project.getExtensions().add("licenseUrl", project.getExtensions().getExtraProperties().get("elasticLicenseUrl"));
            TaskProvider<Task> checkMlCppNoticeTask = registerCheckMlCppNoticeTask(
                project,
                buildDistTask,
                archiveExtractionDir,
                toolAvailableSpec,
                checkExtraction
            );
            checkTask.configure(task -> task.dependsOn(checkMlCppNoticeTask));
        }
    }

    private static TaskProvider<Task> registerCheckMlCppNoticeTask(
        Project project,
        Task buildDistTask,
        File archiveExtractionDir,
        Spec<Task> toolAvailableSpec,
        TaskProvider<LoggedExec> checkExtraction
    ) {
        TaskProvider<Task> checkMlCppNoticeTask = project.getTasks().register("checkMlCppNotice", task -> {
            task.dependsOn(buildDistTask, checkExtraction);
            task.onlyIf(toolAvailableSpec);
            task.doLast(new Action<Task>() {
                @Override
                public void execute(Task task) {
                    // this is just a small sample from the C++ notices,
                    // the idea being that if we've added these lines we've probably added all the required lines
                    final List<String> expectedLines = Arrays.asList(
                        "Apache log4cxx",
                        "Boost Software License - Version 1.0 - August 17th, 2003"
                    );
                    final Path noticePath = archiveExtractionDir.toPath()
                        .resolve("elasticsearch-" + VersionProperties.getElasticsearch() + "/modules/x-pack-ml/NOTICE.txt");
                    final List<String> actualLines;
                    try {
                        actualLines = Files.readAllLines(noticePath);
                        for (final String expectedLine : expectedLines) {
                            if (actualLines.contains(expectedLine) == false) {
                                throw new GradleException("expected [" + noticePath + " to contain [" + expectedLine + "] but it did not");
                            }
                        }
                    } catch (IOException ioException) {
                        ioException.printStackTrace();
                    }
                }
            });
        });
        return checkMlCppNoticeTask;
    }

    private TaskProvider<Task> registerCheckNoticeTask(
        Project project,
        Task buildDistTask,
        File archiveExtractionDir,
        Spec<Task> toolAvailableSpec,
        TaskProvider<LoggedExec> checkExtraction
    ) {
        return project.getTasks().register("checkNotice", task -> {
            task.dependsOn(buildDistTask, checkExtraction);
            task.onlyIf(toolAvailableSpec);
            task.doLast(new Action<Task>() {
                @Override
                public void execute(Task task) {
                    final List<String> noticeLines = Arrays.asList("Elasticsearch", "Copyright 2009-2018 Elasticsearch");
                    final Path noticePath = archiveExtractionDir.toPath()
                        .resolve("elasticsearch-" + VersionProperties.getElasticsearch() + "/NOTICE.txt");
                    assertLinesInFile(noticePath, noticeLines);
                }
            });
        });
    }

    private TaskProvider<Task> registerCheckLicenseTask(
        Project project,
        Task buildDistTask,
        File archiveExtractionDir,
        Spec<Task> toolAvailableSpec,
        TaskProvider<LoggedExec> checkExtraction
    ) {
        TaskProvider<Task> checkLicense = project.getTasks().register("checkLicense", task -> {
            task.dependsOn(buildDistTask, checkExtraction);
            task.onlyIf(toolAvailableSpec);
            task.doLast(new Action<Task>() {
                @Override
                public void execute(Task task) {
                    String licenseFilename = null;
                    if (project.getName().contains("oss-") || project.getName().equals("integ-test-zip")) {
                        licenseFilename = "APACHE-LICENSE-2.0.txt";
                    } else {
                        licenseFilename = "ELASTIC-LICENSE.txt";
                    }
                    final List<String> licenseLines;
                    try {
                        licenseLines = Files.readAllLines(project.getRootDir().toPath().resolve("licenses/" + licenseFilename));
                        final Path licensePath = archiveExtractionDir.toPath()
                            .resolve("elasticsearch-" + VersionProperties.getElasticsearch() + "/LICENSE.txt");
                        assertLinesInFile(licensePath, licenseLines);
                    } catch (IOException ioException) {
                        ioException.printStackTrace();
                    }
                }
            });
        });
        return checkLicense;
    }

    private TaskProvider<LoggedExec> registerCheckExtractionTask(
        Project project,
        Task buildDistTask,
        File archiveExtractionDir,
        Spec<Task> toolAvailableSpec
    ) {
        return project.getTasks().register("checkExtraction", LoggedExec.class, t -> {
            t.onlyIf(toolAvailableSpec);
            t.setCommandLine(
                project.getName().contains("tar")
                    ? Arrays.asList("tar", "-xvzf", distTaskOutput(buildDistTask), "-C", archiveExtractionDir)
                    : Arrays.asList("unzip", distTaskOutput(buildDistTask), "-d", archiveExtractionDir)
            );

            t.dependsOn(buildDistTask);
            t.doFirst(new Action<Task>() {
                @Override
                public void execute(Task task) {
                    project.delete(archiveExtractionDir);
                    archiveExtractionDir.mkdirs();
                }
            });

            // common sanity checks on extracted archive directly as part of checkExtraction
            t.doLast(new Action<Task>() {
                @Override
                public void execute(Task task) {
                    // check no plain class files are packaged
                    try {
                        Files.walkFileTree(archiveExtractionDir.toPath(), new SimpleFileVisitor<>() {
                            @Override
                            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                                assertNoClassFile(file);
                                return FileVisitResult.CONTINUE;
                            }
                        });
                    } catch (IOException ioException) {
                        ioException.printStackTrace();
                    }
                }
            });
        });
    }

    private static void assertLinesInFile(Path path, List<String> expectedLines) {
        try {
            final List<String> actualLines = Files.readAllLines(path);
            int line = 0;
            for (final String expectedLine : expectedLines) {
                final String actualLine = actualLines.get(line);
                if (expectedLine.equals(actualLine) == false) {
                    throw new GradleException(
                        "expected line [" + (line + 1) + "] in [" + path + "] to be [" + expectedLine + "] but was [" + actualLine + "]"
                    );
                }
                line++;
            }
        } catch (IOException ioException) {
            throw new GradleException("Unable to read from file " + path, ioException);
        }
    }

    private static boolean toolExists(Project project) {
        if (project.getName().contains("tar")) {
            return tarExists();
        } else {
            assert project.getName().contains("zip");
            return zipExists();
        }
    }

    private static void assertNoClassFile(Path path) {
        if (path.toFile().getName().endsWith(".class")) {
            throw new GradleException("Detected class file in distribution ('" + path.getFileName() + "')");
        }
    }

    private static boolean zipExists() {
        return new File("/bin/unzip").exists() || new File("/usr/bin/unzip").exists() || new File("/usr/local/bin/unzip").exists();
    }

    private static boolean tarExists() {
        return new File("/bin/tar").exists() || new File("/usr/bin/tar").exists() || new File("/usr/local/bin/tar").exists();
    }

    private Object distTaskOutput(Task buildDistTask) {
        return new Callable<String>() {
            @Override
            public String call() {
                return buildDistTask.getOutputs().getFiles().getSingleFile().getAbsolutePath();
            }

            @Override
            public String toString() {
                return call();
            }
        };
    }

    private String calculateBuildTask(String projectName) {
        return "build" + Arrays.stream(projectName.split("-")).map(f -> capitalize(f)).collect(Collectors.joining());
    }

    private static String capitalize(String str) {
        if (str == null) return str;
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }
}
