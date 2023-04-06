/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.internal.conventions.GUtils;
import org.elasticsearch.gradle.internal.conventions.LicensingPlugin;
import org.gradle.api.Action;
import org.gradle.api.GradleException;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.file.ArchiveOperations;
import org.gradle.api.plugins.BasePlugin;
import org.gradle.api.tasks.Copy;
import org.gradle.api.tasks.TaskProvider;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import javax.inject.Inject;

public class InternalDistributionArchiveCheckPlugin implements Plugin<Project> {

    private ArchiveOperations archiveOperations;

    @Inject
    public InternalDistributionArchiveCheckPlugin(ArchiveOperations archiveOperations) {
        this.archiveOperations = archiveOperations;
    }

    @Override
    public void apply(Project project) {
        project.getPlugins().apply(BasePlugin.class);
        project.getPlugins().apply(LicensingPlugin.class);
        String buildTaskName = calculateBuildTask(project.getName());
        TaskProvider<Task> buildDistTask = project.getParent().getTasks().named(buildTaskName);
        DistributionArchiveCheckExtension distributionArchiveCheckExtension = project.getExtensions()
            .create("distributionArchiveCheck", DistributionArchiveCheckExtension.class);

        File archiveExtractionDir = calculateArchiveExtractionDir(project);

        // sanity checks if archives can be extracted
        TaskProvider<Copy> checkExtraction = registerCheckExtractionTask(project, buildDistTask, archiveExtractionDir);
        TaskProvider<Task> checkLicense = registerCheckLicenseTask(project, checkExtraction);
        TaskProvider<Task> checkNotice = registerCheckNoticeTask(project, checkExtraction);
        TaskProvider<Task> checkModulesTask = InternalDistributionModuleCheckTaskProvider.registerCheckModulesTask(
            project,
            checkExtraction
        );
        TaskProvider<Task> checkTask = project.getTasks().named("check");
        checkTask.configure(task -> {
            task.dependsOn(checkExtraction);
            task.dependsOn(checkLicense);
            task.dependsOn(checkNotice);
            task.dependsOn(checkModulesTask);
        });

        String projectName = project.getName();
        if (projectName.equalsIgnoreCase("integ-test-zip") == false && (projectName.contains("zip") || projectName.contains("tar"))) {
            project.getExtensions()
                .add(
                    "projectLicenses",
                    Map.of("Elastic License 2.0", project.getExtensions().getExtraProperties().get("elasticLicenseUrl"))
                );
            TaskProvider<Task> checkMlCppNoticeTask = registerCheckMlCppNoticeTask(
                project,
                checkExtraction,
                distributionArchiveCheckExtension
            );
            checkTask.configure(task -> task.dependsOn(checkMlCppNoticeTask));
        }
    }

    private File calculateArchiveExtractionDir(Project project) {
        if (project.getName().contains("tar")) {
            return new File(project.getBuildDir(), "tar-extracted");
        }
        if (project.getName().contains("zip") == false) {
            throw new GradleException("Expecting project name containing 'zip' or 'tar'.");
        }
        return new File(project.getBuildDir(), "zip-extracted");
    }

    private static TaskProvider<Task> registerCheckMlCppNoticeTask(
        Project project,
        TaskProvider<Copy> checkExtraction,
        DistributionArchiveCheckExtension extension
    ) {
        TaskProvider<Task> checkMlCppNoticeTask = project.getTasks().register("checkMlCppNotice", task -> {
            task.dependsOn(checkExtraction);
            task.doLast(new Action<Task>() {
                @Override
                public void execute(Task task) {
                    // this is just a small sample from the C++ notices,
                    // the idea being that if we've added these lines we've probably added all the required lines
                    final List<String> expectedLines = extension.expectedMlLicenses.get();
                    final Path noticePath = checkExtraction.get()
                        .getDestinationDir()
                        .toPath()
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

    private TaskProvider<Task> registerCheckNoticeTask(Project project, TaskProvider<Copy> checkExtraction) {
        return project.getTasks().register("checkNotice", task -> {
            task.dependsOn(checkExtraction);
            task.doLast(new Action<Task>() {
                @Override
                public void execute(Task task) {
                    final List<String> noticeLines = Arrays.asList("Elasticsearch", "Copyright 2009-2021 Elasticsearch");
                    final Path noticePath = checkExtraction.get()
                        .getDestinationDir()
                        .toPath()
                        .resolve("elasticsearch-" + VersionProperties.getElasticsearch() + "/NOTICE.txt");
                    assertLinesInFile(noticePath, noticeLines);
                }
            });
        });
    }

    private TaskProvider<Task> registerCheckLicenseTask(Project project, TaskProvider<Copy> checkExtraction) {
        TaskProvider<Task> checkLicense = project.getTasks().register("checkLicense", task -> {
            task.dependsOn(checkExtraction);
            task.doLast(new Action<Task>() {
                @Override
                public void execute(Task task) {
                    String licenseFilename = null;
                    if (project.getName().contains("oss-") || project.getName().equals("integ-test-zip")) {
                        licenseFilename = "SSPL-1.0+ELASTIC-LICENSE-2.0.txt";
                    } else {
                        licenseFilename = "ELASTIC-LICENSE-2.0.txt";
                    }
                    final List<String> licenseLines;
                    try {
                        licenseLines = Files.readAllLines(project.getRootDir().toPath().resolve("licenses/" + licenseFilename));
                        final Path licensePath = checkExtraction.get()
                            .getDestinationDir()
                            .toPath()
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

    private TaskProvider<Copy> registerCheckExtractionTask(Project project, TaskProvider<Task> buildDistTask, File archiveExtractionDir) {
        return project.getTasks().register("checkExtraction", Copy.class, t -> {
            t.dependsOn(buildDistTask);
            if (project.getName().contains("tar")) {
                t.from(archiveOperations.tarTree(distTaskOutput(buildDistTask)));
            } else {
                t.from(archiveOperations.zipTree(distTaskOutput(buildDistTask)));
            }
            t.into(archiveExtractionDir);
            // common sanity checks on extracted archive directly as part of checkExtraction
            t.eachFile(fileCopyDetails -> assertNoClassFile(fileCopyDetails.getFile()));
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

    private static void assertNoClassFile(File file) {
        if (file.getName().endsWith(".class")) {
            throw new GradleException("Detected class file in distribution ('" + file.getName() + "')");
        }
    }

    private static boolean zipExists() {
        return new File("/bin/unzip").exists() || new File("/usr/bin/unzip").exists() || new File("/usr/local/bin/unzip").exists();
    }

    private static boolean tarExists() {
        return new File("/bin/tar").exists() || new File("/usr/bin/tar").exists() || new File("/usr/local/bin/tar").exists();
    }

    private Object distTaskOutput(TaskProvider<Task> buildDistTask) {
        return new Callable<File>() {
            @Override
            public File call() {
                return buildDistTask.get().getOutputs().getFiles().getSingleFile();
            }

            @Override
            public String toString() {
                return call().getAbsolutePath();
            }
        };
    }

    private String calculateBuildTask(String projectName) {
        return "build" + Arrays.stream(projectName.split("-")).map(f -> GUtils.capitalize(f)).collect(Collectors.joining());
    }

}
