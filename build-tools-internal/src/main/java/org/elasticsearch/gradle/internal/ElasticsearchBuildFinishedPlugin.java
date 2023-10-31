/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.file.ConfigurableFileTree;
import org.gradle.api.file.FileSystemOperations;
import org.gradle.api.flow.FlowAction;
import org.gradle.api.flow.FlowParameters;
import org.gradle.api.flow.FlowProviders;
import org.gradle.api.flow.FlowScope;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.tasks.Input;
import org.gradle.api.provider.Property;
import org.jetbrains.annotations.NotNull;

import javax.inject.Inject;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public abstract class ElasticsearchBuildFinishedPlugin implements Plugin<Project> {

    @Inject
    protected abstract FlowScope getFlowScope();

    @Inject
    protected abstract FlowProviders getFlowProviders();

    @Override
    public void apply(Project target) {
        String buildNumber = System.getenv("BUILD_NUMBER") != null
            ? System.getenv("BUILD_NUMBER")
            : System.getenv("BUILDKITE_BUILD_NUMBER");
        String performanceTest = System.getenv("BUILD_PERFORMANCE_TEST");
        if (buildNumber != null && performanceTest == null && GradleUtils.isIncludedBuild(target) == false) {
            var fileset = target.fileTree(target.getProjectDir(), files -> {
                files.include("**/*.hprof");
                files.include("**/build/test-results/**/*.xml");
                files.include("**/build/testclusters/**");
                files.include("**/build/testrun/*/temp/**");
                files.include("**/build/**/hs_err_pid*.log");
                files.exclude("**/build/testclusters/**/data/**");
                files.exclude("**/build/testclusters/**/distro/**");
                files.exclude("**/build/testclusters/**/repo/**");
                files.exclude("**/build/testclusters/**/extract/**");
                files.exclude("**/build/testclusters/**/tmp/**");
                files.exclude("**/build/testrun/*/temp/**/data/**");
                files.exclude("**/build/testrun/*/temp/**/distro/**");
                files.exclude("**/build/testrun/*/temp/**/repo/**");
                files.exclude("**/build/testrun/*/temp/**/extract/**");
                files.exclude("**/build/testrun/*/temp/**/tmp/**");
            }).getFiles().stream().filter(file -> Files.isRegularFile(file.toPath())).toList();

            File daemonsLogDir = new File(target.getGradle().getGradleUserHomeDir(), "daemon/" + target.getGradle().getGradleVersion());
            var gradleDaemonFileSet = target.fileTree(
                daemonsLogDir,
                files -> { files.include("**/daemon-" + ProcessHandle.current().pid() + "*.log"); }
            );
            var gradleWorkersFileSet = target.fileTree(new File(target.getGradle().getGradleUserHomeDir(), "workers/"));
            var reaperFileSet = target.fileTree(new File(target.getProjectDir(), ".gradle/reaper/"));

            getFlowScope().always(BuildFinishedFlowAction.class, spec -> {
                spec.getParameters().getUploadFile().set(getFlowProviders().getBuildWorkResult().map(result -> {
                    File targetFile = target.file("build/" + buildNumber + ".new.tar.bzip2");
                    targetFile.getParentFile().mkdirs();
                    return targetFile;
                }));
                spec.getParameters().getProjectDir().set(target.getProjectDir());
                spec.getParameters().getGradleHome().set(target.getGradle().getGradleHomeDir());
                spec.getParameters()
                    .getFiles()
                    .set(
                        getFlowProviders().getBuildWorkResult()
                            .map(result -> calculateFiles(fileset, gradleDaemonFileSet, gradleWorkersFileSet, reaperFileSet))
                    );
            });
        }
    }

    private Iterable<? extends File> calculateFiles(
        List<File> fileset,
        ConfigurableFileTree gradleDaemonFileSet,
        ConfigurableFileTree gradleWorkersFileSet,
        ConfigurableFileTree reaperFileSet
    ) {
        List<File> allFiles = new ArrayList<>();
        allFiles.addAll(fileset);
        if (allFiles.isEmpty() == false) {

            allFiles.addAll(gradleDaemonFileSet.getFiles());
            allFiles.addAll(gradleWorkersFileSet.getFiles());
            allFiles.addAll(reaperFileSet.getFiles());
        }
        return allFiles;
    }

    public abstract static class BuildFinishedFlowAction implements FlowAction<BuildFinishedFlowAction.Parameters> {
        interface Parameters extends FlowParameters {
            @Input
            Property<File> getUploadFile();

            @Input
            Property<File> getProjectDir();

            @Input
            Property<File> getGradleHome();

            @Input
            ListProperty<File> getFiles();

            Property<BuildScanE>

        }

        @Inject
        protected abstract FileSystemOperations getFileSystemOperations();

        @SuppressWarnings("checkstyle:DescendantToken")
        @Override
        public void execute(BuildFinishedFlowAction.Parameters parameters) throws FileNotFoundException {
            File uploadFile = parameters.getUploadFile().get();
            if (uploadFile.exists()) {
                getFileSystemOperations().delete(spec -> spec.delete(uploadFile));
            }
            createBuildArchiveTar(parameters, uploadFile);

                if (uploadFile.exists() && System.getenv("BUILDKITE") == "true") {
                    String uploadFilePath = "build/" + uploadFile.getName();

                    try {
                        System.out.println("Uploading buildkite artifact: " + uploadFilePath + "...");
                        new ProcessBuilder("buildkite-agent", "artifact", "upload", uploadFilePath)
                            .start()
                            .waitFor();

                        System.out.println("Generating buildscan link for artifact...");

                        Process process = new ProcessBuilder("buildkite-agent", "artifact", "search", uploadFilePath, "--step", System.getenv("BUILDKITE_JOB_ID"), "--format", "%i").start();
                        process.waitFor();
                        String processOutput;
                            try {
                                processOutput = IOUtils.toString(process.getInputStream());
                        }catch (IOException e) {
                                processOutput = "";
                        }
                        String artifactUuid = processOutput.trim();

                        System.out.println("Artifact UUID: " + artifactUuid);
                        if (artifactUuid.isEmpty() == false) {
                            // TODO BUILD SCAN SUPPORT
                            // buildScan.link "Artifact Upload", "https://buildkite.com/organizations/elastic/pipelines/${System.getenv('BUILDKITE_PIPELINE_SLUG')}/builds/${buildNumber}/jobs/${System.getenv('BUILDKITE_JOB_ID')}/artifacts/${artifactUuid}"
                        }
                    } catch (Exception e) {
                        System.out.println("Failed to upload buildkite artifact " + e.getMessage());
                    }
                }

        }

        private static void createBuildArchiveTar(Parameters parameters, File uploadFile) {
            try (
                OutputStream fOut = Files.newOutputStream(uploadFile.toPath());
                BufferedOutputStream buffOut = new BufferedOutputStream(fOut);
                BZip2CompressorOutputStream bzOut = new BZip2CompressorOutputStream(buffOut);
                TarArchiveOutputStream tOut = new TarArchiveOutputStream(bzOut)
            ) {
                Path projectPath = parameters.getProjectDir().get().toPath();
                tOut.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
                for (Path path : parameters.getFiles()
                    .map(fileList -> fileList.stream().map(File::toPath).collect(Collectors.toList()))
                    .get()) {
                    if (!Files.isRegularFile(path)) {
                        throw new IOException("Support only file!");
                    }

                    TarArchiveEntry tarEntry = new TarArchiveEntry(path.toFile(), calculateArchivePath(path, projectPath));

                    tOut.putArchiveEntry(tarEntry);

                    // copy file to TarArchiveOutputStream
                    Files.copy(path, tOut);
                    tOut.closeArchiveEntry();

                }
                tOut.flush();
                tOut.finish();

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @NotNull
        private static String calculateArchivePath(Path path, Path projectPath) {
            return path.startsWith(projectPath) ? projectPath.relativize(path).toString() : path.getFileName().toString();
        }
    }
}
