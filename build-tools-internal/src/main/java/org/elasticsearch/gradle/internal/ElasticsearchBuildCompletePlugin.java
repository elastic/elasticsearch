/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import com.gradle.develocity.agent.gradle.DevelocityConfiguration;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.file.FileSystemOperations;
import org.gradle.api.flow.FlowAction;
import org.gradle.api.flow.FlowParameters;
import org.gradle.api.flow.FlowProviders;
import org.gradle.api.flow.FlowScope;
import org.gradle.api.internal.file.FileOperations;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import javax.inject.Inject;

public abstract class ElasticsearchBuildCompletePlugin implements Plugin<Project> {

    @Inject
    protected abstract FlowScope getFlowScope();

    @Inject
    protected abstract FlowProviders getFlowProviders();

    @Inject
    protected abstract FileOperations getFileOperations();

    @Override
    public void apply(Project target) {
        String buildNumber = System.getenv("BUILD_NUMBER") != null
            ? System.getenv("BUILD_NUMBER")
            : System.getenv("BUILDKITE_BUILD_NUMBER");
        String performanceTest = System.getenv("BUILD_PERFORMANCE_TEST");
        if (buildNumber != null && performanceTest == null && GradleUtils.isIncludedBuild(target) == false) {
            File targetFile = target.file("build/" + buildNumber + ".tar.bz2");
            File projectDir = target.getProjectDir();
            File gradleWorkersDir = new File(target.getGradle().getGradleUserHomeDir(), "workers/");
            DevelocityConfiguration extension = target.getExtensions().getByType(DevelocityConfiguration.class);
            File daemonsLogDir = new File(target.getGradle().getGradleUserHomeDir(), "daemon/" + target.getGradle().getGradleVersion());

            getFlowScope().always(BuildFinishedFlowAction.class, spec -> {
                spec.getParameters().getBuildScan().set(extension);
                spec.getParameters().getUploadFile().set(targetFile);
                spec.getParameters().getProjectDir().set(projectDir);
                spec.getParameters().getFilteredFiles().addAll(getFlowProviders().getBuildWorkResult().map((result) -> {
                    System.out.println("Build Finished Action: Collecting archive files...");
                    List<File> files = new ArrayList<>();
                    files.addAll(resolveProjectLogs(projectDir));
                    if (files.isEmpty() == false) {
                        files.addAll(resolveDaemonLogs(daemonsLogDir));
                        files.addAll(getFileOperations().fileTree(gradleWorkersDir).getFiles());
                        files.addAll(getFileOperations().fileTree(new File(projectDir, ".gradle/reaper/")).getFiles());
                    }
                    return files;
                }));
            });
        }
    }

    private List<File> resolveProjectLogs(File projectDir) {
        var projectDirFiles = getFileOperations().fileTree(projectDir);
        projectDirFiles.include("**/*.hprof");
        projectDirFiles.include("**/build/test-results/**/*.xml");
        projectDirFiles.include("**/build/testclusters/**");
        projectDirFiles.include("**/build/testrun/*/temp/**");
        projectDirFiles.include("**/build/**/hs_err_pid*.log");
        projectDirFiles.include("**/build/**/replay_pid*.log");
        projectDirFiles.exclude("**/build/testclusters/**/data/**");
        projectDirFiles.exclude("**/build/testclusters/**/distro/**");
        projectDirFiles.exclude("**/build/testclusters/**/repo/**");
        projectDirFiles.exclude("**/build/testclusters/**/extract/**");
        projectDirFiles.exclude("**/build/testclusters/**/tmp/**");
        projectDirFiles.exclude("**/build/testrun/*/temp/**/data/**");
        projectDirFiles.exclude("**/build/testrun/*/temp/**/distro/**");
        projectDirFiles.exclude("**/build/testrun/*/temp/**/repo/**");
        projectDirFiles.exclude("**/build/testrun/*/temp/**/extract/**");
        projectDirFiles.exclude("**/build/testrun/*/temp/**/tmp/**");
        return projectDirFiles.getFiles().stream().filter(f -> Files.isRegularFile(f.toPath())).toList();
    }

    private List<File> resolveDaemonLogs(File daemonsLogDir) {
        var gradleDaemonFileSet = getFileOperations().fileTree(daemonsLogDir);
        gradleDaemonFileSet.include("**/daemon-" + ProcessHandle.current().pid() + "*.log");
        return gradleDaemonFileSet.getFiles().stream().filter(f -> Files.isRegularFile(f.toPath())).toList();
    }

    public abstract static class BuildFinishedFlowAction implements FlowAction<BuildFinishedFlowAction.Parameters> {
        interface Parameters extends FlowParameters {
            @Input
            Property<File> getUploadFile();

            @Input
            Property<File> getProjectDir();

            @Input
            ListProperty<File> getFilteredFiles();

            @Input
            Property<DevelocityConfiguration> getBuildScan();

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
            uploadFile.getParentFile().mkdirs();
            createBuildArchiveTar(parameters.getFilteredFiles().get(), parameters.getProjectDir().get(), uploadFile);
            if (uploadFile.exists() && "true".equals(System.getenv("BUILDKITE"))) {
                String uploadFilePath = uploadFile.getName();
                File uploadFileDir = uploadFile.getParentFile();
                try {
                    System.out.println("Uploading buildkite artifact: " + uploadFilePath + "...");
                    ProcessBuilder pb = new ProcessBuilder("buildkite-agent", "artifact", "upload", uploadFilePath);
                    // If we don't switch to the build directory first, the uploaded file will have a `build/` prefix
                    // Buildkite will flip the `/` to a `\` at upload time on Windows, which will make the search command below fail
                    // So, if you change this such that the artifact will have a slash/directory in it, you'll need to update the logic
                    // below as well
                    pb.directory(uploadFileDir);
                    pb.start().waitFor();

                    System.out.println("Generating buildscan link for artifact...");

                    // Output should be in the format: "<UUID><space><ISO-8601-timestamp>\n"
                    // and multiple artifacts could be returned
                    Process process = new ProcessBuilder(
                        "buildkite-agent",
                        "artifact",
                        "search",
                        uploadFilePath,
                        "--step",
                        System.getenv("BUILDKITE_JOB_ID"),
                        "--format",
                        "%i %c"
                    ).start();
                    process.waitFor();
                    String processOutput;
                    try {
                        processOutput = IOUtils.toString(process.getInputStream());
                    } catch (IOException e) {
                        processOutput = "";
                    }

                    // Sort them by timestamp, and grab the most recent one
                    Optional<String> artifact = Arrays.stream(processOutput.trim().split("\n")).map(String::trim).min((a, b) -> {
                        String[] partsA = a.split(" ");
                        String[] partsB = b.split(" ");
                        // ISO-8601 timestamps can be sorted lexicographically
                        return partsB[1].compareTo(partsA[1]);
                    });

                    // Grab just the UUID from the artifact
                    String artifactUuid = artifact.orElse("").split(" ")[0];

                    System.out.println("Artifact UUID: " + artifactUuid);
                    if (artifactUuid.isEmpty() == false) {
                        String buildkitePipelineSlug = System.getenv("BUILDKITE_PIPELINE_SLUG");
                        String targetLink = "https://buildkite.com/organizations/elastic/pipelines/"
                            + buildkitePipelineSlug
                            + "/builds/"
                            + System.getenv("BUILD_NUMBER")
                            + "/jobs/"
                            + System.getenv("BUILDKITE_JOB_ID")
                            + "/artifacts/"
                            + artifactUuid;
                        parameters.getBuildScan().get().getBuildScan().link("Artifact Upload", targetLink);
                    }
                } catch (Exception e) {
                    System.out.println("Failed to upload buildkite artifact " + e.getMessage());
                }
            }

        }

        private static void createBuildArchiveTar(List<File> files, File projectDir, File uploadFile) {
            try (
                OutputStream fOut = Files.newOutputStream(uploadFile.toPath());
                BufferedOutputStream buffOut = new BufferedOutputStream(fOut);
                BZip2CompressorOutputStream bzOut = new BZip2CompressorOutputStream(buffOut);
                TarArchiveOutputStream tOut = new TarArchiveOutputStream(bzOut)
            ) {
                Path projectPath = projectDir.toPath();
                tOut.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
                tOut.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_STAR);
                for (Path path : files.stream().map(File::toPath).toList()) {
                    if (!Files.isRegularFile(path)) {
                        throw new IOException("Support only file!");
                    }

                    long entrySize = Files.size(path);
                    TarArchiveEntry tarEntry = new TarArchiveEntry(path.toFile(), calculateArchivePath(path, projectPath));
                    tarEntry.setSize(entrySize);
                    tOut.putArchiveEntry(tarEntry);

                    // copy file to TarArchiveOutputStream
                    try (BufferedInputStream bin = new BufferedInputStream(Files.newInputStream(path))) {
                        IOUtils.copyLarge(bin, tOut, 0, entrySize);
                    }
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
