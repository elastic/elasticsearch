/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal;

import com.gradle.develocity.agent.gradle.DevelocityConfiguration;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.gradle.OS;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.inject.Inject;

public abstract class ElasticsearchBuildCompletePlugin implements Plugin<Project> {

    private static final Logger log = LoggerFactory.getLogger(ElasticsearchBuildCompletePlugin.class);

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
            File targetFile = calculateTargetFile(target, buildNumber);
            File projectDir = target.getProjectDir();
            File gradleWorkersDir = new File(target.getGradle().getGradleUserHomeDir(), "workers/");
            DevelocityConfiguration extension = target.getExtensions().getByType(DevelocityConfiguration.class);
            File daemonsLogDir = new File(target.getGradle().getGradleUserHomeDir(), "daemon/" + target.getGradle().getGradleVersion());

            File preemptionMarker = new File(projectDir, "build/.preemption-marker.json");
            // Written incrementally by GradleRunner's TaskTracker as each task finishes. Reading it
            // in the flow action is configuration-cache safe: the file path is a stable value captured
            // at configuration time; the file contents are read at execution time after all tasks finish.
            File taskStatusFile = new File(projectDir, "build/task-status-incremental.jsonl");
            getFlowScope().always(BuildFinishedFlowAction.class, spec -> {
                spec.getParameters().getBuildScan().set(extension);
                spec.getParameters().getUploadFile().set(targetFile);
                spec.getParameters().getProjectDir().set(projectDir);
                spec.getParameters().getFilteredFiles().addAll(getFlowProviders().getBuildWorkResult().map((result) -> {
                    if (preemptionMarker.exists()) {
                        System.out.println("Build Finished Action: Skipping archive collection (build was preempted)");
                        return List.<File>of();
                    }
                    if (result.getFailures().isEmpty()) {
                        System.out.println("Build Finished Action: Build succeeded, skipping archive collection");
                        return List.<File>of();
                    }
                    System.out.println("Build Finished Action: Collecting archive files...");
                    // If GradleRunner tracked task failures, collect only from those projects.
                    // Otherwise fall back to the whole project tree (e.g. configuration-cache miss
                    // or dependency resolution error where no individual task ran and failed).
                    Set<File> failedProjectDirs = readFailedTaskProjectDirs(taskStatusFile, projectDir);
                    Set<File> dirsToCollect = failedProjectDirs.isEmpty() ? Set.of(projectDir) : failedProjectDirs;
                    List<File> files = new ArrayList<>();
                    files.addAll(resolveProjectLogs(projectDir, preemptionMarker, dirsToCollect));
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

    private File calculateTargetFile(Project target, String buildNumber) {
        File uploadFile = target.file("build/" + buildNumber + ".tar.bz2");
        int artifactIndex = 1;
        while (uploadFile.exists()) {
            uploadFile = target.file("build/" + buildNumber + "-" + artifactIndex++ + ".tar.bz2");
        }
        return uploadFile;
    }

    /**
     * Reads the incremental task-status JSONL file written by GradleRunner's TaskTracker and
     * returns the project directories for all tasks that finished with outcome {@code FAILED}.
     * Returns an empty set if the file does not exist or cannot be read.
     */
    private static Set<File> readFailedTaskProjectDirs(File taskStatusFile, File rootProjectDir) {
        if (taskStatusFile.exists() == false) {
            return Set.of();
        }
        try {
            return Files.readAllLines(taskStatusFile.toPath())
                .stream()
                .filter(line -> line.contains("\"outcome\":\"FAILED\""))
                .map(line -> {
                    int pathStart = line.indexOf("\"path\":\"") + 8;
                    int pathEnd = line.indexOf("\"", pathStart);
                    return taskPathToProjectDir(line.substring(pathStart, pathEnd), rootProjectDir);
                })
                .collect(Collectors.toCollection(LinkedHashSet::new));
        } catch (IOException e) {
            System.out.println("Build Finished Action: Failed to read incremental task status file: " + e.getMessage());
            return Set.of();
        }
    }

    /**
     * Converts a Gradle task path (e.g. {@code :x-pack:plugin:esql:test}) to the filesystem
     * directory of its project by stripping the task name and translating path separators.
     * Assumes the project directory layout mirrors the Gradle project path, which is true for
     * this repository.
     */
    private static File taskPathToProjectDir(String taskPath, File rootProjectDir) {
        int lastColon = taskPath.lastIndexOf(':');
        String projectSubpath = lastColon > 0 ? taskPath.substring(1, lastColon).replace(':', File.separatorChar) : "";
        return projectSubpath.isEmpty() ? rootProjectDir : new File(rootProjectDir, projectSubpath);
    }

    private List<File> resolveProjectLogs(File rootProjectDir, File preemptionMarker, Set<File> projectDirsToCollect) {
        // HACK: Some tests leave behind symlinks, and gradle throws an exception if it encounters symlinks.
        // Here we remove them before collecting logs to upload. We could instead build our own path matcher
        // but that seemed more complex than just deleting the irrelevant files.
        for (File dir : projectDirsToCollect) {
            try {
                Files.walkFileTree(dir.toPath(), new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        if (preemptionMarker.exists()) {
                            return FileVisitResult.TERMINATE;
                        }
                        try {
                            if (Files.isSymbolicLink(file)) {
                                Files.delete(file);
                            }
                        } catch (java.nio.file.NoSuchFileException e) {
                            System.out.println("Symlink : " + file + " already deleted.");
                        }
                        return FileVisitResult.CONTINUE;
                    }
                });
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            if (preemptionMarker.exists()) {
                System.out.println("Build Finished Action: Aborting file collection (preemption detected during walk)");
                return List.of();
            }
        }

        Set<File> collectedFiles = new LinkedHashSet<>();
        for (File dir : projectDirsToCollect) {
            var projectDirFiles = getFileOperations().fileTree(dir);
            projectDirFiles.include("**/*.hprof");
            projectDirFiles.include("**/build/reports/configuration-cache/**");
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
            projectDirFiles.getFiles().stream().filter(f -> Files.isRegularFile(f.toPath())).forEach(collectedFiles::add);
        }
        // core dump files are in the working directory of the installation, which is not project specific
        var distributionFiles = getFileOperations().fileTree(rootProjectDir);
        distributionFiles.include("distribution/**/build/install/*/core.*");
        distributionFiles.getFiles().stream().filter(f -> Files.isRegularFile(f.toPath())).forEach(collectedFiles::add);

        return new ArrayList<>(collectedFiles);
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
            List<File> filesToArchive = parameters.getFilteredFiles().get();
            if (filesToArchive.isEmpty()) {
                return;
            }
            File projectDir = parameters.getProjectDir().get();
            File preemptionMarker = new File(projectDir, "build/.preemption-marker.json");
            if (preemptionMarker.exists()) {
                System.out.println("Build Finished Action: Skipping archive/upload (build was preempted)");
                return;
            }
            File uploadFile = parameters.getUploadFile().get();
            if (uploadFile.exists()) {
                getFileSystemOperations().delete(spec -> spec.delete(uploadFile));
            }
            uploadFile.getParentFile().mkdirs();

            createBuildArchiveTar(filesToArchive, parameters.getProjectDir().get(), uploadFile);
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
                    try {
                        // we are very generious here, as the upload can take
                        // a long time depending on its size
                        long timeoutSec = calculateUploadWaitTimeoutSeconds(uploadFile);
                        boolean completedInTime = pb.start().waitFor(timeoutSec, TimeUnit.SECONDS);
                        if (completedInTime == false) {
                            System.out.println("Timed out waiting for buildkite artifact upload after " + timeoutSec + " seconds");
                        }
                    } catch (InterruptedException e) {
                        System.out.println("Failed to upload buildkite artifact " + e.getMessage());
                    }

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
                    if (Files.exists(path) == false) {
                        log.warn("File disappeared before it could be added to CI archive: " + path);
                        continue;
                    } else if (!Files.isRegularFile(path)) {
                        throw new IOException("Support only file!: " + path);
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
            String archivePath = path.startsWith(projectPath) ? projectPath.relativize(path).toString() : path.getFileName().toString();
            if (OS.current() == OS.WINDOWS) {
                // tar always uses forward slashes
                archivePath = archivePath.replace("\\", "/");
            }
            return archivePath;
        }

        private static long calculateUploadWaitTimeoutSeconds(File file) {
            long fileSizeBytes = file.length();
            long fileSizeMB = fileSizeBytes / (1024 * 1024);

            // Allocate 8 seconds per MB (assumes ~125 KB/s upload speed)
            // with min 10 seconds and max 30 minutes
            return Math.max(10, Math.min(1800, fileSizeMB * 8));
        }
    }
}
