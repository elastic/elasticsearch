/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.internal.docker;

import org.elasticsearch.gradle.Architecture;
import org.elasticsearch.gradle.LoggedExec;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.services.ServiceReference;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.Classpath;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;
import org.gradle.process.ExecOperations;
import org.gradle.process.ExecSpec;
import org.gradle.workers.WorkAction;
import org.gradle.workers.WorkParameters;
import org.gradle.workers.WorkerExecutor;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

/**
 * Builds a GraalVM native-image binary by running native-image inside a Docker
 * container. Uses the Gradle worker API so multiple architecture builds can
 * run in parallel, and is cacheable for remote build cache.
 *
 * We assume native images to be optional, and a fallback to be available, so
 * this task will be skipped if Docker is not available or the target platform
 * is not supported rather than failing.
 */
@CacheableTask
public abstract class NativeImageBuildTask extends DefaultTask {

    private FileCollection classpath;

    @Inject
    public NativeImageBuildTask() {
        onlyIf(
            "Docker supports target platform",
            task -> Architecture.fromDockerPlatform(getPlatform().getOrNull())
                .map(arch -> getDockerSupport().get().isArchitectureSupported(arch))
                .orElse(false)
                && getDockerSupport().get().getDockerAvailability().isAvailable()
        );
    }

    @Classpath
    public FileCollection getClasspath() {
        return classpath;
    }

    public void setClasspath(FileCollection classpath) {
        this.classpath = classpath;
    }

    @Input
    public abstract Property<String> getImageTag();

    @Input
    public abstract Property<String> getPlatform();

    @Input
    public abstract Property<String> getMainClass();

    /**
     * When true, pass {@code --static} to native-image to produce a fully static binary.
     * Defaults to false.
     */
    @Input
    @Optional
    public abstract Property<Boolean> getStatic();

    @OutputFile
    public abstract RegularFileProperty getOutputFile();

    @ServiceReference(DockerSupportPlugin.DOCKER_SUPPORT_SERVICE_NAME)
    public abstract Property<DockerSupportService> getDockerSupport();

    @Inject
    public abstract WorkerExecutor getWorkerExecutor();

    @TaskAction
    public void execute() {
        String dockerExecutable = getDockerSupport().get().getResolvedDockerExecutable();
        getWorkerExecutor().noIsolation().submit(NativeImageBuildAction.class, params -> {
            params.getClasspath().setFrom(getClasspath());
            params.getImageTag().set(getImageTag());
            params.getPlatform().set(getPlatform());
            params.getMainClass().set(getMainClass());
            params.getStatic().set(getStatic().getOrElse(false));
            params.getOutputFile().set(getOutputFile());
            params.getDockerExecutable().set(dockerExecutable);
        });
    }

    interface Parameters extends WorkParameters {
        ConfigurableFileCollection getClasspath();

        Property<String> getImageTag();

        Property<String> getPlatform();

        Property<String> getMainClass();

        Property<Boolean> getStatic();

        RegularFileProperty getOutputFile();

        Property<String> getDockerExecutable();
    }

    public abstract static class NativeImageBuildAction implements WorkAction<Parameters> {

        private final ExecOperations execOperations;

        @Inject
        public NativeImageBuildAction(ExecOperations execOperations) {
            this.execOperations = execOperations;
        }

        @Override
        public void execute() {
            Parameters params = getParameters();
            String imageTag = params.getImageTag().get();
            String platform = params.getPlatform().get();
            String mainClass = params.getMainClass().get();
            File outputFile = params.getOutputFile().get().getAsFile();
            File outputDir = outputFile.getParentFile();

            if (outputDir.exists() == false && outputDir.mkdirs() == false) {
                throw new GradleException("Failed to create output directory: " + outputDir);
            }

            List<File> classpathFiles = params.getClasspath().getFiles().stream().filter(File::exists).collect(Collectors.toList());
            if (classpathFiles.isEmpty()) {
                throw new GradleException("Native-image classpath is empty");
            }

            // Build classpath string for inside the container: /cp/0:/cp/1:...
            List<String> cpPaths = new ArrayList<>();
            for (int i = 0; i < classpathFiles.size(); i++) {
                cpPaths.add("/cp/" + i);
            }
            // Container is always Linux
            String cpString = String.join(":", cpPaths);

            List<String> args = new ArrayList<>();
            args.add("run");
            args.add("--rm");
            for (int i = 0; i < classpathFiles.size(); i++) {
                File f = classpathFiles.get(i);
                String path = f.getAbsolutePath();
                if (File.separatorChar == '\\') {
                    path = path.replace("\\", "/");
                }
                args.add("-v");
                args.add(path + ":/cp/" + i + ":ro");
            }
            args.add("-v");
            String outPath = outputDir.getAbsolutePath();
            if (File.separatorChar == '\\') {
                outPath = outPath.replace("\\", "/");
            }
            args.add(outPath + ":/output");
            args.add("--platform");
            args.add(platform);
            args.add(imageTag);
            args.add("--no-fallback");
            if (params.getStatic().get()) {
                args.add("--static");
            }
            args.add("-cp");
            args.add(cpString);
            args.add("-o");
            args.add("/output/" + outputFile.getName());
            args.add(mainClass);

            LoggedExec.exec(execOperations, spec -> {
                maybeConfigureDockerConfig(spec);
                spec.executable(params.getDockerExecutable().get());
                spec.args(args);
            });
        }

        private void maybeConfigureDockerConfig(ExecSpec spec) {
            String dockerConfig = System.getenv("DOCKER_CONFIG");
            if (dockerConfig != null) {
                spec.environment("DOCKER_CONFIG", dockerConfig);
            }
        }
    }
}
