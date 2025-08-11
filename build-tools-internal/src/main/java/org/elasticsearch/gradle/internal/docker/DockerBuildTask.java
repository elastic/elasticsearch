/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gradle.internal.docker;

import org.elasticsearch.gradle.Architecture;
import org.elasticsearch.gradle.LoggedExec;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.MapProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;
import org.gradle.process.ExecOperations;
import org.gradle.workers.WorkAction;
import org.gradle.workers.WorkParameters;
import org.gradle.workers.WorkerExecutor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

/**
 * This task wraps up the details of building a Docker image, including adding a pull
 * mechanism that can retry, and emitting the image SHA as a task output.
 */
public class DockerBuildTask extends DefaultTask {
    private static final Logger LOGGER = Logging.getLogger(DockerBuildTask.class);

    private final WorkerExecutor workerExecutor;
    private final RegularFileProperty markerFile;
    private final DirectoryProperty dockerContext;

    private String[] tags;
    private boolean pull = true;
    private boolean noCache = true;
    private String[] baseImages;
    private MapProperty<String, String> buildArgs;
    private Property<String> platform;

    @Inject
    public DockerBuildTask(WorkerExecutor workerExecutor, ObjectFactory objectFactory, ProjectLayout projectLayout) {
        this.workerExecutor = workerExecutor;
        this.markerFile = objectFactory.fileProperty();
        this.dockerContext = objectFactory.directoryProperty();
        this.buildArgs = objectFactory.mapProperty(String.class, String.class);
        this.platform = objectFactory.property(String.class).convention(Architecture.current().dockerPlatform);
        this.markerFile.set(projectLayout.getBuildDirectory().file("markers/" + this.getName() + ".marker"));
    }

    @TaskAction
    public void build() {
        workerExecutor.noIsolation().submit(DockerBuildAction.class, params -> {
            params.getDockerContext().set(dockerContext);
            params.getMarkerFile().set(markerFile);
            params.getTags().set(Arrays.asList(tags));
            params.getPull().set(pull);
            params.getNoCache().set(noCache);
            params.getBaseImages().set(Arrays.asList(baseImages));
            params.getBuildArgs().set(buildArgs);
            params.getPlatform().set(platform);
        });
    }

    @InputDirectory
    @PathSensitive(PathSensitivity.RELATIVE)
    public DirectoryProperty getDockerContext() {
        return dockerContext;
    }

    @Input
    public String[] getTags() {
        return tags;
    }

    public void setTags(String[] tags) {
        this.tags = tags;
    }

    @Input
    public boolean isPull() {
        return pull;
    }

    public void setPull(boolean pull) {
        this.pull = pull;
    }

    @Input
    public boolean isNoCache() {
        return noCache;
    }

    public void setNoCache(boolean noCache) {
        this.noCache = noCache;
    }

    @Input
    public String[] getBaseImages() {
        return baseImages;
    }

    public void setBaseImages(String[] baseImages) {
        this.baseImages = baseImages;
    }

    @Input
    public MapProperty<String, String> getBuildArgs() {
        return buildArgs;
    }

    @Input
    public Property<String> getPlatform() {
        return platform;
    }

    @OutputFile
    public RegularFileProperty getMarkerFile() {
        return markerFile;
    }

    public abstract static class DockerBuildAction implements WorkAction<Parameters> {
        private final ExecOperations execOperations;

        @Inject
        public DockerBuildAction(ExecOperations execOperations) {
            this.execOperations = execOperations;
        }

        /**
         * Wraps `docker pull` in a retry loop, to try and provide some resilience against
         * transient errors
         * @param baseImage the image to pull.
         */
        private void pullBaseImage(String baseImage) {
            final int maxAttempts = 10;

            for (int attempt = 1; attempt <= maxAttempts; attempt++) {
                try {
                    LoggedExec.exec(execOperations, spec -> {
                        spec.executable("docker");
                        spec.args("pull");
                        spec.args(baseImage);
                    });

                    return;
                } catch (Exception e) {
                    LOGGER.warn("Attempt {}/{} to pull Docker base image {} failed", attempt, maxAttempts, baseImage);
                }
            }

            // If we successfully ran `docker pull` above, we would have returned before this point.
            throw new GradleException("Failed to pull Docker base image [" + baseImage + "], all attempts failed");
        }

        @Override
        public void execute() {
            final Parameters parameters = getParameters();

            if (parameters.getPull().get()) {
                parameters.getBaseImages().get().forEach(this::pullBaseImage);
            }

            final List<String> tags = parameters.getTags().get();
            final boolean isCrossPlatform = parameters.getPlatform().get().equals(Architecture.current().dockerPlatform) == false;

            LoggedExec.exec(execOperations, spec -> {
                spec.executable("docker");

                if (isCrossPlatform) {
                    spec.args("buildx");
                }

                spec.args("build", parameters.getDockerContext().get().getAsFile().getAbsolutePath());

                if (isCrossPlatform) {
                    spec.args("--platform", parameters.getPlatform().get());
                }

                if (parameters.getNoCache().get()) {
                    spec.args("--no-cache");
                }

                tags.forEach(tag -> spec.args("--tag", tag));

                parameters.getBuildArgs().get().forEach((k, v) -> spec.args("--build-arg", k + "=" + v));
            });

            // Fetch the Docker image's hash, and write it to desk as the task's output. Doing this allows us
            // to do proper up-to-date checks in Gradle.
            try {
                final String checksum = getImageChecksum(tags.get(0));
                Files.writeString(parameters.getMarkerFile().getAsFile().get().toPath(), checksum + "\n");
            } catch (IOException e) {
                throw new RuntimeException("Failed to write marker file", e);
            }
        }

        private String getImageChecksum(String imageTag) {
            final ByteArrayOutputStream stdout = new ByteArrayOutputStream();

            execOperations.exec(spec -> {
                spec.setCommandLine("docker", "inspect", "--format", "{{ .Id }}", imageTag);
                spec.setStandardOutput(stdout);
                spec.setIgnoreExitValue(false);
            });

            return stdout.toString().trim();
        }
    }

    interface Parameters extends WorkParameters {
        DirectoryProperty getDockerContext();

        RegularFileProperty getMarkerFile();

        ListProperty<String> getTags();

        Property<Boolean> getPull();

        Property<Boolean> getNoCache();

        ListProperty<String> getBaseImages();

        MapProperty<String, String> getBuildArgs();

        Property<String> getPlatform();
    }
}
