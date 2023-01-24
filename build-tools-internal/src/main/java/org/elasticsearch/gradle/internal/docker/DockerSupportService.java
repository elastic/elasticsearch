/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gradle.internal.docker;

import org.elasticsearch.gradle.Architecture;
import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.internal.info.BuildParams;
import org.gradle.api.GradleException;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.services.BuildService;
import org.gradle.api.services.BuildServiceParameters;
import org.gradle.process.ExecOperations;
import org.gradle.process.ExecResult;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import static java.util.function.Predicate.not;

/**
 * Build service for detecting available Docker installation and checking for compatibility with Elasticsearch Docker image build
 * requirements. This includes a minimum version requirement, as well as the ability to run privileged commands.
 */
public abstract class DockerSupportService implements BuildService<DockerSupportService.Parameters> {

    private static final Logger LOGGER = Logging.getLogger(DockerSupportService.class);
    // Defines the possible locations of the Docker CLI. These will be searched in order.
    private static final String[] DOCKER_BINARIES = { "/usr/bin/docker", "/usr/local/bin/docker" };
    private static final String[] DOCKER_COMPOSE_BINARIES = {
        "/usr/local/bin/docker-compose",
        "/usr/bin/docker-compose",
        "/usr/libexec/docker/cli-plugins/docker-compose" };
    private static final Version MINIMUM_DOCKER_VERSION = Version.fromString("17.05.0");

    private final ExecOperations execOperations;
    private DockerAvailability dockerAvailability;

    @Inject
    public DockerSupportService(ExecOperations execOperations) {
        this.execOperations = execOperations;
    }

    /**
     * Searches for a functional Docker installation, and returns information about the search.
     *
     * @return the results of the search.
     */
    public DockerAvailability getDockerAvailability() {
        if (this.dockerAvailability == null) {
            String dockerPath = null;
            String dockerComposePath = null;
            Result lastResult = null;
            Version version = null;
            boolean isVersionHighEnough = false;
            boolean isComposeAvailable = false;
            Set<Architecture> supportedArchitectures = new HashSet<>();

            // Check if the Docker binary exists
            final Optional<String> dockerBinary = getDockerPath();
            if (isExcludedOs() == false && dockerBinary.isPresent()) {
                dockerPath = dockerBinary.get();

                // Since we use a multi-stage Docker build, check the Docker version meets minimum requirement
                lastResult = runCommand(dockerPath, "version", "--format", "{{.Server.Version}}");

                var lastResultOutput = lastResult.stdout.trim();
                // docker returns 0/success if the daemon is not running, so we need to check the
                // output before continuing
                if (lastResult.isSuccess() && dockerDaemonIsRunning(lastResultOutput)) {

                    version = Version.fromString(lastResultOutput, Version.Mode.RELAXED);

                    isVersionHighEnough = version.onOrAfter(MINIMUM_DOCKER_VERSION);

                    if (isVersionHighEnough) {
                        // Check that we can execute a privileged command
                        lastResult = runCommand(dockerPath, "images");

                        // If docker all checks out, see if docker-compose is available and working
                        Optional<String> composePath = getDockerComposePath();
                        if (lastResult.isSuccess() && composePath.isPresent()) {
                            isComposeAvailable = runCommand(composePath.get(), "version").isSuccess();
                            dockerComposePath = composePath.get();
                        }

                        // Now let's check if buildx is available and what supported platforms exist
                        if (lastResult.isSuccess()) {
                            Result buildxResult = runCommand(dockerPath, "buildx", "inspect", "--bootstrap");
                            if (buildxResult.isSuccess()) {
                                supportedArchitectures = buildxResult.stdout()
                                    .lines()
                                    .filter(l -> l.startsWith("Platforms:"))
                                    .map(l -> l.substring(10))
                                    .flatMap(l -> Arrays.stream(l.split(",")).filter(not(String::isBlank)))
                                    .map(String::trim)
                                    .map(s -> Arrays.stream(Architecture.values()).filter(a -> a.dockerPlatform.equals(s)).findAny())
                                    .filter(Optional::isPresent)
                                    .map(Optional::get)
                                    .collect(Collectors.toSet());
                            } else {
                                supportedArchitectures = Set.of(Architecture.current());
                            }
                        }
                    }
                }
            }

            boolean isAvailable = isVersionHighEnough && lastResult != null && lastResult.isSuccess();

            this.dockerAvailability = new DockerAvailability(
                isAvailable,
                isComposeAvailable,
                isVersionHighEnough,
                dockerPath,
                dockerComposePath,
                version,
                lastResult,
                supportedArchitectures
            );
        }

        return this.dockerAvailability;
    }

    private boolean dockerDaemonIsRunning(String lastResultOutput) {
        return lastResultOutput.contains("Cannot connect to the Docker daemon") == false;
    }

    /**
     * Given a list of tasks that requires Docker, check whether Docker is available, otherwise throw an exception.
     *
     * @throws GradleException if Docker is not available. The exception message gives the reason.
     */
    void failIfDockerUnavailable(List<String> tasks) {
        DockerAvailability availability = getDockerAvailability();

        // Docker installation is available and compatible
        if (availability.isAvailable) {
            return;
        }

        // No Docker binary was located
        if (availability.path == null) {
            final String message = String.format(
                Locale.ROOT,
                "Docker (checked [%s]) is required to run the following task%s: \n%s",
                String.join(", ", DOCKER_BINARIES),
                tasks.size() > 1 ? "s" : "",
                String.join("\n", tasks)
            );
            throwDockerRequiredException(message);
        }

        // Docker binaries were located, but did not meet the minimum version requirement
        if (availability.lastCommand.isSuccess() && availability.isVersionHighEnough == false) {
            final String message = String.format(
                Locale.ROOT,
                "building Docker images requires minimum Docker version of %s due to use of multi-stage builds yet was [%s]",
                MINIMUM_DOCKER_VERSION,
                availability.version
            );
            throwDockerRequiredException(message);
        }

        // Some other problem, print the error
        final String message = String.format(
            Locale.ROOT,
            """
                a problem occurred while using Docker from [%s]%s yet it is required to run the following task%s:
                %s
                the problem is that Docker exited with exit code [%d] with standard error output:
                %s""",
            availability.path,
            availability.version == null ? "" : " v" + availability.version,
            tasks.size() > 1 ? "s" : "",
            String.join("\n", tasks),
            availability.lastCommand.exitCode,
            availability.lastCommand.stderr.trim()
        );
        throwDockerRequiredException(message);
    }

    private boolean isExcludedOs() {
        // We don't attempt to check the current flavor and version of Linux unless we're
        // running in CI, because we don't want to stop people running the Docker tests in
        // their own environments if they really want to.
        if (BuildParams.isCi() == false) {
            return false;
        }

        // Only some hosts in CI are configured with Docker. We attempt to work out the OS
        // and version, so that we know whether to expect to find Docker. We don't attempt
        // to probe for whether Docker is available, because that doesn't tell us whether
        // Docker is unavailable when it should be.
        final Path osRelease = Paths.get("/etc/os-release");

        if (Files.exists(osRelease)) {
            Map<String, String> values;

            try {
                final List<String> osReleaseLines = Files.readAllLines(osRelease);
                values = parseOsRelease(osReleaseLines);
            } catch (IOException e) {
                throw new GradleException("Failed to read /etc/os-release", e);
            }

            final String id = deriveId(values);
            final boolean excluded = getLinuxExclusionList().contains(id);

            if (excluded) {
                LOGGER.warn("Linux OS id [{}] is present in the Docker exclude list. Tasks requiring Docker will be disabled.", id);
            }

            return excluded;
        }

        return false;
    }

    private List<String> getLinuxExclusionList() {
        File exclusionsFile = getParameters().getExclusionsFile();

        if (exclusionsFile.exists()) {
            try {
                return Files.readAllLines(exclusionsFile.toPath())
                    .stream()
                    .map(String::trim)
                    .filter(line -> (line.isEmpty() || line.startsWith("#")) == false)
                    .collect(Collectors.toList());
            } catch (IOException e) {
                throw new GradleException("Failed to read " + exclusionsFile.getAbsolutePath(), e);
            }
        } else {
            return Collections.emptyList();
        }
    }

    // visible for testing
    static String deriveId(Map<String, String> values) {
        return values.get("ID") + "-" + values.get("VERSION_ID");
    }

    // visible for testing
    static Map<String, String> parseOsRelease(final List<String> osReleaseLines) {
        final Map<String, String> values = new HashMap<>();

        osReleaseLines.stream().map(String::trim).filter(line -> (line.isEmpty() || line.startsWith("#")) == false).forEach(line -> {
            final String[] parts = line.split("=", 2);
            final String key = parts[0];
            // remove optional leading and trailing quotes and whitespace
            final String value = parts[1].replaceAll("^['\"]?\\s*", "").replaceAll("\\s*['\"]?$", "");

            values.put(key, value.toLowerCase());
        });

        return values;
    }

    /**
     * Searches the entries in {@link #DOCKER_BINARIES} for the Docker CLI. This method does
     * not check whether the Docker installation appears usable, see {@link #getDockerAvailability()}
     * instead.
     *
     * @return the path to a CLI, if available.
     */
    private Optional<String> getDockerPath() {
        // Check if the Docker binary exists
        return List.of(DOCKER_BINARIES).stream().filter(path -> new File(path).exists()).findFirst();
    }

    /**
     * Searches the entries in {@link #DOCKER_COMPOSE_BINARIES} for the Docker Compose CLI. This method does
     * not check whether the installation appears usable, see {@link #getDockerAvailability()} instead.
     *
     * @return the path to a CLI, if available.
     */
    private Optional<String> getDockerComposePath() {
        // Check if the Docker binary exists
        return List.of(DOCKER_COMPOSE_BINARIES).stream().filter(path -> new File(path).exists()).findFirst();
    }

    private void throwDockerRequiredException(final String message) {
        throwDockerRequiredException(message, null);
    }

    private void throwDockerRequiredException(final String message, Exception e) {
        throw new GradleException(
            message + "\nyou can address this by attending to the reported issue, or removing the offending tasks from being executed.",
            e
        );
    }

    /**
     * Runs a command and captures the exit code, standard output and standard error.
     *
     * @param args the command and any arguments to execute
     * @return a object that captures the result of running the command. If an exception occurring
     * while running the command, or the process was killed after reaching the 10s timeout,
     * then the exit code will be -1.
     */
    private Result runCommand(String... args) {
        if (args.length == 0) {
            throw new IllegalArgumentException("Cannot execute with no command");
        }

        ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        ByteArrayOutputStream stderr = new ByteArrayOutputStream();

        final ExecResult execResult = execOperations.exec(spec -> {
            // The redundant cast is to silence a compiler warning.
            spec.setCommandLine((Object[]) args);
            spec.setStandardOutput(stdout);
            spec.setErrorOutput(stderr);
            spec.setIgnoreExitValue(true);
        });
        return new Result(execResult.getExitValue(), stdout.toString(), stderr.toString());
    }

    /**
     * An immutable class that represents the results of a Docker search from {@link #getDockerAvailability()}}.
     */
    public record DockerAvailability(
        /*
         * Indicates whether Docker is available and meets the required criteria.
         * True if, and only if, Docker is:
         * <ul>
         *     <li>Installed</li>
         *     <li>Executable</li>
         *     <li>Is at least version compatible with minimum version</li>
         *     <li>Can execute a command that requires privileges</li>
         * </ul>
         */
        boolean isAvailable,

        // True if docker-compose is available.
        boolean isComposeAvailable,

        // True if the installed Docker version is &gt,= 17.05
        boolean isVersionHighEnough,

        // The path to the Docker CLI, or null
        String path,

        // The path to the Docker Compose CLI, or null
        String dockerComposePath,

        // The installed Docker version, or null
        Version version,

        // Information about the last command executes while probing Docker, or null.
        Result lastCommand,

        // Supported build architectures
        Set<Architecture> supportedArchitectures
    ) {}

    /**
     * This class models the result of running a command. It captures the exit code, standard output and standard error.
     */
    private record Result(int exitCode, String stdout, String stderr) {

        boolean isSuccess() {
            return exitCode == 0;
        }
    }

    interface Parameters extends BuildServiceParameters {
        File getExclusionsFile();

        void setExclusionsFile(File exclusionsFile);
    }
}
