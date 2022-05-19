/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.util.docker;

import org.elasticsearch.packaging.util.Distribution;
import org.elasticsearch.packaging.util.Platforms;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;
import static org.elasticsearch.packaging.util.FileExistenceMatchers.fileExists;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * A utility class for constructing a {@code docker run} command line from Java.
 */
public class DockerRun {

    private Distribution distribution;
    private final Map<String, String> envVars = new HashMap<>();
    private final Map<Path, Path> volumes = new HashMap<>();
    private Integer uid;
    private Integer gid;
    private final List<String> extraArgs = new ArrayList<>();
    private String memory = "2g"; // default to 2g memory limit

    private DockerRun() {}

    public static DockerRun builder() {
        // Disable this setting by default in the Docker tests
        return new DockerRun().envVar("ingest.geoip.downloader.enabled", "false");
    }

    public DockerRun distribution(Distribution distribution) {
        this.distribution = requireNonNull(distribution);
        return this;
    }

    public DockerRun envVar(String key, String value) {
        this.envVars.put(requireNonNull(key), requireNonNull(value));
        return this;
    }

    public DockerRun volume(Path from, String to) {
        requireNonNull(from);
        if (Files.exists(from) == false) {
            throw new RuntimeException("Path [" + from + "] does not exist");
        }
        this.volumes.put(requireNonNull(from), Path.of(requireNonNull(to)));
        return this;
    }

    public DockerRun volume(Path from, Path to) {
        this.volumes.put(requireNonNull(from), requireNonNull(to));
        return this;
    }

    /**
     * Sets the UID that the container is run with, and the GID too if specified.
     *
     * @param uidToUse the UID to use, or {@code null} to use the image default
     * @param gidToUse the GID to use, or {@code null} to use the image default
     * @return the current builder
     */
    public DockerRun uid(Integer uidToUse, Integer gidToUse) {
        if (uidToUse == null) {
            if (gidToUse != null) {
                throw new IllegalArgumentException("Cannot override GID without also overriding UID");
            }
        }
        this.uid = uidToUse;
        this.gid = gidToUse;
        return this;
    }

    public DockerRun memory(String memoryLimit) {
        this.memory = requireNonNull(memoryLimit);
        return this;
    }

    public DockerRun extraArgs(String... args) {
        Collections.addAll(this.extraArgs, args);
        return this;
    }

    String build() {
        final List<String> cmd = new ArrayList<>();

        cmd.add("docker run");

        // Run the container in the background
        cmd.add("--detach");

        // Limit container memory
        cmd.add("--memory " + memory);

        this.envVars.forEach((key, value) -> cmd.add("--env " + key + "=\"" + value + "\""));

        // Map ports in the container to the host, so that we can send requests
        // allow ports to be overridden by tests
        if (this.extraArgs.stream().anyMatch(arg -> arg.startsWith("-p") || arg.startsWith("--publish")) == false) {
            cmd.add("--publish 9200:9200");
            cmd.add("--publish 9300:9300");
        }

        // Bind-mount any volumes
        volumes.forEach((localPath, containerPath) -> {
            assertThat(localPath, fileExists());

            if (Platforms.WINDOWS == false && System.getProperty("user.name").equals("root") && uid == null) {
                // The tests are running as root, but the process in the Docker container runs as `elasticsearch` (UID 1000),
                // so we need to ensure that the container process is able to read the bind-mounted files.
                //
                // NOTE that we don't do this if a UID is specified - in that case, we assume that the caller knows
                // what they're doing!
                Docker.sh.run("chown -R 1000:0 " + localPath);
            }
            cmd.add("--volume \"" + localPath + ":" + containerPath + "\"");
        });

        if (uid != null) {
            cmd.add("--user");
            if (gid != null) {
                cmd.add(uid + ":" + gid);
            } else {
                cmd.add(uid.toString());
            }
        }

        cmd.addAll(this.extraArgs);

        // Image name
        cmd.add(getImageName(distribution));

        return String.join(" ", cmd);
    }

    /**
     * Derives a Docker image name from the supplied distribution.
     * @param distribution the distribution to use
     * @return an image name
     */
    public static String getImageName(Distribution distribution) {
        String suffix = switch (distribution.packaging) {
            case DOCKER -> "";
            case DOCKER_UBI -> "-ubi8";
            case DOCKER_IRON_BANK -> "-ironbank";
            case DOCKER_CLOUD -> "-cloud";
            case DOCKER_CLOUD_ESS -> "-cloud-ess";
            default -> throw new IllegalStateException("Unexpected distribution packaging type: " + distribution.packaging);
        };

        return "elasticsearch" + suffix + ":test";
    }
}
