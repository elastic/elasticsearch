/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.util;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.packaging.util.FileExistenceMatchers.fileExists;
import static org.hamcrest.MatcherAssert.assertThat;

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
        return new DockerRun();
    }

    public DockerRun distribution(Distribution distribution) {
        this.distribution = Objects.requireNonNull(distribution);
        return this;
    }

    public DockerRun envVars(Map<String, String> envVars) {
        if (envVars != null) {
            this.envVars.putAll(envVars);
        }
        return this;
    }

    public DockerRun volumes(Map<Path, Path> volumes) {
        if (volumes != null) {
            this.volumes.putAll(volumes);
        }
        return this;
    }

    public DockerRun uid(Integer uid, Integer gid) {
        if (uid == null) {
            if (gid != null) {
                throw new IllegalArgumentException("Cannot override GID without also overriding UID");
            }
        }
        this.uid = uid;
        this.gid = gid;
        return this;
    }

    public DockerRun memory(String memoryLimit) {
        if (memoryLimit != null) {
            this.memory = memoryLimit;
        }
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

        // The container won't run without configuring discovery
        cmd.add("--env discovery.type=single-node");

        // Map ports in the container to the host, so that we can send requests
        cmd.add("--publish 9200:9200");
        cmd.add("--publish 9300:9300");

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
        String suffix;

        switch (distribution.packaging) {
            case DOCKER:
                suffix = "";
                break;

            case DOCKER_UBI:
                suffix = "-ubi8";
                break;

            case DOCKER_IRON_BANK:
                suffix = "-ironbank";
                break;

            default:
                throw new IllegalStateException("Unexpected distribution packaging type: " + distribution.packaging);
        }

        return "elasticsearch" + suffix + ":test";
    }
}
