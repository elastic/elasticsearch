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

    public DockerRun extraArgs(String... args) {
        Collections.addAll(this.extraArgs, args);
        return this;
    }

    String build() {
        final List<String> cmd = new ArrayList<>();

        cmd.add("docker run");

        // Run the container in the background
        cmd.add("--detach");

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

    static String getImageName(Distribution distribution) {
        return distribution.flavor.name + (distribution.packaging == Distribution.Packaging.DOCKER_UBI ? "-ubi8" : "") + ":test";
    }
}
