/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.fixtures.testcontainers;

import org.elasticsearch.test.fixtures.minio.MinioTestContainer;
import org.junit.Assume;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DockerEnvironmentAwareTestContainer extends GenericContainer<MinioTestContainer> {
    private static final String DOCKER_ON_LINUX_EXCLUSIONS_FILE = ".ci/dockerOnLinuxExclusions";

    private static final boolean CI = Boolean.parseBoolean(System.getProperty("CI", "false"));
    private static final boolean EXCLUDED_OS_FOUND = CI == false || isExcludedOsFound();

    protected static final Logger LOGGER = LoggerFactory.getLogger(DockerEnvironmentAwareTestContainer.class);

    public DockerEnvironmentAwareTestContainer(ImageFromDockerfile imageFromDockerfile) {
        super(imageFromDockerfile);
    }

    @Override
    public void start() {
        Assume.assumeTrue("Docker is not available", shouldDockerBeAvailable());
        super.start();
    }

    private boolean shouldDockerBeAvailable() {
        return CI == false || EXCLUDED_OS_FOUND == false;
    }

    static String deriveId(Map<String, String> values) {
        return values.get("ID") + "-" + values.get("VERSION_ID");
    }

    private static boolean isExcludedOsFound() {
        if (System.getProperty("os.name").toLowerCase().startsWith("windows")) {
            return true;
        }
        final Path osRelease = Paths.get("/etc/os-release");
        if (Files.exists(osRelease)) {
            Map<String, String> values;

            try {
                final List<String> osReleaseLines = Files.readAllLines(osRelease);
                values = parseOsRelease(osReleaseLines);
            } catch (IOException e) {
                throw new RuntimeException("Failed to read /etc/os-release", e);
            }

            final String id = deriveId(values);
            final boolean excluded = getLinuxExclusionList().contains(id);

            if (excluded) {
                LOGGER.warn("Linux OS id [{}] is present in the Docker exclude list. Tasks requiring Docker will be disabled.", id);
            }

            return excluded;
        } else {
            LOGGER.warn("No release info found at /etc/os-release");
        }

        return false;
    }

    private static List<String> getLinuxExclusionList() {
        File exclusionsFile = new File(DOCKER_ON_LINUX_EXCLUSIONS_FILE);
        if (exclusionsFile.exists()) {
            try {
                return Files.readAllLines(exclusionsFile.toPath())
                    .stream()
                    .map(String::trim)
                    .filter(line -> (line.isEmpty() || line.startsWith("#")) == false)
                    .collect(Collectors.toList());
            } catch (IOException e) {
                throw new RuntimeException("Failed to read " + exclusionsFile.getAbsolutePath(), e);
            }
        } else {
            return Collections.emptyList();
        }
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
}
