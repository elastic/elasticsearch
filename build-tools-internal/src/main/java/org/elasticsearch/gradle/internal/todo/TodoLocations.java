/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License, v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.todo;

import java.nio.file.Path;

/**
 * Turns absolute source paths into the repository-relative fields the report stores.
 */
final class TodoLocations {

    private TodoLocations() {}

    /** First segment of a Gradle path: {@code :x-pack:plugin:esql} is {@code x-pack}. */
    static String area(String modulePath) {
        String path = modulePath.startsWith(":") ? modulePath.substring(1) : modulePath;
        int colon = path.indexOf(':');
        if (colon < 0) {
            return path;
        }
        return path.substring(0, colon);
    }

    /**
     * Name of the source set that contains {@code file}, taken from the {@code src/<name>/} directory.
     * Files that are not under {@code src} are {@code unknown}.
     */
    static String sourceSet(Path projectDirectory, Path file) {
        Path relative = projectDirectory.toAbsolutePath().normalize().relativize(file.toAbsolutePath().normalize());
        for (int i = 0; i < relative.getNameCount() - 1; i++) {
            if (relative.getName(i).toString().equals("src")) {
                return relative.getName(i + 1).toString();
            }
        }
        return "unknown";
    }

    /** Path of {@code file} relative to the repository root, with forward slashes. */
    static String repoRelative(Path settingsDirectory, Path file) {
        Path relative = settingsDirectory.toAbsolutePath().normalize().relativize(file.toAbsolutePath().normalize());
        return relative.toString().replace('\\', '/');
    }

    /** True when {@code file} lives under the project's {@code build} directory. */
    static boolean isUnderBuild(Path projectDirectory, Path file) {
        Path relative = projectDirectory.toAbsolutePath().normalize().relativize(file.toAbsolutePath().normalize());
        return relative.getNameCount() > 0 && relative.getName(0).toString().equals("build");
    }
}
