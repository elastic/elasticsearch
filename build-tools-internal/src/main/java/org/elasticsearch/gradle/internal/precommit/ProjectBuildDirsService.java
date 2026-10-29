/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.precommit;

import org.gradle.api.provider.MapProperty;
import org.gradle.api.services.BuildService;
import org.gradle.api.services.BuildServiceParameters;

import java.io.File;
import java.util.Map;

/**
 * Shared build service containing a snapshot of project build directories keyed to project paths.
 *
 * <p>The settings plugin seeds this map from the {@link org.gradle.api.initialization.ProjectDescriptor} tree so
 * split-packages auditing can name the owning project for classpath entries without scanning live {@code Project}
 * instances (which stays friendly to isolated projects).
 *
 * <p>Consumers should look up this service by {@link #NAME} only when present. Some builds intentionally do not apply
 * {@code elasticsearch.project-build-dirs} (for example serverless TestKit builds); those consumers should fall back
 * to a directory-path label.
 */
public abstract class ProjectBuildDirsService implements BuildService<ProjectBuildDirsService.Params> {

    /** Shared-services registration name for this mapping service. */
    public static final String NAME = "elasticsearchProjectBuildDirs";

    private final Map<File, String> projectBuildDirs;

    public ProjectBuildDirsService() {
        this.projectBuildDirs = Map.copyOf(getParameters().getProjectBuildDirs().get());
    }

    /** Returns the immutable build-directory to project-path mapping seeded in {@link Params}. */
    public Map<File, String> getProjectBuildDirs() {
        return projectBuildDirs;
    }

    /** Parameters used to seed this service once at registration time. */
    public interface Params extends BuildServiceParameters {
        MapProperty<File, String> getProjectBuildDirs();
    }
}
