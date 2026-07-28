/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal;

import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.internal.info.BuildParameterExtension;
import org.elasticsearch.gradle.internal.info.GlobalBuildInfoPlugin;
import org.gradle.api.Plugin;
import org.gradle.api.Project;

/**
 * Base plugin applied to every Elasticsearch subproject. Replaces the former
 * {@code elasticsearch.base.gradle} precompiled script plugin. Registers
 * standard project metadata and makes the build-wide {@link BuildParameterExtension}
 * and {@code versions} map available as project extensions so that build scripts
 * can access them directly without triggering deprecated implicit project-hierarchy
 * property lookup.
 */
public class ElasticsearchBasePlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        // Ensure the root project has GlobalBuildInfoPlugin applied (idempotent).
        project.getRootProject().getPluginManager().apply(GlobalBuildInfoPlugin.class);

        // Standard subproject metadata.
        project.setDescription("Elasticsearch subproject " + project.getPath());
        project.setGroup("org.elasticsearch");
        project.setVersion(VersionProperties.getElasticsearch());

        // Resolve the BuildParameterExtension from the root project and register it
        // as a typed extension on this project. This avoids the deprecated implicit
        // project-hierarchy property lookup that would otherwise occur when build
        // scripts access `buildParams` directly.
        BuildParameterExtension buildParams = project.getRootProject().getExtensions().getByType(BuildParameterExtension.class);
        if (project.getExtensions().findByType(BuildParameterExtension.class) == null) {
            project.getExtensions().add(BuildParameterExtension.class, BuildParameterExtension.EXTENSION_NAME, buildParams);
        }

        // Register the dependency-version map as a project extra property so build
        // scripts can use `versions.someLib` without hierarchy lookup.
        project.getExtensions().getExtraProperties().set("versions", VersionProperties.getVersions());
    }
}
