/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.tasks.SourceSet;

import java.nio.file.Files;

record ModularPathsExtension(
    Project project,
    SourceSet sourceSet,
    Configuration compileModulePathConfiguration,
    Configuration runtimeModulePathConfiguration
) {

    public boolean hasModuleDescriptor() {
        return sourceSet.getAllJava().getSrcDirs().stream().map(dir -> dir.toPath().resolve("module-info.java")).anyMatch(Files::exists);
    }
}
