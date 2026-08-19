/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.flakiness;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

/**
 * A Gradle-free snapshot of one project in the build, captured from that project's <em>own</em> configured
 * model (see {@link FlakinessProjectResolve}). It is the authoritative unit the pure {@link RefResolver}
 * operates on.
 *
 * <p>Keeping this a plain record (no Gradle types) is deliberate: it is what lets the resolution logic be
 * unit-tested without Gradle TestKit, and it is what may safely be serialized into a configuration-cache
 * entry. Crucially, every field here is derived from the project's own live model with <b>no cross-project
 * access</b> - that is what keeps the design isolated-projects-clean (see JAVA_RESOLVER_NOTES.md).
 *
 * <p>It deliberately carries no {@code Test}-task facts: those cannot be snapshotted at plain configuration
 * time (see {@link TestTaskInfo}), and realizing them is expensive enough that a project only pays for it
 * once it has claimed a ref (see {@link FlakinessProjectResolve#ownsAnyRef}).
 *
 * @param projectPath  Gradle project path, e.g. {@code :x-pack:plugin:esql}
 * @param projectDir   absolute project directory (used for authoritative path-&gt;project resolution)
 * @param sourceSets   this project's flakiness-relevant test source sets (a subset of
 *                     {@code test}/{@code internalClusterTest}/{@code javaRestTest}/{@code yamlRestTest})
 */
public record ProjectInfo(String projectPath, Path projectDir, List<SourceSetInfo> sourceSets) {

    /** The named source set, if this project has it configured. */
    public Optional<SourceSetInfo> sourceSet(String name) {
        return sourceSets.stream().filter(s -> s.name().equals(name)).findFirst();
    }
}
