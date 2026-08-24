/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.conventions;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.bundling.Zip;

/**
 * Registers the {@code zipDraSnapshotMavenAggregation} task, which produces a
 * DRA-shaped copy of the Central Portal aggregation zip built by
 * {@code com.gradleup.nmcp.aggregation}.
 *
 * <p>The applying project is expected to have {@code com.gradleup.nmcp.aggregation}
 * applied so the upstream {@code zipAggregation} task exists. This plugin
 * intentionally does not touch {@code zipAggregation}; that task's output must
 * remain Sonatype Central Portal compliant.
 *
 * <p>See {@link PrepareDraSnapshotMavenAggregation} for the details of the
 * rewrite.
 */
public class DraMavenAggregationPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        Provider<String> version = project.provider(() -> project.getVersion().toString());

        TaskProvider<PrepareDraSnapshotMavenAggregation> prepare = project.getTasks().register(
            "prepareDraSnapshotMavenAggregation",
            PrepareDraSnapshotMavenAggregation.class,
            task -> {
                task.setGroup("dra");
                task.setDescription(
                    "Extracts the maven aggregation zip into the DRA snapshot layout: "
                        + "renames Maven-timestamped snapshot filenames back to -SNAPSHOT "
                        + "and generates per-version maven-metadata.xml."
                );
                task.getSourceZip().set(
                    project.getTasks().named("zipAggregation", Zip.class).flatMap(Zip::getArchiveFile)
                );
                task.getVersion().set(version);
                task.getOutputDir().set(project.getLayout().getBuildDirectory().dir("dra-maven-aggregation"));
            }
        );

        project.getTasks().register("zipDraSnapshotMavenAggregation", Zip.class, zip -> {
            zip.setGroup("dra");
            zip.setDescription(
                "Repackages the maven aggregation zip into the layout expected by "
                    + "DRA snapshot publishing (snapshots.elastic.co / artifacts.elastic.co)."
            );
            zip.getArchiveBaseName().set("elasticsearch-dra-maven-aggregation");
            zip.getArchiveVersion().set(version);
            zip.getDestinationDirectory().set(project.getLayout().getBuildDirectory().dir("distributions"));
            zip.from(prepare.flatMap(PrepareDraSnapshotMavenAggregation::getOutputDir));
        });
    }
}
