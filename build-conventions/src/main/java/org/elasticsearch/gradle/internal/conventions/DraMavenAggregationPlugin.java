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
import org.gradle.api.tasks.bundling.Zip;

/**
 * Registers the {@code prepareDraSnapshotMavenAggregation} task, which produces a
 * DRA-shaped copy of the Central Portal aggregation zip built by
 * {@code com.gradleup.nmcp.aggregation}.
 *
 * <p>The applying project is expected to have {@code com.gradleup.nmcp.aggregation}
 * applied so the upstream {@code nmcpZipAggregation} task exists. This plugin
 * intentionally does not touch {@code nmcpZipAggregation}; that task's output must
 * remain Sonatype Central Portal compliant.
 *
 * <p>The task emits an <em>exploded</em> maven tree under
 * {@code build/dra-maven-aggregation/} rather than a zip: the DRA publish step
 * ({@code .buildkite/scripts/dra-maven-snapshots-publish.sh}) runs inline in the
 * same workspace and uploads the tree straight to S3, so re-zipping here just to
 * unzip it again there would be wasted work.
 *
 * <p>To avoid zipping on the DRA path entirely, the task consumes
 * {@code nmcpZipAggregation}'s copy-spec source (the already-extracted per-project
 * publications) instead of the {@code aggregation.zip} archive, so that zip is
 * never built for DRA. See {@link PrepareDraSnapshotMavenAggregation}.
 */
public class DraMavenAggregationPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        Provider<String> version = project.provider(() -> project.getVersion().toString());

        project.getTasks().register(
            "prepareDraSnapshotMavenAggregation",
            PrepareDraSnapshotMavenAggregation.class,
            task -> {
                task.setGroup("dra");
                task.setDescription(
                    "Copies the maven aggregation content into the DRA snapshot layout: "
                        + "renames Maven-timestamped snapshot filenames back to -SNAPSHOT "
                        + "and generates per-version maven-metadata.xml."
                );
                // Reuse nmcpZipAggregation's copy-spec source (the extracted
                // per-project publications) rather than its archive output, so
                // the aggregation zip is never built on the DRA path. The
                // lookup is deferred inside a plain provider (rather than
                // resolved eagerly here, or mapped off the TaskProvider):
                //  - TaskProvider.map would add a dependency on nmcpZipAggregation
                //    itself, forcing the zip to build;
                //  - resolving named("nmcpZipAggregation") eagerly at apply() time
                //    would couple this plugin to being applied *after*
                //    nmcp.aggregation.
                // getSource()'s FileTree already carries the build dependencies
                // of the underlying publication tasks, so @InputFiles
                // establishes the correct task ordering on its own.
                task.getSource().from(
                    project.provider(() -> project.getTasks().named("nmcpZipAggregation", Zip.class).get().getSource())
                );
                task.getVersion().set(version);
                task.getOutputDir().set(project.getLayout().getBuildDirectory().dir("dra-maven-aggregation"));
            }
        );
    }
}
