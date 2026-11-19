/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.docker;

import org.junit.Test;

import java.util.function.Supplier;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThrows;

/// Covers which exporter loads the image into the daemon. A wrong choice is only noticed when the image is inspected after the
/// build: `type=image` against a `docker-container` builder leaves the image in the builder, not in the daemon.
public class DockerExporterTests {

    private static final String LOAD = "type=docker";
    private static final String LOAD_REWRITING_TIMESTAMPS = "type=docker,rewrite-timestamp=true";
    private static final String UNPACK_FALSE = "type=image,unpack=false,rewrite-timestamp=true";

    private static final String DAEMON_BUILDER = "Name:          desktop-linux\nDriver:        docker\nLast Activity: 2026-11-05\n";
    private static final String CONTAINER_BUILDER =
        "Name:          ci-builder\nDriver:        docker-container\nLast Activity: 2026-11-05\n";

    private static final String CONTAINERD_STORE = "[[driver-type io.containerd.snapshotter.v1]]";
    private static final String CLASSIC_STORE = "[[Backing Filesystem extfs] [Supports d_type true]]";

    @Test
    public void withoutTimestampRewritingNoDockerCommandIsRun() {
        final var spec = DockerExporter.daemonSpec(false, false, "ci-builder", notQueried(), notQueried());
        assertThat(spec, equalTo(LOAD));
    }

    @Test
    public void nativeBuildWithoutOverrideUsesTheDaemonBuilder() {
        // the selection made with `docker buildx use` is not honored by plain `docker build`, so inspecting is not needed
        assertThat(DockerExporter.daemonSpec(true, false, null, notQueried(), () -> CONTAINERD_STORE), equalTo(UNPACK_FALSE));
        assertThat(DockerExporter.daemonSpec(true, false, null, notQueried(), () -> CLASSIC_STORE), equalTo(LOAD_REWRITING_TIMESTAMPS));
    }

    @Test
    public void nativeBuildWithBlankOverrideUsesTheDaemonBuilder() {
        assertThat(DockerExporter.daemonSpec(true, false, "", notQueried(), () -> CONTAINERD_STORE), equalTo(UNPACK_FALSE));
        assertThat(DockerExporter.daemonSpec(true, false, "  ", notQueried(), () -> CONTAINERD_STORE), equalTo(UNPACK_FALSE));
    }

    @Test
    public void nativeBuildWithContainerBuilderOverrideKeepsTypeDocker() {
        final var spec = DockerExporter.daemonSpec(true, false, "ci-builder", () -> CONTAINER_BUILDER, () -> CONTAINERD_STORE);
        assertThat(spec, equalTo(LOAD_REWRITING_TIMESTAMPS));
    }

    @Test
    public void nativeBuildWithDaemonBuilderOverrideUsesUnpackFalse() {
        final var spec = DockerExporter.daemonSpec(true, false, "desktop-linux", () -> DAEMON_BUILDER, () -> CONTAINERD_STORE);
        assertThat(spec, equalTo(UNPACK_FALSE));
    }

    @Test
    public void nativeBuildWithOverrideOnClassicStoreKeepsTypeDocker() {
        assertThat(
            DockerExporter.daemonSpec(true, false, "desktop-linux", () -> DAEMON_BUILDER, () -> CLASSIC_STORE),
            equalTo(LOAD_REWRITING_TIMESTAMPS)
        );
        assertThat(
            DockerExporter.daemonSpec(true, false, "ci-builder", () -> CONTAINER_BUILDER, () -> CLASSIC_STORE),
            equalTo(LOAD_REWRITING_TIMESTAMPS)
        );
    }

    @Test
    public void crossPlatformBuildInspectsTheSelectedBuilder() {
        assertThat(
            DockerExporter.daemonSpec(true, true, null, () -> CONTAINER_BUILDER, () -> CONTAINERD_STORE),
            equalTo(LOAD_REWRITING_TIMESTAMPS)
        );
        assertThat(DockerExporter.daemonSpec(true, true, null, () -> DAEMON_BUILDER, () -> CONTAINERD_STORE), equalTo(UNPACK_FALSE));
    }

    @Test
    public void failedBuilderInspectionIsNotGuessed() {
        final var failure = new IllegalStateException("inspection failed");
        final var thrown = assertThrows(
            IllegalStateException.class,
            () -> DockerExporter.daemonSpec(true, true, null, () -> { throw failure; }, () -> CONTAINERD_STORE)
        );
        assertThat(thrown, sameInstance(failure));
    }

    @Test
    public void failedDaemonInspectionIsTreatedAsClassicStore() {
        assertThat(DockerExporter.daemonSpec(true, false, null, notQueried(), () -> null), equalTo(LOAD_REWRITING_TIMESTAMPS));
    }

    private static Supplier<String> notQueried() {
        return () -> { throw new AssertionError("docker should not have been queried"); };
    }
}
