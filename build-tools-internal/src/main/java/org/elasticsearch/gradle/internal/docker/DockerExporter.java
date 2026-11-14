/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.docker;

import java.util.function.Supplier;

/// Chooses the `--output` value that loads a built image into the local Docker daemon.
final class DockerExporter {

    private DockerExporter() {}

    /// The `--output` value that loads the image into the local daemon, with normalized timestamps if requested
    /// because the image is published later.
    ///
    /// If the daemon itself is the builder and uses the containerd image store, `type=docker` implies
    /// `unpack=true`, which conflicts with `rewrite-timestamp`, so `type=image,unpack=false` is used.
    /// Other builders (e.g. `docker-container`) need `type=docker`: `type=image` would export into
    /// the builder, not the daemon.
    ///
    /// @param isCrossPlatform whether the build uses `docker buildx build` rather than plain `docker build`
    /// @param builderOverride the `BUILDX_BUILDER` value, or `null` if unset
    /// @param buildxInspect supplies the output of `docker buildx inspect`. It must throw if the inspection fails, since the
    ///                      driver of the selected builder cannot be guessed
    /// @param daemonDriverStatus supplies the output of `docker info --format '{{ .DriverStatus }}'`, or `null`
    ///                           if it failed
    static String daemonSpec(
        boolean rewriteTimestamps,
        boolean isCrossPlatform,
        String builderOverride,
        Supplier<String> buildxInspect,
        Supplier<String> daemonDriverStatus
    ) {
        if (rewriteTimestamps == false) {
            return "type=docker";
        }
        if (builderIsDaemon(isCrossPlatform, builderOverride, buildxInspect) && usesContainerdImageStore(daemonDriverStatus.get())) {
            return "type=image,unpack=false,rewrite-timestamp=true";
        }
        return "type=docker,rewrite-timestamp=true";
    }

    private static boolean builderIsDaemon(boolean isCrossPlatform, String builderOverride, Supplier<String> buildxInspect) {
        // Plain `docker build` ignores `docker buildx use` but honors `BUILDX_BUILDER`, so without the variable it always
        // uses the daemon's builder. Otherwise, inspect which honors both ways of selecting a builder.
        if (isCrossPlatform == false && (builderOverride == null || builderOverride.isBlank())) {
            return true;
        }
        return buildxInspect.get().lines().anyMatch(line -> line.trim().matches("Driver:\\s+docker"));
    }

    private static boolean usesContainerdImageStore(String daemonDriverStatus) {
        return daemonDriverStatus != null && daemonDriverStatus.contains("io.containerd.snapshotter.v1");
    }
}
