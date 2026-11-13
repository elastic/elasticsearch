/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.distribution;

import java.util.ArrayList;
import java.util.List;

/**
 * The platform specific directories that modules (e.g. ML) ship native code in. A platform specific distribution
 * only ships the directory of its own platform, so the others have to be excluded when assembling it.
 *
 * <p>These directories use the {@code x86_64} nomenclature, whereas distributions and bundled JDKs are classified
 * as {@code x64}.
 */
public final class ModulePlatforms {

    private static final List<String> DIRECTORIES = List.of(
        "linux-x86_64",
        "linux-aarch64",
        "windows-x86_64",
        "darwin-x86_64",
        "darwin-aarch64"
    );

    private ModulePlatforms() {}

    /**
     * The name of the directory modules ship native code for the given platform and architecture in.
     *
     * @param platform one of {@code linux}, {@code darwin} or {@code windows}
     * @param architecture the distribution architecture classifier, {@code x64} or {@code aarch64}
     */
    public static String directory(String platform, String architecture) {
        String directory = platform + "-" + ("x64".equals(architecture) ? "x86_64" : architecture);
        if (DIRECTORIES.contains(directory) == false) {
            throw new IllegalArgumentException("Unknown module platform [" + platform + "-" + architecture + "]");
        }
        return directory;
    }

    /**
     * The platform specific module directories that must be excluded from a distribution for the given platform and
     * architecture. A platform independent distribution ({@code platform == null}) keeps all of them.
     */
    public static List<String> excludedDirectories(String platform, String architecture) {
        if (platform == null) {
            return List.of();
        }
        List<String> excluded = new ArrayList<>(DIRECTORIES);
        excluded.remove(directory(platform, architecture));
        return List.copyOf(excluded);
    }
}
