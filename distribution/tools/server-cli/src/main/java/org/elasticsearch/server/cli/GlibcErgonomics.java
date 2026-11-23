/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.server.cli;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.util.concurrent.EsExecutors;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Tunes glibc malloc for the Elasticsearch server process. Only applies on Linux.
 *
 * <p> With glibc's defaults, Elasticsearch can retain GBs of freed native memory, which comes out of the page cache, or gets the
 * process killed in a memory-limited container. We set:
 * <ul>
 *     <li>MAX one glibc arena per processor, the count that also sizes our thread pools;</li>
 *     <li>fixed mmap and trim thresholds of 1 MB.</li>
 * </ul>
 * The hourly trim of the native heap, which returns the whole free pages in glibc's free lists to the OS, is a JVM option set in
 * {@link SystemJvmOptions}.
 */
final class GlibcErgonomics {

    static final String MALLOC_ARENA_MAX = "MALLOC_ARENA_MAX";
    static final String MALLOC_MMAP_THRESHOLD = "MALLOC_MMAP_THRESHOLD_";
    static final String MALLOC_TRIM_THRESHOLD = "MALLOC_TRIM_THRESHOLD_";

    /**
     * The glibc mmap and trim threshold we set, in bytes.
     */
    static final long MALLOC_THRESHOLD_BYTES = ByteSizeUnit.MB.toBytes(1);

    /**
     * A glibc malloc environment variable we set by default, and the glibc tunable that the variable is an alias for.
     */
    private record MallocDefault(String name, String tunable, long value) {}

    private GlibcErgonomics() {
        throw new AssertionError("No instances intended");
    }

    /**
     * Chooses the glibc malloc environment variables to add to the server process environment. A default is left out if the user
     * already configured it: {@code environment} has the variable (from the launcher's environment, or from a
     * {@link ServerCli#GLIBC_ENV_SYSPROP_PREFIX} system property), or its {@code GLIBC_TUNABLES} sets the tunable the variable is an
     * alias for.
     *
     * @param environment the server process environment so far
     * @param nodeSettings the node settings
     * @param isLinux whether the server runs on Linux
     * @return the environment variables to add, by name
     */
    static Map<String, String> chooseEnvironment(Map<String, String> environment, Settings nodeSettings, boolean isLinux) {
        if (isLinux == false) {
            return Map.of();
        }
        final Set<String> userTunables = tunableNames(environment.get("GLIBC_TUNABLES"));
        final List<MallocDefault> defaults = List.of(
            new MallocDefault(MALLOC_ARENA_MAX, "glibc.malloc.arena_max", EsExecutors.allocatedProcessors(nodeSettings)),
            new MallocDefault(MALLOC_MMAP_THRESHOLD, "glibc.malloc.mmap_threshold", MALLOC_THRESHOLD_BYTES),
            new MallocDefault(MALLOC_TRIM_THRESHOLD, "glibc.malloc.trim_threshold", MALLOC_THRESHOLD_BYTES)
        );
        final Map<String, String> chosen = new LinkedHashMap<>();
        for (MallocDefault mallocDefault : defaults) {
            boolean userSet = environment.containsKey(mallocDefault.name()) || userTunables.contains(mallocDefault.tunable());
            if (userSet == false) {
                chosen.put(mallocDefault.name(), Long.toString(mallocDefault.value()));
            }
        }
        return chosen;
    }

    /**
     * @return the names of the tunables in a {@code GLIBC_TUNABLES} value ({@code name=value} entries separated by {@code :})
     */
    private static Set<String> tunableNames(String glibcTunables) {
        if (glibcTunables == null) {
            return Set.of();
        }
        return Arrays.stream(glibcTunables.split(":")).map(entry -> {
            int equals = entry.indexOf('=');
            return equals < 0 ? entry : entry.substring(0, equals);
        }).collect(Collectors.toUnmodifiableSet());
    }
}
