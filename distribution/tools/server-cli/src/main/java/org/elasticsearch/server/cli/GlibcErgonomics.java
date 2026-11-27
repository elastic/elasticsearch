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
    static final String MALLOC_ARENA_TEST = "MALLOC_ARENA_TEST";
    static final String MALLOC_MMAP_THRESHOLD = "MALLOC_MMAP_THRESHOLD_";
    static final String MALLOC_TRIM_THRESHOLD = "MALLOC_TRIM_THRESHOLD_";

    /**
     * The glibc mmap and trim threshold we set, in bytes.
     */
    static final long MALLOC_THRESHOLD_BYTES = ByteSizeUnit.MB.toBytes(1);

    /**
     * A glibc malloc parameter: the environment variable linked to it, and its glibc tunable alias.
     */
    private record MallocParameter(String variable, String tunable) {}

    private static final MallocParameter ARENA_MAX = new MallocParameter(MALLOC_ARENA_MAX, "glibc.malloc.arena_max");
    private static final MallocParameter ARENA_TEST = new MallocParameter(MALLOC_ARENA_TEST, "glibc.malloc.arena_test");
    private static final MallocParameter MMAP_THRESHOLD = new MallocParameter(MALLOC_MMAP_THRESHOLD, "glibc.malloc.mmap_threshold");
    private static final MallocParameter TRIM_THRESHOLD = new MallocParameter(MALLOC_TRIM_THRESHOLD, "glibc.malloc.trim_threshold");

    /**
     * A value we set for {@code parameter}, unless the user configured any of {@code userParameters}.
     */
    private record MallocDefault(MallocParameter parameter, long value, List<MallocParameter> userParameters) {}

    private GlibcErgonomics() {
        throw new AssertionError("No instances intended");
    }

    /**
     * Chooses the glibc malloc environment variables to add to the server process environment. A default is left out if the user
     * already configured it: {@code environment} has the variable (from the launcher's environment, or from a
     * {@link ServerCli#GLIBC_ENV_SYSPROP_PREFIX} system property), or its {@code GLIBC_TUNABLES} sets the tunable the variable is an
     * alias for. The arena max is also left out if the user configured the arena test, because glibc ignores it
     * once the arena max is set.
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
            new MallocDefault(ARENA_MAX, EsExecutors.allocatedProcessors(nodeSettings), List.of(ARENA_MAX, ARENA_TEST)),
            new MallocDefault(MMAP_THRESHOLD, MALLOC_THRESHOLD_BYTES, List.of(MMAP_THRESHOLD)),
            new MallocDefault(TRIM_THRESHOLD, MALLOC_THRESHOLD_BYTES, List.of(TRIM_THRESHOLD))
        );
        final Map<String, String> chosen = new LinkedHashMap<>();
        for (MallocDefault mallocDefault : defaults) {
            boolean userSet = mallocDefault.userParameters()
                .stream()
                .anyMatch(parameter -> environment.containsKey(parameter.variable()) || userTunables.contains(parameter.tunable()));
            if (userSet == false) {
                chosen.put(mallocDefault.parameter().variable(), Long.toString(mallocDefault.value()));
            }
        }
        return chosen;
    }

    /**
     * @return the names of the tunables that a {@code GLIBC_TUNABLES} value sets ({@code name=value} entries separated by {@code :}).
     *         glibc ignores an entry without a value, so it does not count.
     */
    private static Set<String> tunableNames(String glibcTunables) {
        if (glibcTunables == null) {
            return Set.of();
        }
        return Arrays.stream(glibcTunables.split(":"))
            .filter(entry -> entry.indexOf('=') > 0)
            .map(entry -> entry.substring(0, entry.indexOf('=')))
            .collect(Collectors.toUnmodifiableSet());
    }
}
