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
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

public class GlibcErgonomicsTests extends ESTestCase {

    public void testEnvironmentDefaultsOnLinux() {
        Map<String, String> chosen = GlibcErgonomics.chooseEnvironment(Map.of(), Settings.EMPTY, true);
        assertThat(
            chosen,
            equalTo(
                Map.of(
                    "MALLOC_ARENA_MAX",
                    Integer.toString(EsExecutors.allocatedProcessors(Settings.EMPTY)),
                    "MALLOC_MMAP_THRESHOLD_",
                    "1048576",
                    "MALLOC_TRIM_THRESHOLD_",
                    "1048576"
                )
            )
        );
    }

    public void testNothingOutsideLinux() {
        assertThat(GlibcErgonomics.chooseEnvironment(Map.of(), Settings.EMPTY, false), anEmptyMap());
    }

    public void testArenaMaxFollowsNodeProcessors() {
        Settings settings = Settings.builder().put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), 1.5).build();
        Map<String, String> chosen = GlibcErgonomics.chooseEnvironment(Map.of(), settings, true);
        assertThat(chosen, hasEntry("MALLOC_ARENA_MAX", "2"));
    }

    public void testDefaultLeftOutWhenVariableAlreadySet() {
        String name = randomFrom("MALLOC_ARENA_MAX", "MALLOC_MMAP_THRESHOLD_", "MALLOC_TRIM_THRESHOLD_");
        Map<String, String> chosen = GlibcErgonomics.chooseEnvironment(Map.of(name, "8"), Settings.EMPTY, true);
        assertThat(chosen, not(hasKey(name)));
        // the other two defaults are still chosen
        assertThat(chosen, aMapWithSize(2));
    }

    public void testDefaultLeftOutWhenGlibcTunablesSetsIt() {
        String name = randomFrom("MALLOC_ARENA_MAX", "MALLOC_MMAP_THRESHOLD_", "MALLOC_TRIM_THRESHOLD_");
        String tunable = switch (name) {
            case "MALLOC_ARENA_MAX" -> "glibc.malloc.arena_max";
            case "MALLOC_MMAP_THRESHOLD_" -> "glibc.malloc.mmap_threshold";
            case "MALLOC_TRIM_THRESHOLD_" -> "glibc.malloc.trim_threshold";
            default -> throw new AssertionError(name);
        };
        Map<String, String> chosen = GlibcErgonomics.chooseEnvironment(
            Map.of("GLIBC_TUNABLES", "glibc.malloc.tcache_count=0:" + tunable + "=8"),
            Settings.EMPTY,
            true
        );
        assertThat(chosen, not(hasKey(name)));
        // the other two defaults are still chosen
        assertThat(chosen, aMapWithSize(2));
    }
}
