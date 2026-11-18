/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test;

import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.testing.Test;
import org.gradle.process.CommandLineArgumentProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Passes the DRA builds that a bwc test task's versions resolved to on to its test JVM, as the arguments that select
 * exactly those builds: {@code -Dtests.bwc.mode=dra} and {@code -Dtests.bwc.dra.hash.<branch>=<hash>}.
 * {@code ReproduceInfoPrinter} puts them in the reproduce line, so a repro runs against the snapshots that failed,
 * however this build chose them (known-good, latest, auto), without looking them up again.
 *
 * <p>A version built from source gets no pin, and a task without any pinned version gets no arguments.
 */
public final class BwcSnapshotPins {

    static final String MODE = "tests.bwc.mode";
    static final String DRA_HASH_PREFIX = "tests.bwc.dra.hash.";

    private BwcSnapshotPins() {}

    /**
     * Pins {@code version} (on {@code branch}) for {@code task} to the DRA build the bwc project of that version resolved.
     */
    public static void pin(Test task, String version, String branch) {
        Provider<BwcSnapshotBuildsService> builds = BwcSnapshotBuildsService.register(task.getProject().getGradle());
        Arguments arguments = null;
        for (CommandLineArgumentProvider provider : task.getJvmArgumentProviders()) {
            if (provider instanceof Arguments existing) {
                arguments = existing;
            }
        }
        if (arguments == null) {
            arguments = new Arguments();
            task.getJvmArgumentProviders().add(arguments);
        }
        arguments.buildIdsByBranch.put(branch, builds.flatMap(service -> service.buildId(version)));
    }

    /**
     * Not inputs of the task: the distributions they select already are.
     */
    static class Arguments implements CommandLineArgumentProvider {

        private final Map<String, Provider<String>> buildIdsByBranch = new TreeMap<>();

        @Internal
        public Map<String, Provider<String>> getBuildIdsByBranch() {
            return buildIdsByBranch;
        }

        @Override
        public Iterable<String> asArguments() {
            List<String> pins = new ArrayList<>();
            buildIdsByBranch.forEach((branch, buildId) -> {
                String id = buildId.getOrElse("");
                int dash = id.lastIndexOf('-');
                if (dash > 0) {
                    pins.add("-D" + DRA_HASH_PREFIX + branch + "=" + id.substring(dash + 1));
                }
            });
            if (pins.isEmpty()) {
                return pins;
            }
            pins.add(0, "-D" + MODE + "=dra");
            return pins;
        }
    }
}
