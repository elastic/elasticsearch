/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test;

import org.elasticsearch.gradle.test.SystemPropertyCommandLineArgumentProvider;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.api.tasks.testing.Test;

import java.util.Map;
import java.util.TreeMap;

/**
 * Passes the DRA snapshot pins of a bwc build ({@code -Dtests.bwc.mode} and {@code -Dtests.bwc.dra.hash.<branch>}) on
 * to the JVM of a test task that uses bwc distributions. Only the build reads them to pick the snapshots; the test JVM
 * needs them so that {@code ReproduceInfoPrinter} puts them in the reproduce line, which otherwise would build each
 * branch's current tip from source instead of the snapshot that failed.
 *
 * <p>They are non-input properties: the distributions they select are already inputs of the task.
 */
public final class BwcSnapshotPins {

    static final String MODE = "tests.bwc.mode";
    static final String DRA_HASH_PREFIX = "tests.bwc.dra.hash.";

    private BwcSnapshotPins() {}

    public static void forward(Test task, ProviderFactory providers) {
        SystemPropertyCommandLineArgumentProvider nonInputProperties = task.getExtensions()
            .getByType(SystemPropertyCommandLineArgumentProvider.class);
        String mode = providers.systemProperty(MODE).getOrNull();
        if (mode != null) {
            nonInputProperties.systemProperty(MODE, mode);
        }
        Map<String, String> hashes = new TreeMap<>(providers.systemPropertiesPrefixedBy(DRA_HASH_PREFIX).get());
        hashes.forEach(nonInputProperties::systemProperty);
    }
}
