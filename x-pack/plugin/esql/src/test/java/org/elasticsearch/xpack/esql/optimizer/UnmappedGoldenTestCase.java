/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.TransportVersionUtils;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Base for golden tests that run with both unmapped_fields=nullify and unmapped_fields=load. */
public abstract class UnmappedGoldenTestCase extends GoldenTestCase {
    @Override
    protected List<String> filteredWarnings() {
        var filtered = new ArrayList<>(super.filteredWarnings());
        filtered.add(
            "has no implicit conversion from KEYWORD, so it will not be loaded from _source; values will be null in those indices"
        );
        return filtered;
    }

    /** Runs the query with both {@code NULLIFY} and {@code LOAD}; throws if either fails. */
    protected void runTestsNullifyAndLoad(
        String query,
        EnumSet<Stage> stages,
        TransportVersion minimumSupportedVersion,
        String... nestedPaths
    ) {
        runTestsNullifyAndLoad(query, stages, minimumSupportedVersion, Map.of(), nestedPaths);
    }

    /** Runs the query (referencing the given views) with both {@code NULLIFY} and {@code LOAD}; throws if either fails. */
    protected void runTestsNullifyAndLoad(
        String query,
        EnumSet<Stage> stages,
        TransportVersion minimumSupportedVersion,
        Map<String, String> views,
        String... nestedPaths
    ) {
        Optional<Throwable> nullifyException = tryRunTestsNullifyOnlyAtVersion(
            query,
            stages,
            randomVersionSupportingOrNull(minimumSupportedVersion),
            views,
            nestedPaths
        );
        Optional<Throwable> loadException = tryRunTestsLoadOnlyAtVersion(
            query,
            stages,
            randomVersionSupportingOrNull(minimumSupportedVersion),
            views,
            nestedPaths
        );
        nullifyException.ifPresent(e -> {
            throw new RuntimeException(
                loadException.isPresent() ? "Both nullify and load modes failed" : "Nullify mode failed (but load succeeded)",
                e
            );
        });
        throwOnFailure(loadException, "Load mode failed (but nullify succeeded)");
    }

    protected void runTestsNullifyOnly(String query, EnumSet<Stage> stages, String... nestedPaths) {
        runTestsNullifyOnly(query, stages, null, nestedPaths);
    }

    protected void runTestsNullifyOnly(
        String query,
        EnumSet<Stage> stages,
        TransportVersion minimumSupportedVersion,
        String... nestedPaths
    ) {
        runTestsNullifyOnlyAtVersion(query, stages, randomVersionSupportingOrNull(minimumSupportedVersion), Map.of(), nestedPaths);
    }

    protected void runTestsLoadOnly(String query, EnumSet<Stage> stages, String... nestedPaths) {
        runTestsLoadOnly(query, stages, null, nestedPaths);
    }

    protected void runTestsLoadOnly(String query, EnumSet<Stage> stages, TransportVersion minimumSupportedVersion, String... nestedPaths) {
        runTestsLoadOnlyAtVersion(query, stages, randomVersionSupportingOrNull(minimumSupportedVersion), Map.of(), nestedPaths);
    }

    /**
     * Runs NULLIFY mode at a random version that does <em>not</em> support {@code maxVersionExclusive}. Most queries
     * don't need this, since NULLIFY nulls out unmapped fields outright rather than resolving a union type; use it
     * for queries that also have a genuine mapped-vs-mapped type conflict, where NULLIFY still builds a
     * {@code MultiTypeEsField}/{@code CompactMultiTypeEsField} for the conflicting legs.
     */
    protected void runTestsNullifyOnlyBelow(
        String query,
        EnumSet<Stage> stages,
        TransportVersion maxVersionExclusive,
        String... nestedPaths
    ) {
        runTestsNullifyOnlyAtVersion(
            query,
            stages,
            TransportVersionUtils.randomVersionNotSupporting(maxVersionExclusive),
            Map.of(),
            nestedPaths
        );
    }

    /**
     * Runs LOAD mode at a random version that does <em>not</em> support {@code maxVersionExclusive}, unlike the
     * {@code minimumSupportedVersion} overloads which randomize a version that does. Use this to deterministically
     * exercise a version that predates a given wire-format change.
     */
    protected void runTestsLoadOnlyBelow(String query, EnumSet<Stage> stages, TransportVersion maxVersionExclusive, String... nestedPaths) {
        runTestsLoadOnlyAtVersion(
            query,
            stages,
            TransportVersionUtils.randomVersionNotSupporting(maxVersionExclusive),
            Map.of(),
            nestedPaths
        );
    }

    /**
     * Runs LOAD mode at a random version that supports {@code minSupportedVersion} but does not support
     * {@code maxVersionExclusive} -- i.e. a version between the two. Use this when the query has some other minimum
     * version requirement (e.g. a TS-mode query needing dimension-column support) that a version predating
     * {@code maxVersionExclusive} might otherwise not meet.
     */
    protected void runTestsLoadOnlyBetween(
        String query,
        EnumSet<Stage> stages,
        TransportVersion minSupportedVersion,
        TransportVersion maxVersionExclusive,
        String... nestedPaths
    ) {
        runTestsLoadOnlyAtVersion(
            query,
            stages,
            randomVersionSupportingButNot(minSupportedVersion, maxVersionExclusive),
            Map.of(),
            nestedPaths
        );
    }

    /** Runs NULLIFY mode at the given exact transport version (or a random one if {@code null}) */
    private void runTestsNullifyOnlyAtVersion(
        String query,
        EnumSet<Stage> stages,
        @Nullable TransportVersion transportVersion,
        Map<String, String> views,
        String... nestedPaths
    ) {
        throwOnFailure(tryRunTestsNullifyOnlyAtVersion(query, stages, transportVersion, views, nestedPaths), "Nullify mode failed");
    }

    /** Runs LOAD mode at the given exact transport version (or a random one if {@code null}) */
    private void runTestsLoadOnlyAtVersion(
        String query,
        EnumSet<Stage> stages,
        @Nullable TransportVersion transportVersion,
        Map<String, String> views,
        String... nestedPaths
    ) {
        throwOnFailure(tryRunTestsLoadOnlyAtVersion(query, stages, transportVersion, views, nestedPaths), "Load mode failed");
    }

    /** Runs NULLIFY mode at the given exact transport version, or a random version if {@code null}. */
    private Optional<Throwable> tryRunTestsNullifyOnlyAtVersion(
        String query,
        EnumSet<Stage> stages,
        @Nullable TransportVersion transportVersion,
        Map<String, String> views,
        String... nestedPaths
    ) {
        var builder = builder(setUnmappedNullify(query)).views(views).nestedPath(ArrayUtils.prepend("nullify", nestedPaths)).stages(stages);
        if (transportVersion != null) {
            builder.transportVersion(transportVersion);
        }
        return builder.tryRun();
    }

    /** Runs LOAD mode at the given exact transport version, or a random version if {@code null}. */
    private Optional<Throwable> tryRunTestsLoadOnlyAtVersion(
        String query,
        EnumSet<Stage> stages,
        @Nullable TransportVersion transportVersion,
        Map<String, String> views,
        String... nestedPaths
    ) {
        var builder = builder(setUnmappedLoad(query)).views(views).nestedPath(ArrayUtils.prepend("load", nestedPaths)).stages(stages);
        if (transportVersion != null) {
            builder.transportVersion(transportVersion);
        }
        return builder.tryRun();
    }

    private static TransportVersion randomVersionSupportingOrNull(TransportVersion minimumSupportedVersion) {
        return minimumSupportedVersion == null ? null : TransportVersionUtils.randomVersionSupporting(minimumSupportedVersion);
    }

    private static TransportVersion randomVersionSupportingButNot(TransportVersion mustSupport, TransportVersion mustNotSupport) {
        return RandomPicks.randomFrom(
            random(),
            TransportVersionUtils.allReleasedVersions()
                .stream()
                .filter(v -> v.supports(mustSupport) && v.supports(mustNotSupport) == false)
                .toList()
        );
    }

    private static void throwOnFailure(Optional<Throwable> exception, String message) {
        exception.ifPresent(e -> { throw new RuntimeException(message, e); });
    }

    private static String setUnmappedNullify(String query) {
        return "SET unmapped_fields=\"nullify\"; " + query;
    }

    private static String setUnmappedLoad(String query) {
        return "SET unmapped_fields=\"load\"; " + query;
    }
}
