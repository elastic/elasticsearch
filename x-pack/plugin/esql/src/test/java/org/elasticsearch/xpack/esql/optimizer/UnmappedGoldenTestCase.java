/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.util.ArrayUtils;
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
        Optional<Throwable> nullifyException = tryRunTestsNullifyOnly(query, stages, minimumSupportedVersion, views, nestedPaths);
        Optional<Throwable> loadException = tryRunTestsLoadOnly(query, stages, minimumSupportedVersion, views, nestedPaths);
        nullifyException.ifPresent(e -> {
            throw new RuntimeException(
                loadException.isPresent() ? "Both nullify and load modes failed" : "Nullify mode failed (but load succeeded)",
                e
            );
        });
        loadException.ifPresent(e -> { throw new RuntimeException("Load mode failed (but nullify succeeded)", e); });
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
        tryRunTestsNullifyOnly(query, stages, minimumSupportedVersion, Map.of(), nestedPaths).ifPresent(e -> {
            throw new RuntimeException("Nullify mode failed", e);
        });
    }

    protected void runTestsLoadOnly(String query, EnumSet<Stage> stages, String... nestedPaths) {
        tryRunTestsLoadOnly(query, stages, null, Map.of(), nestedPaths).ifPresent(
            e -> { throw new RuntimeException("Load mode failed", e); }
        );
    }

    /**
     * Runs LOAD mode pinned to the exact given transport version, unlike the {@code minimumSupportedVersion} overloads
     * which randomize a version that <em>supports</em> (i.e. is at or after) the given one. Use this to deterministically
     * exercise a version that predates a given wire-format change, e.g. via {@link TransportVersionUtils#randomVersionNotSupporting}.
     */
    protected void runTestsLoadOnlyAtVersion(
        String query,
        EnumSet<Stage> stages,
        TransportVersion transportVersion,
        String... nestedPaths
    ) {
        loadOnlyBuilder(query, stages, Map.of(), nestedPaths).transportVersion(transportVersion).run();
    }

    private Optional<Throwable> tryRunTestsNullifyOnly(
        String query,
        EnumSet<Stage> stages,
        TransportVersion minimumSupportedVersion,
        Map<String, String> views,
        String... nestedPaths
    ) {
        var builder = builder(setUnmappedNullify(query)).views(views).nestedPath(ArrayUtils.prepend("nullify", nestedPaths)).stages(stages);
        if (minimumSupportedVersion != null) {
            builder.transportVersion(TransportVersionUtils.randomVersionSupporting(minimumSupportedVersion));
        }
        return builder.tryRun();
    }

    private Optional<Throwable> tryRunTestsLoadOnly(
        String query,
        EnumSet<Stage> stages,
        TransportVersion minimumSupportedVersion,
        Map<String, String> views,
        String... nestedPaths
    ) {
        var builder = loadOnlyBuilder(query, stages, views, nestedPaths);
        if (minimumSupportedVersion != null) {
            builder.transportVersion(TransportVersionUtils.randomVersionSupporting(minimumSupportedVersion));
        }
        return builder.tryRun();
    }

    /** Shared builder setup for LOAD mode, used by both {@link #tryRunTestsLoadOnly} and {@link #runTestsLoadOnlyAtVersion}. */
    private TestBuilder loadOnlyBuilder(String query, EnumSet<Stage> stages, Map<String, String> views, String... nestedPaths) {
        return builder(setUnmappedLoad(query)).views(views).nestedPath(ArrayUtils.prepend("load", nestedPaths)).stages(stages);
    }

    private static String setUnmappedNullify(String query) {
        return "SET unmapped_fields=\"nullify\"; " + query;
    }

    private static String setUnmappedLoad(String query) {
        return "SET unmapped_fields=\"load\"; " + query;
    }
}
