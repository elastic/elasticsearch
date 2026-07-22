/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.core.Nullable;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

/** Base for golden tests that run with both unmapped_fields=nullify and unmapped_fields=load. */
public abstract class UnmappedGoldenTestCase extends GoldenTestCase {
    protected UnmappedGoldenTestCase() {}

    protected UnmappedGoldenTestCase(String goldenMode) {
        super(goldenMode);
    }

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
            minimumSupportedVersion,
            builder -> {},
            views,
            nestedPaths
        );
        Optional<Throwable> loadException = tryRunTestsLoadOnlyAtVersion(
            query,
            stages,
            minimumSupportedVersion,
            builder -> {},
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
        runTestsNullifyOnlyAtVersion(query, stages, minimumSupportedVersion, builder -> {}, Map.of(), nestedPaths);
    }

    protected void runTestsLoadOnly(String query, EnumSet<Stage> stages, String... nestedPaths) {
        runTestsLoadOnly(query, stages, null, nestedPaths);
    }

    protected void runTestsLoadOnly(String query, EnumSet<Stage> stages, TransportVersion minimumSupportedVersion, String... nestedPaths) {
        runTestsLoadOnlyAtVersion(query, stages, minimumSupportedVersion, builder -> {}, Map.of(), nestedPaths);
    }

    /** Runs NULLIFY mode with version ranges configured on its golden test builder. */
    protected void runTestsNullifyOnlyWithVersionRanges(
        String query,
        EnumSet<Stage> stages,
        Consumer<TestBuilder> configureVersionRanges,
        String... nestedPaths
    ) {
        runTestsNullifyOnlyAtVersion(query, stages, null, configureVersionRanges, Map.of(), nestedPaths);
    }

    /** Runs LOAD mode with version ranges configured on its golden test builder. */
    protected void runTestsLoadOnlyWithVersionRanges(
        String query,
        EnumSet<Stage> stages,
        Consumer<TestBuilder> configureVersionRanges,
        String... nestedPaths
    ) {
        runTestsLoadOnlyAtVersion(query, stages, null, configureVersionRanges, Map.of(), nestedPaths);
    }

    /** Runs NULLIFY mode, optionally lower-bounded by the given transport version. */
    private void runTestsNullifyOnlyAtVersion(
        String query,
        EnumSet<Stage> stages,
        @Nullable TransportVersion minimumSupportedVersion,
        Consumer<TestBuilder> configureVersionRanges,
        Map<String, String> views,
        String... nestedPaths
    ) {
        throwOnFailure(
            tryRunTestsNullifyOnlyAtVersion(query, stages, minimumSupportedVersion, configureVersionRanges, views, nestedPaths),
            "Nullify mode failed"
        );
    }

    /** Runs LOAD mode, optionally lower-bounded by the given transport version. */
    private void runTestsLoadOnlyAtVersion(
        String query,
        EnumSet<Stage> stages,
        @Nullable TransportVersion minimumSupportedVersion,
        Consumer<TestBuilder> configureVersionRanges,
        Map<String, String> views,
        String... nestedPaths
    ) {
        throwOnFailure(
            tryRunTestsLoadOnlyAtVersion(query, stages, minimumSupportedVersion, configureVersionRanges, views, nestedPaths),
            "Load mode failed"
        );
    }

    /** Runs NULLIFY mode with the requested version coverage. */
    private Optional<Throwable> tryRunTestsNullifyOnlyAtVersion(
        String query,
        EnumSet<Stage> stages,
        @Nullable TransportVersion minimumSupportedVersion,
        Consumer<TestBuilder> configureVersionRanges,
        Map<String, String> views,
        String... nestedPaths
    ) {
        var builder = builder(setUnmappedNullify(query)).views(views).nestedPath(ArrayUtils.prepend("nullify", nestedPaths)).stages(stages);
        if (minimumSupportedVersion != null) {
            builder.since(minimumSupportedVersion);
        }
        configureVersionRanges.accept(builder);
        return builder.tryRun();
    }

    /** Runs LOAD mode with the requested version coverage. */
    private Optional<Throwable> tryRunTestsLoadOnlyAtVersion(
        String query,
        EnumSet<Stage> stages,
        @Nullable TransportVersion minimumSupportedVersion,
        Consumer<TestBuilder> configureVersionRanges,
        Map<String, String> views,
        String... nestedPaths
    ) {
        var builder = builder(setUnmappedLoad(query)).views(views).nestedPath(ArrayUtils.prepend("load", nestedPaths)).stages(stages);
        if (minimumSupportedVersion != null) {
            builder.since(minimumSupportedVersion);
        }
        configureVersionRanges.accept(builder);
        return builder.tryRun();
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
