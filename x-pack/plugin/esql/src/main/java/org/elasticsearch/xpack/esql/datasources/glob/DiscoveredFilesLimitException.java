/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.core.Nullable;

/**
 * Thrown when a listing kept more files than {@code esql.external.max_discovered_files}.
 * Distinct from a bare {@link IllegalArgumentException} so callers catch the cap by type,
 * never on message text. Same REST status as {@code Check#clientError} (400).
 */
public final class DiscoveredFilesLimitException extends IllegalArgumentException {
    public DiscoveredFilesLimitException(int discoveredCount, int maxDiscoveredFiles) {
        this(discoveredCount, maxDiscoveredFiles, null);
    }

    DiscoveredFilesLimitException(int discoveredCount, int maxDiscoveredFiles, @Nullable WalkDiagnostics walk) {
        super(limitMessage(discoveredCount, maxDiscoveredFiles, walk));
    }

    private static String limitMessage(int discoveredCount, int maxDiscoveredFiles, @Nullable WalkDiagnostics walk) {
        String message = LoggerMessageFormat.format(
            "Glob pattern discovered too many files ({}, limit {}). Narrow your glob pattern, add partition "
                + "filters, or increase the [esql.external.max_discovered_files] cluster setting.",
            discoveredCount,
            maxDiscoveredFiles
        );
        return walk == null ? message : walk.appendTo(message);
    }
}
