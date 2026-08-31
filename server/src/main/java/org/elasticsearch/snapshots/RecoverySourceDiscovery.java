/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.snapshots;

import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.regex.Regex;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Predicate;

/**
 * Discovers eligible recovery sources from a snapshot for the data recovery API.
 * <p>
 * It filters candidates (excluding system, backing, and incomplete indices/streams),
 * applies a multi-target expression list, and returns a bounded result.
 */
public final class RecoverySourceDiscovery {

    private RecoverySourceDiscovery() {}

    /**
     * An eligible logical recovery source returned by the source-discovery API. Sources are ordered
     * deterministically by {@link #name()} and each name appears at most once in a result set.
     */
    public record RecoverySource(String name, Type type) implements Comparable<RecoverySource> {

        /** Whether the source is a standalone index or a data stream. */
        public enum Type {
            INDEX,
            DATA_STREAM;
        }

        @Override
        public int compareTo(RecoverySource other) {
            return this.name.compareTo(other.name);
        }
    }

    /** Paged result from {@link #discover}: up to {@code size} sources ordered by name, plus a {@code hasMore} flag. */
    public record Result(List<RecoverySource> sources, boolean hasMore) {}

    /**
     * Returns up to {@code size} eligible recovery sources from a snapshot, ordered by name.
     * Excludes system resources, backing/failure-store indices, and incomplete indices or streams.
     * Expressions support wildcards and {@code -} negation; empty returns all eligible sources.
     *
     * @param snapshotInfo    the snapshot to evaluate
     * @param projectMetadata project metadata captured in the snapshot
     * @param expressions     multi-target expression list; empty means return all eligible sources
     * @param size            maximum number of sources to return; must be positive
     * @throws IllegalArgumentException if {@code size} is not positive
     */
    public static Result discover(SnapshotInfo snapshotInfo, ProjectMetadata projectMetadata, List<String> expressions, int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("size must be positive, got [" + size + "]");
        }

        SortedSet<RecoverySource> candidates = buildCandidates(snapshotInfo, projectMetadata);

        return filterAndCollect(size, candidates, buildMatcher(expressions));
    }

    /**
     * Builds the full set of eligible candidates from the snapshot, sorted by name.
     * Excludes system indices/streams, backing and failure-store indices, and incomplete indices/streams.
     */
    static SortedSet<RecoverySource> buildCandidates(SnapshotInfo snapshotInfo, ProjectMetadata projectMetadata) {
        Map<String, DataStream> dataStreams = projectMetadata.dataStreams();

        Set<String> backingAndFailureIndexNames = new java.util.HashSet<>();
        for (DataStream ds : dataStreams.values()) {
            ds.getIndices().forEach(idx -> backingAndFailureIndexNames.add(idx.getName()));
            ds.getFailureIndices().forEach(idx -> backingAndFailureIndexNames.add(idx.getName()));
        }

        SortedSet<RecoverySource> candidates = new TreeSet<>();

        for (String indexName : snapshotInfo.indices()) {
            if (backingAndFailureIndexNames.contains(indexName)) {
                continue;
            }
            IndexMetadata meta = projectMetadata.index(indexName);
            if (meta != null && meta.isSystem()) {
                continue;
            }
            if (snapshotInfo.isIndexComplete(indexName) == false) {
                continue;
            }
            candidates.add(new RecoverySource(indexName, RecoverySource.Type.INDEX));
        }

        for (String dsName : snapshotInfo.dataStreams()) {
            DataStream ds = dataStreams.get(dsName);
            if (ds == null || ds.isSystem() || snapshotInfo.isDataStreamComplete(ds) == false) {
                continue;
            }
            candidates.add(new RecoverySource(dsName, RecoverySource.Type.DATA_STREAM));
        }

        return candidates;
    }

    private static Predicate<String> buildMatcher(List<String> expressions) {
        if (expressions.isEmpty()) {
            return name -> true;
        }
        record Term(boolean positive, Predicate<String> matcher) {}
        List<Term> terms = new ArrayList<>(expressions.size());
        for (String expr : expressions) {
            if (expr.startsWith("-")) {
                terms.add(new Term(false, Regex.simpleMatcher(expr.substring(1))));
            } else {
                terms.add(new Term(true, Regex.simpleMatcher(expr)));
            }
        }
        return name -> {
            boolean included = false;
            for (Term term : terms) {
                if (term.matcher().test(name)) {
                    included = term.positive();
                }
            }
            return included;
        };
    }

    private static Result filterAndCollect(int size, SortedSet<RecoverySource> candidates, Predicate<String> matcher) {
        boolean hasMore = false;
        List<RecoverySource> filteredSources = new ArrayList<>(Math.min(size, candidates.size()));
        for (RecoverySource source : candidates) {
            if (matcher.test(source.name()) == false) {
                continue;
            }
            if (filteredSources.size() == size) {
                hasMore = true;
                break;
            } else {
                filteredSources.add(source);
            }
        }

        return new Result(filteredSources, hasMore);
    }
}
