/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.usage;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Holds usage statistics for an incoming search request
 */
public final class SearchUsage {

    public final String RETRIEVERS_NAME = "retrievers";

    private final Set<String> queries = new HashSet<>();
    private final Set<String> rescorers = new HashSet<>();
    private final Set<String> sections = new HashSet<>();
    private final Set<String> retrievers = new HashSet<>();
    private final ExtendedUsageTracker extendedUsage = new ExtendedUsageTracker();

    /**
     * Track the usage of the provided query
     */
    public void trackQueryUsage(String query) {
        queries.add(query);
    }

    /**
     * Track the usage of the provided search section
     */
    public void trackSectionUsage(String section) {
        sections.add(section);
    }

    /**
     * Track the usage of the provided rescorer
     */
    public void trackRescorerUsage(String name) {
        rescorers.add(name);
    }

    public void trackRetrieverUsage(String retriever) {
        retrievers.add(retriever);
        extendedUsage.initialize(RETRIEVERS_NAME, retriever);
    }

    /**
     * Track the usage of extended data for a specific category
     */
    private void trackExtendedDataUsage(String category, String name, Set<String> values) {
        extendedUsage.track(category, name, values);
    }

    public void trackRetrieverExtendedDataUsage(String name, Set<String> values) {
        trackExtendedDataUsage(RETRIEVERS_NAME, name, values);
    }

    /**
     * Returns the query types that have been used at least once in the tracked search request
     */
    public Set<String> getQueryUsage() {
        return Collections.unmodifiableSet(queries);
    }

    /**
     * Returns the rescorer types that have been used at least once in the tracked search request
     */
    public Set<String> getRescorerUsage() {
        return Collections.unmodifiableSet(rescorers);
    }

    /**
     * Returns the search section names that have been used at least once in the tracked search request
     */
    public Set<String> getSectionsUsage() {
        return Collections.unmodifiableSet(sections);
    }

    /**
     * Returns the retriever names that have been used at least once in the tracked search request
     */
    public Set<String> getRetrieverUsage() {
        return Collections.unmodifiableSet(retrievers);
    }

    /**
     * Returns the extended data that has been tracked for the search request
     */
    public Map<String, Map<String, Set<String>>> getExtendedDataUsage() {
        return extendedUsage.getUsage();
    }

    private static final class ExtendedUsageTracker {

        /**
         * A map of categories to extended data. Categories correspond to a high-level search usage statistic,
         * e.g. `queries`, `rescorers`, `sections`, `retrievers`.
         *
         * Extended data is further segmented by name, for example collecting specific statistics for certain retrievers only.
         * Finally we keep track of the set of values we are tracking for each category and name.
         */
        private final Map<String, Map<String, Set<String>>> categoriesToExtendedUsage = new HashMap<>();

        public void initialize(String category, String name) {
            categoriesToExtendedUsage.computeIfAbsent(category, k -> new HashMap<>()).computeIfAbsent(name, k -> new HashSet<>());
        }

        public void track(String category, String name, String value) {
            categoriesToExtendedUsage.computeIfAbsent(category, k -> new HashMap<>())
                .computeIfAbsent(name, k -> new HashSet<>())
                .add(value);
        }

        public void track(String category, String name, Set<String> values) {
            categoriesToExtendedUsage.computeIfAbsent(category, k -> new HashMap<>())
                .computeIfAbsent(name, k -> new HashSet<>())
                .addAll(values);
        }

        public Map<String, Map<String, Set<String>>> getUsage() {
            return Collections.unmodifiableMap(categoriesToExtendedUsage);
        }
    }
}
