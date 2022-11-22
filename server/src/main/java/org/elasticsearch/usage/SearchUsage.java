/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.usage;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Holds usage statistics for an incoming search request
 */
public final class SearchUsage {
    private final Set<String> queries = new HashSet<>();
    private final Set<String> sections = new HashSet<>();

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
     * Returns the query types that have been used at least once in the tracked search request
     */
    public Set<String> getQueryUsage() {
        return Collections.unmodifiableSet(queries);
    }

    /**
     * Returns the search section names that have been used at least once in the tracked search request
     */
    public Set<String> getSectionsUsage() {
        return Collections.unmodifiableSet(sections);
    }
}
