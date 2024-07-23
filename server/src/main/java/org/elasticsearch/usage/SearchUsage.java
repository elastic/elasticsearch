/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.usage;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Holds usage statistics for an incoming search request
 */
public final class SearchUsage implements Writeable {
    private final Set<String> queries;
    private final Set<String> rescorers;
    private final Set<String> sections;

    public SearchUsage() {
        queries = new HashSet<>();
        rescorers = new HashSet<>();
        sections = new HashSet<>();

    }

    public SearchUsage(StreamInput in) throws IOException {
        queries = in.readCollectionAsSet(StreamInput::readString);
        rescorers = in.readCollectionAsSet(StreamInput::readString);
        sections = in.readCollectionAsSet(StreamInput::readString);
    }

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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(queries);
        out.writeStringCollection(rescorers);
        out.writeStringCollection(sections);
    }
}
