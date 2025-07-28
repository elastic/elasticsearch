/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.retriever;

import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.usage.SearchUsage;

import java.util.Objects;
import java.util.function.Predicate;

public class RetrieverParserContext {

    protected final SearchUsage searchUsage;
    protected final Predicate<NodeFeature> clusterSupportsFeature;

    public RetrieverParserContext(SearchUsage searchUsage, Predicate<NodeFeature> clusterSupportsFeature) {
        this.searchUsage = Objects.requireNonNull(searchUsage);
        this.clusterSupportsFeature = clusterSupportsFeature;
    }

    public void trackSectionUsage(String section) {
        searchUsage.trackSectionUsage(section);
    }

    public void trackQueryUsage(String query) {
        searchUsage.trackQueryUsage(query);
    }

    public void trackRescorerUsage(String name) {
        searchUsage.trackRescorerUsage(name);
    }

    public void trackRetrieverUsage(String name) {
        searchUsage.trackRetrieverUsage(name);
    }

    public boolean clusterSupportsFeature(NodeFeature nodeFeature) {
        return clusterSupportsFeature != null && clusterSupportsFeature.test(nodeFeature);
    }
}
