/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.profile.coordinator;

import org.elasticsearch.search.profile.AbstractProfileBreakdown;
import org.elasticsearch.search.profile.SearchProfileCoordinatorResult;

public class SearchCoordinatorProfiler extends AbstractProfileBreakdown<SearchCoordinatorTimingType> {

    private final String nodeId;
    private RetrieverProfileResult retriever;

    public SearchCoordinatorProfiler(final String nodeId) {
        super(SearchCoordinatorTimingType.class);
        this.nodeId = nodeId;
    }

    public void captureInnerRetrieverResult(SearchProfileCoordinatorResult profileResult) {
        if (retriever == null) {
            throw new IllegalArgumentException("parent [retriever] results have not been initialized");
        }
        retriever.addChild(profileResult);
    }

    public void captureRetrieverTookInMillis(long tookInMillis) {
        if (retriever == null) {
            throw new IllegalArgumentException("parent [retriever] results have not been initialized");
        }
        retriever.setTookInMillis(tookInMillis);
    }

    public void retriever(RetrieverProfileResult retriever) {
        if (this.retriever != null) {
            throw new IllegalArgumentException("[retriever] results have already been set");
        }
        this.retriever = retriever;
    }

    public String getNodeId() {
        return nodeId;
    }

    public RetrieverProfileResult getRetriever() {
        return this.retriever;
    }

    public SearchProfileCoordinatorResult build() {
        return new SearchProfileCoordinatorResult(nodeId, retriever, toBreakdownMap());
    }
}
