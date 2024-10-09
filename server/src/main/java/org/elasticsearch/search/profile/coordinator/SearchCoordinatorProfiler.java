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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SearchCoordinatorProfiler extends AbstractProfileBreakdown<SearchCoordinatorTimingType> {

    private final String nodeId;
    private String retriever;
    private long tookInMillis = -1;
    List<RetrieverProfileResult> children = new ArrayList<>();

    public SearchCoordinatorProfiler(final String nodeId) {
        super(SearchCoordinatorTimingType.class);
        this.nodeId = nodeId;
    }

    public void captureRetrieverResult(RetrieverProfileResult profileResult) {
        children.add(profileResult);
    }

    public void retriever(String retriever) {
        this.retriever = retriever;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getRetriever() {
        return this.retriever;
    }

    public List<RetrieverProfileResult> getRetrieverChildren() {
        return children;
    }

    public SearchProfileCoordinatorResult build() {
        Map<String, Long> breakdownMap = toBreakdownMap();
        return new SearchProfileCoordinatorResult(
            nodeId,
            new RetrieverProfileResult(
                retriever,
                nodeId,
                tookInMillis,
                breakdownMap.getOrDefault(SearchCoordinatorTimingType.RETRIEVER_REWRITE.toString(), -1L),
                children.toArray(new RetrieverProfileResult[0])
            )
        );
    }

    public void captureTook(long millis) {
        tookInMillis = millis;
    }
}
