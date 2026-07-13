/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;

import java.util.List;

/**
 * Infrastructure interface used to decouple listener consumers from the stateful classes holding client-references and co.
 */
public interface QueryClient {

    void query(QueryRequest request, ActionListener<SearchResponse> listener);

    default void close(ActionListener<Boolean> closed) {}

    void fetchHits(Iterable<List<HitReference>> refs, ActionListener<List<List<SearchHit>>> listener);

    default void multiQuery(List<SearchRequest> searches, ActionListener<MultiSearchResponse> listener) {}
}
