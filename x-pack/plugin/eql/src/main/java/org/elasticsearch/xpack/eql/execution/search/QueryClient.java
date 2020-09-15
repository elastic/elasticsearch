/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;

import java.util.List;

/**
 * Infrastructure interface used to decouple listener consumers from the stateful classes holding client-references and co.
 */
public interface QueryClient {

    void query(QueryRequest request, ActionListener<SearchResponse> listener);

    void get(Iterable<List<HitReference>> refs, ActionListener<List<List<GetResponse>>> listener);

    default void close(ActionListener<Boolean> closed) {}
}
