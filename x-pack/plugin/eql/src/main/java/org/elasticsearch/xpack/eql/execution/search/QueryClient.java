/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.eql.session.Payload;

import java.util.List;

/**
 * Infrastructure interface used to decouple listener consumers from the stateful classes holding client-references and co.
 */
public interface QueryClient {

    void query(QueryRequest request, ActionListener<Payload> listener);

    void get(Iterable<List<HitReference>> refs, ActionListener<List<List<SearchHit>>> listener);
}
