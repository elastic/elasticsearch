/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.payload;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.eql.session.Results.Type;

import java.util.Arrays;
import java.util.List;

public class SearchResponsePayload extends AbstractPayload {

    private final List<SearchHit> hits;

    public SearchResponsePayload(SearchResponse response) {
        super(response.isTimedOut(), response.getTook(), null);
        hits = Arrays.asList(response.getHits().getHits());
    }

    @Override
    public Type resultType() {
        return Type.SEARCH_HIT;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V> List<V> values() {
        return (List<V>) hits;
    }
}
