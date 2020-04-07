/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.payload;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;

import java.util.Arrays;
import java.util.List;

public class SearchResponsePayload implements Payload<SearchHit> {

    private final SearchResponse response;

    public SearchResponsePayload(SearchResponse response) {
        this.response = response;
    }

    @Override
    public boolean timedOut() {
        return response.isTimedOut();
    }

    @Override
    public TimeValue timeTook() {
        return response.getTook();
    }

    @Override
    public Object[] nextKeys() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<SearchHit> values() {
        return Arrays.asList(response.getHits().getHits());
    }
}
