/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.payload;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse.Event;

import java.util.ArrayList;
import java.util.List;

public class EventPayload extends AbstractPayload {

    private final List<Event> values;

    public EventPayload(SearchResponse response) {
        super(response.isTimedOut(), response.getTook(), response.getShardFailures());

        SearchHits hits = response.getHits();
        values = new ArrayList<>(hits.getHits().length);
        for (SearchHit hit : hits) {
            // TODO: remove unpooled usage
            values.add(new Event(hit.asUnpooled()));
        }
    }

    @Override
    public Type resultType() {
        return Type.EVENT;
    }

    @Override
    public List<Event> values() {
        return values;
    }
}
