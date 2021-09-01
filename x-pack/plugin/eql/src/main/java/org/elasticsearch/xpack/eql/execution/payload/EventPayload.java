/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.payload;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse.Event;
import org.elasticsearch.xpack.eql.execution.search.RuntimeUtils;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.eql.util.SearchHitUtils.qualifiedIndex;

public class EventPayload extends AbstractPayload {

    private final List<Event> values;

    public EventPayload(SearchResponse response) {
        super(response.isTimedOut(), response.getTook());

        List<SearchHit> hits = RuntimeUtils.searchHits(response);
        values = new ArrayList<>(hits.size());
        for (SearchHit hit : hits) {
            values.add(new Event(qualifiedIndex(hit), hit.getId(), hit.getSourceRef(), hit.getFields()));
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
