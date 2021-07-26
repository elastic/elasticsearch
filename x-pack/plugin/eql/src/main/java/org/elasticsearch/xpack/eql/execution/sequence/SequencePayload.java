/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.sequence;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse.Event;
import org.elasticsearch.xpack.eql.execution.payload.AbstractPayload;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.eql.util.SearchHitUtils.qualifiedIndex;

class SequencePayload extends AbstractPayload {

    private final List<org.elasticsearch.xpack.eql.action.EqlSearchResponse.Sequence> values;

    SequencePayload(List<Sequence> sequences, List<List<SearchHit>> docs, boolean timedOut, TimeValue timeTook) {
        super(timedOut, timeTook);
        values = new ArrayList<>(sequences.size());

        for (int i = 0; i < sequences.size(); i++) {
            Sequence s = sequences.get(i);
            List<SearchHit> hits = docs.get(i);
            List<Event> events = new ArrayList<>(hits.size());
            for (SearchHit hit : hits) {
                events.add(new Event(qualifiedIndex(hit), hit.getId(), hit.getSourceRef(), hit.getFields()));
            }
            values.add(new org.elasticsearch.xpack.eql.action.EqlSearchResponse.Sequence(s.key().asList(), events));
        }
    }

    @Override
    public Type resultType() {
        return Type.SEQUENCE;
    }

    @Override
    public List<org.elasticsearch.xpack.eql.action.EqlSearchResponse.Sequence> values() {
        return values;
    }
}
