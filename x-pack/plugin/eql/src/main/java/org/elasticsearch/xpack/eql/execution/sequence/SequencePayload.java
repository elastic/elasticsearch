/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.sequence;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.eql.execution.payload.AbstractPayload;
import org.elasticsearch.xpack.eql.session.Results.Type;

import java.util.ArrayList;
import java.util.List;

class SequencePayload extends AbstractPayload {

    private final List<org.elasticsearch.xpack.eql.action.EqlSearchResponse.Sequence> values;

    SequencePayload(List<Sequence> sequences, List<List<SearchHit>> searchHits, boolean timedOut, TimeValue timeTook) {
        super(timedOut, timeTook);
        values = new ArrayList<>(sequences.size());
        
        for (int i = 0; i < sequences.size(); i++) {
            Sequence s = sequences.get(i);
            List<SearchHit> hits = searchHits.get(i);
            values.add(new org.elasticsearch.xpack.eql.action.EqlSearchResponse.Sequence(s.key().asStringList(), hits));
        }
    }

    @Override
    public Type resultType() {
        return Type.SEQUENCE;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V> List<V> values() {
        return (List<V>) values;
    }
}
