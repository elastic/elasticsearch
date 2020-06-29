/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.assembler;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.eql.execution.payload.AbstractPayload;
import org.elasticsearch.xpack.eql.execution.sequence.Sequence;
import org.elasticsearch.xpack.eql.session.Results.Type;

import java.util.ArrayList;
import java.util.List;

class SequencePayload extends AbstractPayload {

    private final List<org.elasticsearch.xpack.eql.action.EqlSearchResponse.Sequence> sequences;

    SequencePayload(List<Sequence> seq, boolean timedOut, TimeValue timeTook, Object[] nextKeys) {
        super(timedOut, timeTook, nextKeys);
        sequences = new ArrayList<>(seq.size());
        for (Sequence s : seq) {
            sequences.add(new org.elasticsearch.xpack.eql.action.EqlSearchResponse.Sequence(s.key().asStringList(), s.hits()));
        }
    }

    @Override
    public Type resultType() {
        return Type.SEQUENCE;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V> List<V> values() {
        return (List<V>) sequences;
    }
}
