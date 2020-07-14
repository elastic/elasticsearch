/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.sequence;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.eql.execution.payload.AbstractPayload;
import org.elasticsearch.xpack.eql.session.Results.Type;
import org.elasticsearch.xpack.eql.util.ReversedIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

class SequencePayload extends AbstractPayload {

    private final List<org.elasticsearch.xpack.eql.action.EqlSearchResponse.Sequence> sequences;

    SequencePayload(List<Sequence> seq, boolean timedOut, TimeValue timeTook) {
        super(timedOut, timeTook);
        sequences = new ArrayList<>(seq.size());
        boolean needsReversal = seq.size() > 1 && (seq.get(0).ordinal().compareTo(seq.get(1).ordinal()) > 0);
        
        for (Iterator<Sequence> it = needsReversal ? new ReversedIterator<>(seq) : seq.iterator(); it.hasNext();) {
            Sequence s = it.next();
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
