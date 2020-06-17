/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.payload;

import org.elasticsearch.common.unit.TimeValue;

import java.util.List;

public class SequencePayload implements Payload<Sequence> {

    private final List<Sequence> seq;
    private boolean timedOut;
    private TimeValue timeTook;
    private Object[] nextKeys;

    public SequencePayload(List<Sequence> seq, boolean timedOut, TimeValue timeTook, Object[] nextKeys) {
        this.seq = seq;
        this.timedOut = timedOut;
        this.timeTook = timeTook;
        this.nextKeys = nextKeys;
    }

    @Override
    public boolean timedOut() {
        return false;
    }

    @Override
    public TimeValue timeTook() {
        return timeTook;
    }

    @Override
    public Object[] nextKeys() {
        return nextKeys;
    }

    @Override
    public List<Sequence> values() {
        return seq;
    }
}
