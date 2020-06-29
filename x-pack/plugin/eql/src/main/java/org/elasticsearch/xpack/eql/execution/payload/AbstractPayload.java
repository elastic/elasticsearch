/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.payload;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.eql.session.Payload;

public abstract class AbstractPayload implements Payload {

    private final boolean timedOut;
    private final TimeValue timeTook;
    private final Object[] nextKeys;

    protected AbstractPayload(boolean timedOut, TimeValue timeTook, Object[] nextKeys) {
        this.timedOut = timedOut;
        this.timeTook = timeTook;
        this.nextKeys = nextKeys;
    }

    @Override
    public boolean timedOut() {
        return timedOut;
    }

    @Override
    public TimeValue timeTook() {
        return timeTook;
    }

    @Override
    public Object[] nextKeys() {
        return nextKeys;
    }
}
