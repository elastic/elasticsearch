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

    protected AbstractPayload(boolean timedOut, TimeValue timeTook) {
        this.timedOut = timedOut;
        this.timeTook = timeTook;
    }

    @Override
    public boolean timedOut() {
        return timedOut;
    }

    @Override
    public TimeValue timeTook() {
        return timeTook;
    }
}
