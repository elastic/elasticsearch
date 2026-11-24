/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.payload;

import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.eql.session.Payload;

import java.util.List;

public abstract class AbstractPayload implements Payload {

    private final boolean timedOut;
    private final TimeValue timeTook;
    private final ShardSearchFailure[] shardFailures;

    protected AbstractPayload(boolean timedOut, TimeValue timeTook, List<ShardSearchFailure> shardFailures) {
        this.timedOut = timedOut;
        this.timeTook = timeTook;
        this.shardFailures = shardFailures.toArray(ShardSearchFailure.EMPTY_ARRAY);
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
    public ShardSearchFailure[] shardFailures() {
        return shardFailures;
    }
}
