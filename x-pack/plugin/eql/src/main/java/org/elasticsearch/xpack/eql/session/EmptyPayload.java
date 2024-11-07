/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.session;

import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.core.TimeValue;

import java.util.List;

import static java.util.Collections.emptyList;

public class EmptyPayload implements Payload {

    private final Type type;
    private final TimeValue timeTook;
    private final ShardSearchFailure[] shardFailures;

    public EmptyPayload(Type type) {
        this(type, TimeValue.ZERO, ShardSearchFailure.EMPTY_ARRAY);
    }

    public EmptyPayload(Type type, TimeValue timeTook, ShardSearchFailure[] shardFailures) {
        this.type = type;
        this.timeTook = timeTook;
        this.shardFailures = shardFailures;
    }

    @Override
    public Type resultType() {
        return type;
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
    public List<?> values() {
        return emptyList();
    }

    @Override
    public ShardSearchFailure[] shardFailures() {
        return shardFailures;
    }

}
