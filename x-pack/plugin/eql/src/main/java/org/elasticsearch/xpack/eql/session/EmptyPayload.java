/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.session;

import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.eql.action.EqlClusterShards;

import java.util.List;

import static java.util.Collections.emptyList;

public class EmptyPayload implements Payload {

    private final Type type;
    private final TimeValue timeTook;
    private final List<ShardSearchFailure> shardFailures;
    private final List<EqlClusterShards> clusterShards;

    public EmptyPayload(Type type) {
        this(type, TimeValue.ZERO, List.of());
    }

    public EmptyPayload(Type type, TimeValue timeTook, List<ShardSearchFailure> shardFailures) {
        this(type, timeTook, shardFailures, List.of());
    }

    public EmptyPayload(Type type, TimeValue timeTook, List<ShardSearchFailure> shardFailures, List<EqlClusterShards> clusterShards) {
        this.type = type;
        this.timeTook = timeTook;
        this.shardFailures = shardFailures;
        this.clusterShards = List.copyOf(clusterShards);
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
    public List<ShardSearchFailure> shardFailures() {
        return shardFailures;
    }

    @Override
    public List<EqlClusterShards> clusterShards() {
        return clusterShards;
    }

}
