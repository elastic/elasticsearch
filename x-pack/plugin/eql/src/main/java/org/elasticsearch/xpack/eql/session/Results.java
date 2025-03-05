/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.session;

import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.TotalHits.Relation;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse.Event;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse.Sequence;
import org.elasticsearch.xpack.eql.session.Payload.Type;

import java.util.List;

public class Results {

    private final TotalHits totalHits;
    private final List<?> results;
    private final boolean timedOut;
    private final TimeValue tookTime;
    private final Type type;
    private ShardSearchFailure[] shardFailures;

    public static Results fromPayload(Payload payload) {
        List<?> values = payload.values();
        payload.shardFailures();
        return new Results(
            new TotalHits(values.size(), Relation.EQUAL_TO),
            payload.timeTook(),
            false,
            values,
            payload.resultType(),
            payload.shardFailures()
        );
    }

    Results(TotalHits totalHits, TimeValue tookTime, boolean timedOut, List<?> results, Type type, ShardSearchFailure[] shardFailures) {
        this.totalHits = totalHits;
        this.tookTime = tookTime;
        this.timedOut = timedOut;
        this.results = results;
        this.type = type;
        this.shardFailures = shardFailures;
    }

    public TotalHits totalHits() {
        return totalHits;
    }

    @SuppressWarnings("unchecked")
    public List<Event> events() {
        return type == Type.EVENT ? (List<Event>) results : null;
    }

    @SuppressWarnings("unchecked")
    public List<Sequence> sequences() {
        return (type == Type.SEQUENCE || type == Type.SAMPLE) ? (List<Sequence>) results : null;
    }

    public ShardSearchFailure[] shardFailures() {
        return shardFailures;
    }

    public TimeValue tookTime() {
        return tookTime;
    }

    public boolean timedOut() {
        return timedOut;
    }
}
