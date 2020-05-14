/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.session;

import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.TotalHits.Relation;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse.Count;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse.Sequence;

import java.util.List;

public class Results {

    public enum Type {
        SEARCH_HIT,
        SEQUENCE,
        COUNT;
    }
    
    private final TotalHits totalHits;
    private final List<?> results;
    private final boolean timedOut;
    private final TimeValue tookTime;
    private final Type type;

    public static Results fromHits(TimeValue tookTime, List<SearchHit> hits) {
        return new Results(new TotalHits(hits.size(), Relation.EQUAL_TO), tookTime, false, hits, Type.SEARCH_HIT);
    }

    public static Results fromSequences(TimeValue tookTime, List<Sequence> sequences) {
        return new Results(new TotalHits(sequences.size(), Relation.EQUAL_TO), tookTime, false, sequences, Type.SEQUENCE);
    }

    public static Results fromCounts(TimeValue tookTime, List<Count> counts) {
        return new Results(new TotalHits(counts.size(), Relation.EQUAL_TO), tookTime, false, counts, Type.COUNT);
    }

    Results(TotalHits totalHits, TimeValue tookTime, boolean timedOut, List<?> results, Type type) {
        this.totalHits = totalHits;
        this.tookTime = tookTime;
        this.timedOut = timedOut;
        this.results = results;
        this.type = type;
    }

    public TotalHits totalHits() {
        return totalHits;
    }

    @SuppressWarnings("unchecked")
    public List<SearchHit> searchHits() {
        return type == Type.SEARCH_HIT ? (List<SearchHit>) results : null;
    }

    @SuppressWarnings("unchecked")
    public List<Sequence> sequences() {
        return type == Type.SEQUENCE ? (List<Sequence>) results : null;
    }

    @SuppressWarnings("unchecked")
    public List<Count> counts() {
        return type == Type.COUNT ? (List<Count>) results : null;
    }

    public TimeValue tookTime() {
        return tookTime;
    }

    public boolean timedOut() {
        return timedOut;
    }
}