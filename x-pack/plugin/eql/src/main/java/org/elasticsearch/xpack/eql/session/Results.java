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

import static java.util.Collections.emptyList;

public class Results {

    private enum Type {
        SEARCH_HIT,
        SEQUENCE,
        COUNT;
    }
    
    public static final Results EMPTY = new Results(new TotalHits(0, Relation.EQUAL_TO), TimeValue.MINUS_ONE, false, emptyList());

    private final TotalHits totalHits;
    private final List<?> results;
    private final boolean timedOut;
    private final TimeValue time;
    private final Type type;

    public Results(TotalHits totalHits, TimeValue time, boolean timedOut, List<?> results) {
        this.totalHits = totalHits;
        this.time = time;
        this.timedOut = timedOut;
        this.results = results;

        Type t = Type.SEARCH_HIT;

        if (results.isEmpty() == false) {
            Object o = results.get(0);

            if (o instanceof Sequence) {
                t = Type.SEQUENCE;
            }
            if (o instanceof Count) {
                t = Type.COUNT;
            }
        }
        type = t;
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

    public TimeValue execTime() {
        return time;
    }

    public boolean timedOut() {
        return timedOut;
    }
}