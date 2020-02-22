/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.session;

import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.TotalHits.Relation;

import java.util.List;

import static java.util.Collections.emptyList;

public class Results {

    public static final Results EMPTY = new Results(new TotalHits(0, Relation.EQUAL_TO), emptyList());

    private final TotalHits totalHits;
    private final List<Object> results;

    public Results(TotalHits totalHits, List<Object> results) {
        this.totalHits = totalHits;
        this.results = results;
    }

    public TotalHits totalHits() {
        return totalHits;
    }

    public List<Object> results() {
        return results;
    }
}
