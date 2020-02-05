/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.session;

import org.apache.lucene.search.TotalHits;

import java.util.List;

public class Results {

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
