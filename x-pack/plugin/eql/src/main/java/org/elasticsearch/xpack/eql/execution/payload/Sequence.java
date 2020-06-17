/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.payload;

import org.elasticsearch.search.SearchHit;

public class Sequence {

    private final Iterable<SearchHit> events;

    public Sequence(Iterable<SearchHit> event) {
        this.events = event;
    }

    public Iterable<SearchHit> event() {
        return events;
    }
}
