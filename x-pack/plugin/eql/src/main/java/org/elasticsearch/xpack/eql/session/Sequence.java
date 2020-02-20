/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.session;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.search.SearchHit;

import java.util.List;

public class Sequence {

    private final List<Tuple<Object, List<SearchHit>>> events;

    public Sequence(List<Tuple<Object, List<SearchHit>>> events) {
        this.events = events;
    }

    public List<Tuple<Object, List<SearchHit>>> events() {
        return events;
    }
}
