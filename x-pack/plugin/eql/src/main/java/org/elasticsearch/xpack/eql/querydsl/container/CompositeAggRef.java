/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.querydsl.container;

import org.elasticsearch.xpack.ql.execution.search.AggRef;

public class CompositeAggRef extends AggRef {

    private final String key;
    private final boolean isDateTimeBased;

    public CompositeAggRef(String key, boolean isDateTimeBased) {
        this.key = key;
        this.isDateTimeBased = isDateTimeBased;
    }

    public String key() {
        return key;
    }

    public boolean isDateTimeBased() {
        return isDateTimeBased;
    }

    @Override
    public String toString() {
        return "|" + key + "|";
    }
}
