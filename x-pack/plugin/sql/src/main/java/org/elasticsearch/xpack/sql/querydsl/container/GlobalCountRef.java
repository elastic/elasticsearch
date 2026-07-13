/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.querydsl.container;

import org.elasticsearch.xpack.ql.execution.search.AggRef;

/**
 * Aggregation reference pointing to the (so called) global count, meaning
 * COUNT over the entire data set.
 */
public final class GlobalCountRef extends AggRef {
    public static final GlobalCountRef INSTANCE = new GlobalCountRef();

    private GlobalCountRef() {}

    @Override
    public String toString() {
        return "#_Total_Hits_#";
    }
}
