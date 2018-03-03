/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.container;

import org.elasticsearch.xpack.sql.execution.search.FieldExtraction;
import org.elasticsearch.xpack.sql.execution.search.SqlSourceBuilder;
import org.elasticsearch.xpack.sql.querydsl.agg.AggPath;

public class AggRef implements FieldExtraction {
    private final String path;
    private final int depth;
    private final String innerKey;

    public AggRef(String path) {
        this(path, null);
    }

    public AggRef(String path, String innerKey) {
        this.path = path;
        depth = AggPath.depth(path);
        this.innerKey = innerKey;
    }


    @Override
    public String toString() {
        return path;
    }

    @Override
    public int depth() {
        return depth;
    }

    public String path() {
        return path;
    }

    public String innerKey() {
        return innerKey;
    }

    @Override
    public void collectFields(SqlSourceBuilder sourceBuilder) {
        // Aggregations do not need any special fields
    }

    @Override
    public boolean supportedByAggsOnlyQuery() {
        return true;
    }
}
