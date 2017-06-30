/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.container;

import org.elasticsearch.xpack.sql.querydsl.agg.AggPath;

public class AggRef implements Reference {
    private final String path;
    private final int depth;

    AggRef(String path) {
        this.path = path;
        depth = AggPath.depth(path);
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
}
