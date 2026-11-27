/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.predicate;

/**
 * One identity domain for row ids. A Lucene doc id belongs to one leaf reader, and a child row belongs to a different
 * domain than its parent. Equality is by instance, so two spaces with the same name still describe different rows.
 */
public final class RowSpace {
    private final String name;

    public RowSpace(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }
}
