/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.querydsl.query;

import org.elasticsearch.search.sort.NestedSortBuilder;
import org.elasticsearch.xpack.ql.tree.Source;

abstract class LeafQuery extends Query {
    LeafQuery(Source source) {
        super(source);
    }

    @Override
    public final boolean containsNestedField(String path, String field) {
        // No leaf queries are nested
        return false;
    }

    @Override
    public Query addNestedField(String path, String field, String format, boolean hasDocValues) {
        // No leaf queries are nested
        return this;
    }

    @Override
    public void enrichNestedSort(NestedSortBuilder sort) {
        // No leaf queries are nested
    }
}
