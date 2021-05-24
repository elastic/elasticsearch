/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz.accesscontrol;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;

import java.util.Set;

/**
 * Extracts fields from a query, or throws UnsupportedOperationException.
 */
class FieldExtractor {

    /**
     * Populates {@code fields} with the set of fields used by the query, or throws
     * UnsupportedOperationException if it doesn't know how to do this.
     */
    static void extractFields(Query query, Set<String> fields) throws UnsupportedOperationException {
        query.visit(new QueryVisitor() {
            @Override
            public boolean acceptField(String field) {
                fields.add(field);
                return true;
            }

            @Override
            public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
                return this;    // include MUST_NOT leaves as well
            }
        });
    }
}
