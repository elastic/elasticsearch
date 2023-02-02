/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz.accesscontrol;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.util.automaton.ByteRunAutomaton;

import java.util.Set;
import java.util.function.Supplier;

/**
 * Extracts fields from a query, or throws UnsupportedOperationException.
 * <p>
 * Lucene queries used to have Weight#extractTerms, but this was really geared at things
 * such as highlighting, not security. For example terms in a Boolean {@code MUST_NOT} clause
 * are not included, TermsQuery doesn't implement the method as it could be terribly slow, etc.
 *
 */
class FieldExtractor {

    /**
     * Populates {@code fields} with the set of fields used by the query, or throws
     * UnsupportedOperationException if it doesn't know how to do this.
     */
    static void extractFields(Query query, Set<String> fields) {
        query.visit(new QueryVisitor() {
            @Override
            public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
                return this;
            }

            @Override
            public boolean acceptField(String field) {
                fields.add(field);
                return super.acceptField(field);
            }
        });
    }
}
