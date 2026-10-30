/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.accesscontrol;

import org.apache.lucene.index.Term;
import org.apache.lucene.sandbox.search.CoveringQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.search.vectors.DenseVectorQuery;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;

import java.util.function.Supplier;

/**
 * Marks the visited query as not cacheable if it touches a field not granted by FLS, has a leaf that does not report a field,
 * or wraps an additional query or value source it does not report. The result is available from {@link #isCacheable()}.
 */
final class FieldCheckingVisitor extends QueryVisitor {
    private final IndicesAccessControl.IndexAccessControl permissions;
    private boolean cacheable = true;

    FieldCheckingVisitor(IndicesAccessControl.IndexAccessControl permissions) {
        this.permissions = permissions;
    }

    boolean isCacheable() {
        return cacheable;
    }

    /**
     * Marks the visitor as cacheable again, so it can be reused for another query.
     */
    void reset() {
        cacheable = true;
    }

    @Override
    public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
        if (cacheable == false) {
            return EMPTY_VISITOR;
        }

        // CoveringQuery doesn't report the field its minimum match count comes from (e.g. terms_set minimum_should_match_field).
        // DenseVectorQuery with a filter doesn't visit its filter.
        if (parent instanceof CoveringQuery
            || (parent instanceof DenseVectorQuery denseVectorQuery && denseVectorQuery.getFilter() != null)) {
            cacheable = false;
            return EMPTY_VISITOR;
        }

        // we want to use the same visitor for must_not clauses too
        return this;
    }

    @Override
    public boolean acceptField(String field) {
        return checkField(field);
    }

    @Override
    public void visitLeaf(Query query) {
        if (cacheable == false) {
            return;
        }

        // these match the same docs regardless of field permissions, so they are safe to cache without reporting a field
        if (query instanceof MatchAllDocsQuery || query instanceof MatchNoDocsQuery) {
            return;
        }

        if (new FieldReportingProbe().reportsCacheableField(query) == false) {
            cacheable = false;
        }
    }

    @Override
    public void consumeTerms(Query query, Term... terms) {
        for (Term term : terms) {
            if (checkField(term.field()) == false) {
                break;
            }
        }
    }

    @Override
    public void consumeTermsMatching(Query query, String field, Supplier<ByteRunAutomaton> automaton) {
        checkField(field);
    }

    private boolean checkField(String field) {
        if (cacheable == false) {
            return false;
        }

        // don't cache any internal fields (e.g. _field_names), these are complicated.
        if (field.startsWith("_") || permissions.getFieldPermissions().grantsAccessTo(field) == false) {
            cacheable = false;
        }
        return cacheable;
    }

    /**
     * Records whether a query reports a field to the visitor it is given, and checks that field's permissions through the outer
     * visitor.
     */
    private class FieldReportingProbe extends QueryVisitor {
        private boolean reported = false;

        private boolean reportsCacheableField(Query query) {
            query.visit(this);
            return reported && cacheable;
        }

        @Override
        public boolean acceptField(String field) {
            reported = true;
            return FieldCheckingVisitor.this.acceptField(field);
        }

        @Override
        public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
            // only the query's own fields count, the outer visitor checks its children
            return EMPTY_VISITOR;
        }
    }
}
