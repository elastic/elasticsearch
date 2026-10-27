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
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Clears {@code cacheable} if a query touches a field not granted by FLS, has a leaf that does not report a field,
 * or wraps a value source it does not report.
 */
final class FieldCheckingVisitor extends QueryVisitor {
    private final IndicesAccessControl.IndexAccessControl permissions;
    private final AtomicBoolean cacheable;

    FieldCheckingVisitor(IndicesAccessControl.IndexAccessControl permissions, AtomicBoolean cacheable) {
        this.permissions = permissions;
        this.cacheable = cacheable;
    }

    @Override
    public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
        if (cacheable.get() == false) {
            return EMPTY_VISITOR;
        }

        // CoveringQuery doesn't report the field its minimum match count comes from (e.g. terms_set minimum_should_match_field)
        if (parent instanceof CoveringQuery) {
            cacheable.set(false);
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
        if (cacheable.get() == false) {
            return;
        }

        // these match the same docs regardless of field permissions, so they are safe to cache without reporting a field
        if (query instanceof MatchAllDocsQuery || query instanceof MatchNoDocsQuery) {
            return;
        }

        if (new FieldReportingProbe().reportsCacheableField(query) == false) {
            cacheable.set(false);
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
        if (cacheable.get() == false) {
            return false;
        }

        // don't cache any internal fields (e.g. _field_names), these are complicated.
        if (field.startsWith("_") || permissions.getFieldPermissions().grantsAccessTo(field) == false) {
            cacheable.set(false);
        }
        return cacheable.get();
    }

    /**
     * Records whether a query reports a field to the visitor it is given, and checks that field's permissions through the outer
     * visitor.
     */
    private class FieldReportingProbe extends QueryVisitor {
        private boolean reported = false;

        private boolean reportsCacheableField(Query query) {
            query.visit(this);
            return reported && cacheable.get();
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
