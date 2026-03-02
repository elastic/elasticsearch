/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.IndexSortSortedNumericDocValuesRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.util.automaton.ByteRunAutomaton;

import java.util.function.Supplier;

/**
 * {@link QueryVisitor} that counts visited clauses and throws {@link IndexSearcher.TooManyNestedClauses}
 * when the configured maximum is exceeded.
 * <p>
 * {@link IndexOrDocValuesQuery} is treated as a single clause and its inner queries are ignored,
 * and {@link IndexSortSortedNumericDocValuesRangeQuery} is skipped so only the fallback query is counted.
 */
public class MaxClauseCountQueryVisitor extends QueryVisitor {

    private int numClauses;
    private final int maxClauseCount;

    public MaxClauseCountQueryVisitor(int maxClauseCount) {
        this.maxClauseCount = maxClauseCount;
    }

    public int getMaxClauseCount() {
        return maxClauseCount;
    }

    public int getNumClauses() {
        return numClauses;
    }

    @Override
    public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
        if (parent instanceof IndexOrDocValuesQuery) {
            if (++numClauses > maxClauseCount) {
                throw new IndexSearcher.TooManyNestedClauses();
            }
            // ignore the subqueries inside IndexOrDocValuesQuery
            return QueryVisitor.EMPTY_VISITOR;
        }
        // Return this instance even for MUST_NOT and not an empty QueryVisitor
        return this;
    }

    @Override
    public void visitLeaf(Query query) {
        if (query instanceof IndexSortSortedNumericDocValuesRangeQuery) {
            // ignore so we only count the fallback query
            return;
        }
        if (++numClauses > maxClauseCount) {
            throw new IndexSearcher.TooManyNestedClauses();
        }
    }

    @Override
    public void consumeTerms(Query query, Term... terms) {
        numClauses += terms.length;
        if (numClauses > maxClauseCount) {
            throw new IndexSearcher.TooManyNestedClauses();
        }
    }

    @Override
    public void consumeTermsMatching(Query query, String field, Supplier<ByteRunAutomaton> automaton) {
        if (++numClauses > maxClauseCount) {
            throw new IndexSearcher.TooManyNestedClauses();
        }
    }
}
