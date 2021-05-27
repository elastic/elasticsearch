/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.util.automaton.ByteRunAutomaton;

import java.util.List;
import java.util.function.Supplier;

/**
 * Aggregations have optimizations that only work if the top level query is
 * fairly cheap to prepare and they use this to detect expensive queries and
 * disable the optimization.
 */
public class ExpensiveQueriesToPrepare {
    private final List<Class<? extends Query>> expensiveQueries;

    public ExpensiveQueriesToPrepare(List<Class<? extends Query>> expensiveQueries) {
        this.expensiveQueries = expensiveQueries;
    }

    public boolean isExpensive(Query query) {
        Visitor visitor = new Visitor();
        query.visit(visitor);
        return visitor.expensive;
    }

    private class Visitor extends QueryVisitor {
        boolean expensive = false;

        @Override
        public QueryVisitor getSubVisitor(Occur occur, Query parent) {
            if (expensive) {
                return QueryVisitor.EMPTY_VISITOR;
            }
            // The default behavior is to ignore occur == NONE, but we want them.
            return this;
        }

        @Override
        public void visitLeaf(Query query) {
            expensive = expensive || isExpensive(query);
        }

        @Override
        public void consumeTerms(Query query, Term... terms) {
            visitLeaf(query);
        }

        @Override
        public void consumeTermsMatching(Query query, String field, Supplier<ByteRunAutomaton> automaton) {
            visitLeaf(query);
        }

        private boolean isExpensive(Query query) {
            for (Class<? extends Query> clazz : expensiveQueries) {
                if (clazz.isAssignableFrom(query.getClass())) {
                    return true;
                }
            }
            return false;
        }
    }
}
