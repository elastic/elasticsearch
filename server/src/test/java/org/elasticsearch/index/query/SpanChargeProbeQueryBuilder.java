/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.apache.lucene.index.Term;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.queries.spans.SpanWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.Accountable;
import org.elasticsearch.test.AbstractBuilderTestCase;
import org.elasticsearch.test.ChargeProbeQueryBuilder;

/**
 * A {@link ChargeProbeQueryBuilder} that span queries accept as a clause.
 */
final class SpanChargeProbeQueryBuilder extends ChargeProbeQueryBuilder implements SpanQueryBuilder {

    @Override
    protected Query newProbeQuery() {
        return new ProbeSpanQuery();
    }

    /** Identity-equal so that no clause deduplication can merge two builds. */
    private static final class ProbeSpanQuery extends SpanQuery implements Accountable {
        @Override
        public long ramBytesUsed() {
            return PROBE_BYTES;
        }

        @Override
        public String getField() {
            return AbstractBuilderTestCase.TEXT_FIELD_NAME;
        }

        @Override
        public SpanWeight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void visit(QueryVisitor visitor) {
            if (visitor.acceptField(getField())) {
                visitor.consumeTerms(this, new Term(getField(), "probe"));
            }
        }

        @Override
        public String toString(String field) {
            return NAME;
        }

        @Override
        public boolean equals(Object obj) {
            return this == obj;
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(this);
        }
    }
}
