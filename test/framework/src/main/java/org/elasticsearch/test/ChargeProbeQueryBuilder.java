/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Accountable;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.LeafQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Leaf builder whose queries each cost exactly {@link #PROBE_BYTES} on the request breaker.
 * It counts how often it is built, so a test can check that the breaker was charged once per build.
 * Never serialized or parsed.
 */
public class ChargeProbeQueryBuilder extends LeafQueryBuilder<ChargeProbeQueryBuilder> {
    public static final String NAME = "charge_probe";
    public static final long PROBE_BYTES = 1L << 20;

    private int builtCount;

    public int builtCount() {
        return builtCount;
    }

    /** Override to return a narrower query type when the parent query only accepts that type. */
    protected Query newProbeQuery() {
        return new ProbeQuery();
    }

    @Override
    protected final Query doToQuery(SearchExecutionContext context) {
        builtCount++;
        return newProbeQuery();
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME).endObject();
    }

    @Override
    protected void doWriteTo(StreamOutput out) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected int doHashCode() {
        return System.identityHashCode(this);
    }

    @Override
    protected boolean doEquals(ChargeProbeQueryBuilder other) {
        return this == other;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.zero();
    }

    /** Identity-equal so that no clause deduplication can merge two builds. */
    private static final class ProbeQuery extends Query implements Accountable {
        @Override
        public long ramBytesUsed() {
            return PROBE_BYTES;
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void visit(QueryVisitor visitor) {
            String field = AbstractBuilderTestCase.TEXT_FIELD_NAME;
            if (visitor.acceptField(field)) {
                visitor.consumeTerms(this, new Term(field, "probe"));
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
