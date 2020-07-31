/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.versionfield;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Objects;

/**
 * Query that runs a validation for version ranges across sorted doc values.
 * Used in conjunction with more selective query clauses.
 */
class ValidationOnSortedDv extends Query {

    private final String field;
    private final BytesRef lower;
    private final BytesRef upper;
    private final boolean includeLower;
    private final boolean includeUpper;

    ValidationOnSortedDv(String field, BytesRef lower, BytesRef upper, boolean includeLower, boolean includeUpper) {
        this.field = field;
        this.lower = lower;
        this.upper = upper;
        this.includeLower = includeLower;
        this.includeUpper = includeUpper;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {

        return new ConstantScoreWeight(this, boost) {

            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                final SortedSetDocValues values = DocValues.getSortedSet(context.reader(), field);

                TwoPhaseIterator twoPhase = new TwoPhaseIterator(values) {
                    @Override
                    public boolean matches() throws IOException {
                        long ord = values.nextOrd();
                        // multi-value document can have more than one value, iterate over ords
                        while (ord != SortedSetDocValues.NO_MORE_ORDS) {
                            BytesRef value = values.lookupOrd(ord);
                            boolean inRange = true;
                            if (lower != null) {
                                if (includeLower) {
                                    inRange = lower.compareTo(value) <= 0;
                                } else {
                                    inRange = lower.compareTo(value) < 0;
                                }
                            }
                            if (inRange && upper != null) {
                                if (includeUpper) {
                                    inRange = upper.compareTo(value) >= 0;
                                } else {
                                    inRange = upper.compareTo(value) > 0;
                                }
                            }
                            if (inRange) {
                                return true; // found at least one matching value
                            }
                            ord = values.nextOrd();
                        }
                        return false;
                    }

                    @Override
                    public float matchCost() {
                        // TODO: how can we compute this?
                        return 1000f;
                    }
                };
                return new ConstantScoreScorer(this, score(), scoreMode, twoPhase);
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return true;
            }
        };
    }

    @Override
    public String toString(String field) {
        StringBuilder sb = new StringBuilder(field + ":");
        sb.append(includeLower ? "[" : "(");
        sb.append(lower == null ? "" : VersionEncoder.decodeVersion(lower));
        sb.append("-");
        sb.append(upper == null ? "" : VersionEncoder.decodeVersion(upper));
        sb.append(includeUpper ? "]" : ")");
        return sb.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        ValidationOnSortedDv other = (ValidationOnSortedDv) obj;
        return Objects.equals(field, other.field)
            && Objects.equals(lower, other.lower)
            && Objects.equals(lower, other.upper)
            && includeLower == other.includeLower
            && includeUpper == other.includeUpper;

    }

    @Override
    public int hashCode() {
        return Objects.hash(field, lower, upper, includeLower, includeUpper);
    }

}
