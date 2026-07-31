/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query.bitmapterms;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.IOException;
import java.util.Objects;

/**
 * A query that matches documents whose numeric field value is present in a bitmap, for fields that
 * use the {@code index_terms} inverted-index path (see {@code NumberFieldMapper}).
 * <p>
 * Terms are sortable bytes, so the terms dictionary and the bitmap ascend in the same direction for
 * non-negative values. A merge-scan advances both cursors without re-seeking, giving
 * O(N_terms_in_range + M_bitmap_values) work per segment.
 * <p>
 * The field's width lives entirely in the {@link BitmapValues}, so this handles {@code integer} and
 * {@code long} fields with one implementation. Terms are decoded to {@code long} rather than compared
 * as bytes, which keeps the merge working on values that cannot be invalidated by moving either
 * cursor &mdash; the {@link BytesRef} from {@link TermsEnum#term()} is only valid until the enum next
 * moves.
 * <p>
 * Only non-negative values are supported; the caller must validate this before constructing the
 * query.
 */
public class BitmapTermsQuery extends Query implements Accountable {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(BitmapTermsQuery.class);

    private final String field;
    private final BitmapValues values;

    public BitmapTermsQuery(String field, BitmapValues values) {
        this.field = Objects.requireNonNull(field);
        this.values = Objects.requireNonNull(values);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new ConstantScoreWeight(this, boost) {
            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                if (values.isEmpty()) {
                    return null;
                }
                LeafReader reader = context.reader();
                Terms terms = reader.terms(field);
                if (terms == null) {
                    return null;
                }

                return new ScorerSupplier() {
                    long cost = -1;

                    @Override
                    public Scorer get(long leadCost) throws IOException {
                        DocIdSetBuilder result = new DocIdSetBuilder(reader.maxDoc(), terms);
                        collectDocs(result, terms.iterator());
                        return new ConstantScoreScorer(score(), scoreMode, result.build().iterator());
                    }

                    @Override
                    public long cost() {
                        if (cost == -1) {
                            // Upper bound: assume each bitmap value matches at least one doc
                            cost = values.cardinality();
                        }
                        return cost;
                    }
                };
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return true;
            }
        };
    }

    /**
     * Merge-scans the terms dictionary and the bitmap together, collecting docs for every term that is
     * present in both. Both sides are sorted in the same order for non-negative values, so each cursor
     * advances monotonically.
     */
    private void collectDocs(DocIdSetBuilder result, TermsEnum termsEnum) throws IOException {
        BitmapValues.Cursor cursor = values.cursor();
        if (cursor.hasNext() == false) {
            return;
        }

        // Reused across seeks: `encoded` always holds the encoding of `bitmapValue`, refreshed
        // wherever that is reassigned, and `seekTarget` is a fixed view onto it.
        byte[] encoded = new byte[values.bytesPerValue()];
        BytesRef seekTarget = new BytesRef(encoded);

        cursor.encodePeek(encoded);
        long bitmapValue = cursor.next();

        // Seek terms enum to the first relevant term
        if (termsEnum.seekCeil(seekTarget) == TermsEnum.SeekStatus.END) {
            return;
        }
        long termValue = decodeTerm(termsEnum.term());
        PostingsEnum postings = null;

        while (true) {
            if (bitmapValue == termValue) {
                // Exact match: collect all docs for this term
                postings = termsEnum.postings(postings, PostingsEnum.NONE);
                DocIdSetBuilder.BulkAdder adder = result.grow(termsEnum.docFreq());
                adder.add(postings);

                if (cursor.hasNext() == false) {
                    break;
                }
                cursor.encodePeek(encoded);
                bitmapValue = cursor.next();

                BytesRef next = termsEnum.next();
                if (next == null) {
                    break;
                }
                termValue = decodeTerm(next);
            } else if (bitmapValue > termValue) {
                // Terms enum is behind: seek it forward to the current bitmap value
                if (termsEnum.seekCeil(seekTarget) == TermsEnum.SeekStatus.END) {
                    break;
                }
                termValue = decodeTerm(termsEnum.term());
            } else {
                // Bitmap is behind: advance it to at least termValue
                cursor.advanceTo(termValue);
                if (cursor.hasNext() == false) {
                    break;
                }
                cursor.encodePeek(encoded);
                bitmapValue = cursor.next();
            }
        }
    }

    /** Decodes immediately, so nothing holds a {@link BytesRef} across a move of the terms enum. */
    private long decodeTerm(BytesRef term) {
        return values.decode(term.bytes, term.offset);
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(field)) {
            visitor.visitLeaf(this);
        }
    }

    @Override
    public String toString(String defaultField) {
        return "BitmapTermsQuery(field=" + field + ", " + values + ")";
    }

    @Override
    public boolean equals(Object other) {
        if (sameClassAs(other) == false) {
            return false;
        }
        BitmapTermsQuery that = (BitmapTermsQuery) other;
        return field.equals(that.field) && values.equals(that.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), field, values);
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        if (values.isEmpty()) {
            return new MatchNoDocsQuery("empty bitmap");
        }
        return super.rewrite(indexSearcher);
    }

    /**
     * The bitmap dominates this query's footprint and can reach tens of megabytes. Reporting it lets the
     * query cache account for a cached entry's true size instead of treating it as negligible.
     */
    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE + RamUsageEstimator.sizeOf(field) + values.ramBytesUsed();
    }
}
