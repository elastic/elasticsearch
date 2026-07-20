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
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.NumericUtils;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.util.Objects;

/**
 * A query that matches documents whose integer field value is present in a {@link RoaringBitmap},
 * for fields that use the {@code index_terms} inverted-index path (see {@code NumberFieldMapper}).
 * <p>
 * Values are encoded as sortable bytes (via {@link NumericUtils#intToSortableBytes}), so the
 * terms dictionary and the bitmap's unsigned iteration order both ascend in the same direction
 * for non-negative integers. A merge-scan advances both cursors without re-seeking, giving
 * O(N_terms_in_range + M_bitmap_values) work per segment.
 * <p>
 * Only non-negative integer values are supported; the caller must validate this before
 * constructing the query.
 */
public class IntBitmapIndexTermsQuery extends Query {

    private final String field;
    private final RoaringBitmap bitmap;

    public IntBitmapIndexTermsQuery(String field, RoaringBitmap bitmap) {
        this.field = Objects.requireNonNull(field);
        this.bitmap = Objects.requireNonNull(bitmap);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new ConstantScoreWeight(this, boost) {
            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                if (bitmap.isEmpty()) {
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
                    public org.apache.lucene.search.Scorer get(long leadCost) throws IOException {
                        DocIdSetBuilder result = new DocIdSetBuilder(reader.maxDoc(), terms);
                        TermsEnum termsEnum = terms.iterator();
                        collectDocs(result, termsEnum);
                        return new ConstantScoreScorer(score(), scoreMode, result.build().iterator());
                    }

                    @Override
                    public long cost() {
                        if (cost == -1) {
                            // Upper bound: assume each bitmap value matches at least one doc
                            cost = bitmap.getCardinality();
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
     * Merge-scans the terms dictionary and the bitmap together, collecting docs for every term
     * that is present in both. Both sides are sorted in the same order for non-negative integers,
     * so each cursor advances monotonically.
     */
    private void collectDocs(DocIdSetBuilder result, TermsEnum termsEnum) throws IOException {
        PeekableIntIterator bitmapIter = bitmap.getIntIterator();
        if (bitmapIter.hasNext() == false) {
            return;
        }

        int bitmapValue = bitmapIter.next();

        // Seek terms enum to the first relevant term
        TermsEnum.SeekStatus status = termsEnum.seekCeil(encodeValue(bitmapValue));
        if (status == TermsEnum.SeekStatus.END) {
            return;
        }

        int termValue = decodeValue(termsEnum.term());
        PostingsEnum postings = null;

        while (true) {
            if (bitmapValue == termValue) {
                // Exact match: collect all docs for this term
                postings = termsEnum.postings(postings, PostingsEnum.NONE);
                DocIdSetBuilder.BulkAdder adder = result.grow(termsEnum.docFreq());
                adder.add(postings);

                if (bitmapIter.hasNext() == false) {
                    break;
                }
                bitmapValue = bitmapIter.next();

                BytesRef next = termsEnum.next();
                if (next == null) {
                    break;
                }
                termValue = decodeValue(next);
            } else if (bitmapValue > termValue) {
                // Terms enum is behind: seek it forward to the current bitmap value
                status = termsEnum.seekCeil(encodeValue(bitmapValue));
                if (status == TermsEnum.SeekStatus.END) {
                    break;
                }
                termValue = decodeValue(termsEnum.term());
            } else {
                // Bitmap is behind: advance it to at least termValue
                bitmapIter.advanceIfNeeded(termValue);
                if (bitmapIter.hasNext() == false) {
                    break;
                }
                bitmapValue = bitmapIter.next();
            }
        }
    }

    private static BytesRef encodeValue(int value) {
        byte[] bytes = new byte[Integer.BYTES];
        NumericUtils.intToSortableBytes(value, bytes, 0);
        return new BytesRef(bytes);
    }

    private static int decodeValue(BytesRef term) {
        return NumericUtils.sortableBytesToInt(term.bytes, term.offset);
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(field)) {
            visitor.visitLeaf(this);
        }
    }

    @Override
    public String toString(String defaultField) {
        if (bitmap.isEmpty()) {
            return "IntBitmapIndexTermsQuery(field=" + field + ", cardinality=0)";
        }
        return "IntBitmapIndexTermsQuery(field="
            + field
            + ", cardinality="
            + bitmap.getCardinality()
            + ", first="
            + bitmap.first()
            + ", last="
            + bitmap.last()
            + ")";
    }

    @Override
    public boolean equals(Object other) {
        if (sameClassAs(other) == false) {
            return false;
        }
        IntBitmapIndexTermsQuery that = (IntBitmapIndexTermsQuery) other;
        return field.equals(that.field) && bitmap.equals(that.bitmap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), field, bitmap);
    }
}
