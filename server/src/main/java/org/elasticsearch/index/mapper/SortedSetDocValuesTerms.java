/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.ReaderSlice;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CompiledAutomaton;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A simple terms implementation for SortedSetDocValues that only provides access to {@link TermsEnum} via
 * {@link #iterator} and {@link #intersect(CompiledAutomaton, BytesRef)} methods.
 * We have this custom implementation based on {@link MultiTerms} instead of using
 * {@link org.apache.lucene.index.MultiDocValues#getSortedSetValues(IndexReader, String)}
 * because {@link org.apache.lucene.index.MultiDocValues} builds global ordinals up-front whereas
 * {@link MultiTerms}, which exposes the terms enum via {@link org.apache.lucene.index.MultiTermsEnum},
 * merges terms on the fly.
 */
class SortedSetDocValuesTerms extends Terms {

    public static Terms getTerms(IndexReader r, String field) throws IOException {
        final List<LeafReaderContext> leaves = r.leaves();
        if (leaves.size() == 1) {
            SortedSetDocValues sortedSetDocValues = leaves.get(0).reader().getSortedSetDocValues(field);
            if (sortedSetDocValues == null) {
                return null;
            } else {
                return new org.elasticsearch.index.mapper.SortedSetDocValuesTerms(sortedSetDocValues);
            }
        }

        final List<Terms> termsPerLeaf = new ArrayList<>(leaves.size());
        final List<ReaderSlice> slicePerLeaf = new ArrayList<>(leaves.size());

        for (int leafIdx = 0; leafIdx < leaves.size(); leafIdx++) {
            LeafReaderContext ctx = leaves.get(leafIdx);
            SortedSetDocValues sortedSetDocValues = ctx.reader().getSortedSetDocValues(field);
            if (sortedSetDocValues != null) {
                termsPerLeaf.add(new org.elasticsearch.index.mapper.SortedSetDocValuesTerms(sortedSetDocValues));
                slicePerLeaf.add(new ReaderSlice(ctx.docBase, r.maxDoc(), leafIdx));
            }
        }

        if (termsPerLeaf.isEmpty()) {
            return null;
        } else {
            return new MultiTerms(termsPerLeaf.toArray(EMPTY_ARRAY), slicePerLeaf.toArray(ReaderSlice.EMPTY_ARRAY));
        }
    }

    private final SortedSetDocValues values;

    SortedSetDocValuesTerms(SortedSetDocValues values) {
        this.values = values;
    }

    @Override
    public TermsEnum iterator() throws IOException {
        return values.termsEnum();
    }

    @Override
    public TermsEnum intersect(CompiledAutomaton compiled, final BytesRef startTerm) throws IOException {
        if (startTerm == null) {
            return values.intersect(compiled);
        } else {
            return super.intersect(compiled, startTerm);
        }
    }

    @Override
    public long size() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getSumTotalTermFreq() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getSumDocFreq() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getDocCount() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasFreqs() {
        return false;
    }

    @Override
    public boolean hasOffsets() {
        return false;
    }

    @Override
    public boolean hasPositions() {
        return false;
    }

    @Override
    public boolean hasPayloads() {
        return false;
    }

}
