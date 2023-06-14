/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntroSorter;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ConstantIntVector;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntArrayVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.SourceOperator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

/**
 * Lookup document IDs for the input terms.
 * - The input terms are sorted alphabetically to minimize I/O when positioning the terms.
 * - The output document IDs are sorted in ascending order to improve the performance of extracting fields.
 * Output: a {@link DocVector} and an {@link IntBlock} of positions of the input terms.
 * The position block will be used as keys to combine the extracted values by {@link MergePositionsOperator}.
 */
final class MatchQuerySourceOperator extends SourceOperator {
    private final String field;
    private final List<LeafReaderContext> leaves;
    private final TermsList termsList;
    private int currentLeaf = 0;

    MatchQuerySourceOperator(String field, IndexReader indexReader, BytesRefBlock inputTerms) {
        this.field = field;
        this.leaves = indexReader.leaves();
        this.termsList = buildTermsList(inputTerms);
    }

    @Override
    public void finish() {}

    @Override
    public boolean isFinished() {
        return currentLeaf >= leaves.size();
    }

    @Override
    public Page getOutput() {
        if (isFinished()) {
            return null;
        }
        try {
            int leafIndex = currentLeaf++;
            return queryOneLeaf(leafIndex);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    private Page queryOneLeaf(int leafIndex) throws IOException {
        Terms terms = leaves.get(leafIndex).reader().terms(field);
        if (terms == null) {
            return null;
        }
        BytesRef pivotTerm = new BytesRef();
        BytesRef nextTerm = new BytesRef();
        TermsEnum termsEnum = terms.iterator();
        PostingsEnum postings = null;
        int doc;
        int[] docs = new int[termsList.size()];
        int[] positions = new int[termsList.size()];
        int matches = 0;
        int pivotIndex = 0;
        while (pivotIndex < termsList.size()) {
            pivotTerm = termsList.getTerm(pivotIndex, pivotTerm);
            int group = 1;
            for (int i = pivotIndex + 1; i < termsList.size(); i++) {
                nextTerm = termsList.getTerm(i, nextTerm);
                if (nextTerm.equals(pivotTerm)) {
                    group++;
                } else {
                    break;
                }
            }
            if (termsEnum.seekExact(pivotTerm)) {
                postings = termsEnum.postings(postings, 0);
                while ((doc = postings.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                    docs = ArrayUtil.grow(docs, matches + group);
                    positions = ArrayUtil.grow(positions, matches + group);
                    for (int g = 0; g < group; g++) {
                        docs[matches] = doc;
                        positions[matches] = termsList.getPosition(pivotIndex + g);
                        matches++;
                    }
                }
            }
            pivotIndex += group;
        }
        int[] finalDocs = docs;
        int[] finalPositions = positions;
        new IntroSorter() {
            int pivot;

            @Override
            protected void setPivot(int i) {
                pivot = finalDocs[i];
            }

            @Override
            protected int comparePivot(int j) {
                return Integer.compare(pivot, finalDocs[j]);
            }

            @Override
            protected void swap(int i, int j) {
                int tmp = finalDocs[i];
                finalDocs[i] = finalDocs[j];
                finalDocs[j] = tmp;

                tmp = finalPositions[i];
                finalPositions[i] = finalPositions[j];
                finalPositions[j] = tmp;
            }
        }.sort(0, matches);
        IntBlock positionsBlock = new IntArrayVector(finalPositions, matches).asBlock();
        // TODO: Should we combine positions for the same docId to avoid extracting the same doc Id multiple times?
        DocVector docVector = new DocVector(
            new ConstantIntVector(0, matches),
            new ConstantIntVector(leafIndex, matches),
            new IntArrayVector(finalDocs, matches),
            true
        );
        return new Page(docVector.asBlock(), positionsBlock);
    }

    @Override
    public void close() {

    }

    /**
     * TODO:
     * We might need two modes: sorted and unsorted terms lists. If the input terms are large and
     * the lookup index is small, then the sorting cost might outweigh the benefits of seeking terms.
     */
    static TermsList buildTermsList(BytesRefBlock block) {
        BytesRefVector vector = block.asVector();
        final int[] indices;
        final int[] positions = new int[block.getTotalValueCount()];
        if (vector != null) {
            for (int i = 0; i < positions.length; i++) {
                positions[i] = i;
            }
            indices = positions;
        } else {
            indices = new int[block.getTotalValueCount()];
            int total = 0;
            for (int i = 0; i < block.getPositionCount(); i++) {
                if (block.isNull(i)) {
                    continue;
                }
                int valueCount = block.getValueCount(i);
                int firstIndex = block.getFirstValueIndex(i);
                for (int j = 0; j < valueCount; j++) {
                    positions[total] = i;
                    indices[total] = firstIndex + j;
                    total++;
                }
            }
            assert total == block.getTotalValueCount();
        }
        new IntroSorter() {
            int pivot;
            final BytesRef scratch1 = new BytesRef();
            final BytesRef scratch2 = new BytesRef();

            @Override
            protected void setPivot(int i) {
                pivot = indices[i];
            }

            @Override
            protected int comparePivot(int j) {
                BytesRef bj = block.getBytesRef(indices[j], scratch1);
                BytesRef bi = block.getBytesRef(pivot, scratch2);
                return bi.compareTo(bj);
            }

            @Override
            protected void swap(int i, int j) {
                int tmp = indices[i];
                indices[i] = indices[j];
                indices[j] = tmp;

                if (indices != positions) {
                    tmp = positions[i];
                    positions[i] = positions[j];
                    positions[j] = tmp;
                }
            }
        }.sort(0, indices.length);
        return new TermsList(positions, indices, block);
    }

    static final class TermsList {
        private final int[] positions;
        private final int[] indices;
        private final BytesRefBlock terms;

        private TermsList(int[] positions, int[] indices, BytesRefBlock terms) {
            this.positions = positions;
            this.indices = indices;
            this.terms = terms;
        }

        int size() {
            return indices.length;
        }

        BytesRef getTerm(int index, BytesRef scratch) {
            return terms.getBytesRef(indices[index], scratch);
        }

        int getPosition(int index) {
            return positions[index];
        }
    }
}
