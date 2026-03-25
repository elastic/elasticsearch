/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.lookup;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.internal.AliasFilter;

import java.io.IOException;
import java.util.List;
import java.util.function.BiFunction;

/**
 * Helper class used in lookup joins.
 * The {@code processMatches()} method searches a field in a Lucene index
 * for documents containing a given term.  Positions, segments and docs
 * of the matches are gathered into {@code IntVector.Builder}s.
 */

public class BulkKeywordLookup {
    private final MappedFieldType rightFieldType;
    private final int matchChannelOffset;
    private final int extractChannelOffset;
    private final SearchExecutionContext context;
    private final ClusterService clusterService;
    private final AliasFilter aliasFilter;
    private final Warnings warnings;
    private final String fieldName;
    private final BiFunction<Block, Integer, Object> blockValueReader;

    private TermsEnum[] termsEnumCache = null;
    private PostingsEnum[] postingsCache = null;
    private final BytesRef scratch = new BytesRef();

    private int previousPosition = -1;
    private BytesRef termBytes = null;
    private List<LeafReaderContext> rightLeaves = null;
    private int rightLeafOrd = 0;

    public BulkKeywordLookup(
        MappedFieldType rightFieldType,
        ElementType leftElementType,
        SearchExecutionContext context,
        int matchChannelOffset,
        int extractChannelOffset,
        ClusterService clusterService,
        AliasFilter aliasFilter,
        Warnings warnings
    ) {
        this.rightFieldType = rightFieldType;
        this.context = context;
        this.matchChannelOffset = matchChannelOffset; // offset of field in left (page shipped to lookup index)
        this.extractChannelOffset = extractChannelOffset; // offset of field in right (page from ValuesSourceReaderOperator)
        this.clusterService = clusterService;
        this.aliasFilter = aliasFilter;
        this.warnings = warnings;
        this.fieldName = rightFieldType.name();
        this.blockValueReader = QueryList.createBlockValueReaderForType(leftElementType);
    }

    /**
     * Process a single query at the given position using direct Lucene index access
     * populating the builders with at most {@code maxMatches} values.
     * Returns number of matches added to builders.
     *
     * When the returned value equals {@code maxMatches} there may be additional matches
     * to process for the position.  The caller should repeat the call with the same position
     * until all matches are processed.
     *
     * This method bypasses Lucene's query framework entirely and directly accesses
     * the inverted index using TermsEnum and PostingsEnum for maximum performance.
     */
    public int processMatches(
        Page inputPage,
        int position,
        IndexReader indexReader,
        int maxMatches,
        IntVector.Builder docsBuilder,
        IntVector.Builder segmentsBuilder,
        IntVector.Builder positionsBuilder
    ) {
        try {

            // Reset state when the left position is advanced.
            // Repeated calls with the same left position process successive groups of matches.
            //
            if (previousPosition != position) {
                final BytesRefBlock block = inputPage.getBlock(matchChannelOffset);
                final int valueCount = block.getValueCount(position);
                if (valueCount > 1) {
                    warnings.registerException(new IllegalArgumentException("LOOKUP JOIN encountered multi-value"));
                    return 0; // Skip left multi-value positions
                }
                if (valueCount < 1) {
                    return 0; // Skip left null positions
                }
                final int firstValueIndex = block.getFirstValueIndex(position);
                termBytes = block.getBytesRef(firstValueIndex, scratch);
                rightLeaves = indexReader.leaves();
                previousPosition = position;
            }

            // Process up to maxMatches from the index leaves on the right side of the join.
            //
            int matches = 0;
            while (true) {
                if (rightLeafOrd >= rightLeaves.size()) {
                    return 0;
                }
                LeafReaderContext leafContext = rightLeaves.get(rightLeafOrd);
                PostingsEnum postings = getPostingsEnum(rightLeafOrd);
                if (postings == null) {
                    rightLeafOrd++;
                    continue;
                }
                Bits liveDocs = leafContext.reader().getLiveDocs();
                while (matches < maxMatches) {
                    int docId = postings.nextDoc();
                    if (docId == PostingsEnum.NO_MORE_DOCS) {
                        break;
                    }
                    if (liveDocs != null && liveDocs.get(docId) == false) {
                        continue; // Skip deleted documents
                    }
                    docsBuilder.appendInt(docId);
                    if (segmentsBuilder != null) {
                        segmentsBuilder.appendInt(rightLeafOrd);
                    }
                    positionsBuilder.appendInt(position);
                    matches++;
                }
                return matches;
            }

        } catch (Exception e) {
            warnings.registerException(e);
            return 0;
        }
    }

    /*
     * Return the postings (docIds) for the current term in the specified leaf.
     * Updates postings cache on first use.
     */
    private PostingsEnum getPostingsEnum(int leafOrd) throws IOException {
        final TermsEnum termsEnum = termsEnumCache[leafOrd];
        if (termsEnum.seekExact(termBytes) == false) {
            return null; // Term doesn't exist in this segment
        }
        PostingsEnum postings = postingsCache[leafOrd];
        if (postings == null) {
            postings = termsEnum.postings(null, 0);
            postingsCache[leafOrd] = postings;
        }
        return postings;
    }

    public int getPositionCount(Page inputPage) {
        final Block block = inputPage.getBlock(matchChannelOffset);
        return block.getPositionCount();
    }

    public int getExtractChannelOffset() {
        return extractChannelOffset;
    }

    /**
     * Initialize caches for the given index reader. This should be called once
     * before the first processMatches call for a given index reader.
     */
    public void initializeCaches(IndexReader indexReader) throws IOException {
        if (termsEnumCache == null) {
            final int numLeaves = indexReader.leaves().size();
            termsEnumCache = new TermsEnum[numLeaves];
            postingsCache = new PostingsEnum[numLeaves];

            // Pre-populate caches with TermsEnum for each leaf
            for (int i = 0; i < numLeaves; i++) {
                LeafReaderContext leafContext = indexReader.leaves().get(i);
                Terms terms = leafContext.reader().terms(fieldName);
                termsEnumCache[i] = (terms == null) ? TermsEnum.EMPTY : terms.iterator();
            }
        }
    }
}
