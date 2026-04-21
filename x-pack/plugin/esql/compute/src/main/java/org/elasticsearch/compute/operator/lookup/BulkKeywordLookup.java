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
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.io.IOException;

/**
 * Helper class used in lookup joins.
 * The {@code processQuery()} method searches a field in a Lucene index
 * for documents containing a given term.  Positions, segments and docs
 * of the matches are gathered into {@code IntVector.Builder}s.
 */

public class BulkKeywordLookup {
    private final int matchChannelOffset;
    private final int extractChannelOffset;
    private final Warnings warnings;
    private final String fieldName;

    private TermsEnum[] termsEnumCache = null;
    private PostingsEnum[] postingsCache = null;
    private final BytesRef scratch = new BytesRef();

    public BulkKeywordLookup(MappedFieldType rightFieldType, int matchChannelOffset, int extractChannelOffset, Warnings warnings) {
        this.matchChannelOffset = matchChannelOffset; // offset of field in left (page shipped to lookup index)
        this.extractChannelOffset = extractChannelOffset; // offset of field in right (page from ValuesSourceReaderOperator)
        this.warnings = warnings;
        this.fieldName = rightFieldType.name();
    }

    /**
     * Process a single query at the given position using direct Lucene index access.
     * This method bypasses Lucene's query framework entirely and directly accesses
     * the inverted index using TermsEnum and PostingsEnum for maximum performance.
     */
    public int processQuery(
        Page inputPage,
        int position,
        IndexReader indexReader,
        IntVector.Builder docsBuilder,
        IntVector.Builder segmentsBuilder,
        IntVector.Builder positionsBuilder
    ) {
        try {
            final BytesRefBlock block = inputPage.getBlock(matchChannelOffset);
            final int valueCount = block.getValueCount(position);
            if (valueCount > 1) {
                warnings.registerException(new IllegalArgumentException("LOOKUP JOIN encountered multi-value"));
                return 0; // Skip multi-value positions
            }
            if (valueCount < 1) {
                return 0; // Skip null positions
            }
            final int firstValueIndex = block.getFirstValueIndex(position);
            final BytesRef termBytes = block.getBytesRef(firstValueIndex, scratch);
            int totalMatches = 0;
            for (LeafReaderContext leafContext : indexReader.leaves()) {
                int leafOrd = leafContext.ord;
                TermsEnum termsEnum = termsEnumCache[leafOrd];
                if (termsEnum.seekExact(termBytes) == false) {
                    continue; // Term doesn't exist in this segment
                }
                PostingsEnum postings = postingsCache[leafOrd];
                if (postings == null) {
                    postings = termsEnum.postings(null, 0);
                    postingsCache[leafOrd] = postings;
                } else {
                    // Reset the postings to the current term (reuse the cached PostingsEnum)
                    postings = termsEnum.postings(postings, 0);
                }

                Bits liveDocs = leafContext.reader().getLiveDocs();
                int docId;
                while ((docId = postings.nextDoc()) != PostingsEnum.NO_MORE_DOCS) {
                    // Check if document is not deleted
                    if (liveDocs != null && liveDocs.get(docId) == false) {
                        continue; // Skip deleted documents
                    }
                    docsBuilder.appendInt(docId);
                    if (segmentsBuilder != null) {
                        segmentsBuilder.appendInt(leafContext.ord);
                    }
                    positionsBuilder.appendInt(position);
                    totalMatches++;
                }
            }
            return totalMatches;
        } catch (Exception e) {
            warnings.registerException(e);
            return 0;
        }
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
     * before the first processQuery call for a given index reader.
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
