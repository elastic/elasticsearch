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
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.internal.AliasFilter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.IntFunction;

public class BulkKeywordQueryList {
    private final MappedFieldType rightFieldType;
    private final SearchExecutionContext context;
    private final BytesRefBlock block;
    private final ClusterService clusterService;
    private final AliasFilter aliasFilter;
    private final Warnings warnings;
    private final String fieldName;
    private final IntFunction<Object> blockValueReader;

    private final Map<LeafReaderContext, TermsEnum> termsEnumCache = new HashMap<>();
    private final Map<LeafReaderContext, PostingsEnum> postingsCache = new HashMap<>();

    public BulkKeywordQueryList(
        MappedFieldType rightFieldType,
        SearchExecutionContext context,
        Block block,
        ClusterService clusterService,
        AliasFilter aliasFilter,
        Warnings warnings
    ) {
        this.rightFieldType = rightFieldType;
        this.context = context;
        this.block = (BytesRefBlock) block;
        this.clusterService = clusterService;
        this.aliasFilter = aliasFilter;
        this.warnings = warnings;
        this.fieldName = rightFieldType.name();
        this.blockValueReader = QueryList.createBlockValueReader(block);

    }

    /**
     * Process a single query at the given position using direct Lucene index access.
     * This method bypasses Lucene's query framework entirely and directly accesses
     * the inverted index using TermsEnum and PostingsEnum for maximum performance.
     */
    public int processQuery(
        int position,
        IndexReader indexReader,
        IntVector.Builder docsBuilder,
        IntVector.Builder segmentsBuilder,
        IntVector.Builder positionsBuilder
    ) {
        try {
            final int valueCount = block.getValueCount(position);
            if (valueCount != 1) {
                return 0; // Skip multi-value positions and null positions
            }
            final int firstValueIndex = block.getFirstValueIndex(position);
            BytesRef termBytes = block.getBytesRef(firstValueIndex, new BytesRef());
            int totalMatches = 0;
            for (LeafReaderContext leafContext : indexReader.leaves()) {
                TermsEnum termsEnum = termsEnumCache.computeIfAbsent(leafContext, ctx -> {
                    try {
                        Terms terms = ctx.reader().terms(fieldName);
                        return terms != null ? terms.iterator() : null;
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });

                if (termsEnum.seekExact(termBytes) == false) {
                    continue; // Term doesn't exist in this segment
                }
                PostingsEnum postings = postingsCache.computeIfAbsent(leafContext, ctx -> {
                    try {
                        return termsEnum.postings(null, 0);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });

                // Reset the postings to the current term (reuse the cached PostingsEnum)
                postings = termsEnum.postings(postings, 0);

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

    public int getPositionCount() {
        return block.getPositionCount();
    }
}
