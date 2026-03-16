/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.lookup;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.lucene.ShardContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Lookup document IDs for the input queries.
 * This operator will emit Pages consisting of a {@link DocVector} and {@link IntBlock} of positions for each query of the input queries.
 * The position block will be used as keys to combine the extracted values by {@link MergePositionsOperator}.
 */
public final class EnrichQuerySourceOperator extends SourceOperator {
    private final BlockFactory blockFactory;
    private final LookupEnrichQueryGenerator queryList;
    private final Page originalPage;
    private final BlockOptimization blockOptimization;
    private Page optimizedPage;
    private int queryPosition = -1;
    private final IndexedByShardId<? extends ShardContext> shardContexts;
    private final ShardContext shardContext;
    private final SearchExecutionContext searchExecutionContext;
    private final IndexReader indexReader;
    private final IndexSearcher searcher;
    private final Warnings warnings;
    private final int maxPageSize;

    // using smaller pages enables quick cancellation and reduces sorting costs
    public static final int DEFAULT_MAX_PAGE_SIZE = 256;

    public EnrichQuerySourceOperator(
        BlockFactory blockFactory,
        int maxPageSize,
        LookupEnrichQueryGenerator queryList,
        Page originalPage,
        BlockOptimization blockOptimization,
        IndexedByShardId<? extends ShardContext> shardContexts,
        int shardId,
        SearchExecutionContext searchExecutionContext,
        Warnings warnings
    ) {
        this.blockFactory = blockFactory;
        this.maxPageSize = maxPageSize;
        this.queryList = queryList;
        this.originalPage = originalPage;
        this.blockOptimization = blockOptimization;
        this.shardContexts = shardContexts;
        this.shardContext = shardContexts.get(shardId);
        this.shardContext.incRef();
        this.searchExecutionContext = searchExecutionContext;
        try {
            if (shardContext.searcher().getIndexReader() instanceof DirectoryReader directoryReader) {
                // This optimization is currently disabled for ParallelCompositeReader
                this.indexReader = new CachedDirectoryReader(directoryReader);
            } else {
                this.indexReader = shardContext.searcher().getIndexReader();
            }
            this.searcher = new IndexSearcher(this.indexReader);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        this.warnings = warnings;
    }

    /**
     * Get the input page to use for queryList calls.
     * Creates optimized page on-demand if needed (DICTIONARY state).
     */
    private Page getInputPageInternal() {
        if (blockOptimization == BlockOptimization.DICTIONARY) {
            if (optimizedPage == null) {
                // Create optimized page on-demand
                OrdinalBytesRefBlock ordinalsBytesRefBlock = BlockOptimization.extractOrdinalBlock(originalPage);
                Block optimizedBlock = ordinalsBytesRefBlock.getDictionaryVector().asBlock();
                Block[] blocks = new Block[originalPage.getBlockCount()];
                blocks[0] = optimizedBlock;
                optimizedBlock.incRef();
                for (int i = 1; i < blocks.length; i++) {
                    Block originalBlock = originalPage.getBlock(i);
                    originalBlock.incRef();
                    blocks[i] = originalBlock;
                }
                optimizedPage = new Page(blocks);
            }
            return optimizedPage;
        }
        return originalPage;
    }

    /**
     * Get the input page (may be optimized, e.g., using dictionary block instead of ordinal block).
     * Exposed for testing to verify optimization behavior.
     */
    public Page getInputPage() {
        return getInputPageInternal();
    }

    @Override
    public void finish() {}

    @Override
    public boolean isFinished() {
        Page inputPage = getInputPageInternal();
        return queryPosition >= queryList.getPositionCount(inputPage);
    }

    @Override
    public Page getOutput() {
        Page inputPage = getInputPageInternal();
        int estimatedSize = Math.min(maxPageSize, queryList.getPositionCount(inputPage) - queryPosition);
        IntVector.Builder positionsBuilder = null;
        IntVector.Builder docsBuilder = null;
        IntVector.Builder segmentsBuilder = null;
        try {
            positionsBuilder = blockFactory.newIntVectorBuilder(estimatedSize);
            docsBuilder = blockFactory.newIntVectorBuilder(estimatedSize);
            if (indexReader.leaves().size() > 1) {
                segmentsBuilder = blockFactory.newIntVectorBuilder(estimatedSize);
            }
            int totalMatches = 0;
            do {
                Query query;
                try {
                    query = nextQuery();
                    if (query == null) {
                        assert isFinished();
                        break;
                    }
                    query = searcher.rewrite(new ConstantScoreQuery(query));
                } catch (Exception e) {
                    warnings.registerException(e);
                    continue;
                }
                final var weight = searcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 1.0f);
                if (weight == null) {
                    continue;
                }
                for (LeafReaderContext leaf : indexReader.leaves()) {
                    var scorer = weight.bulkScorer(leaf);
                    if (scorer == null) {
                        continue;
                    }
                    final DocCollector collector = new DocCollector(docsBuilder);
                    scorer.score(collector, leaf.reader().getLiveDocs(), 0, DocIdSetIterator.NO_MORE_DOCS);
                    int matches = collector.matches;

                    if (segmentsBuilder != null) {
                        for (int i = 0; i < matches; i++) {
                            segmentsBuilder.appendInt(leaf.ord);
                        }
                    }
                    for (int i = 0; i < matches; i++) {
                        positionsBuilder.appendInt(queryPosition);
                    }
                    totalMatches += matches;
                }
            } while (totalMatches < maxPageSize);

            return buildPage(totalMatches, positionsBuilder, segmentsBuilder, docsBuilder);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            Releasables.close(docsBuilder, segmentsBuilder, positionsBuilder);
        }
    }

    Page buildPage(int positions, IntVector.Builder positionsBuilder, IntVector.Builder segmentsBuilder, IntVector.Builder docsBuilder) {
        IntVector positionsVector = null;
        IntVector shardsVector = null;
        IntVector segmentsVector = null;
        IntVector docsVector = null;
        Page page = null;
        try {
            positionsVector = positionsBuilder.build();
            shardsVector = blockFactory.newConstantIntVector(0, positions);
            if (segmentsBuilder == null) {
                segmentsVector = blockFactory.newConstantIntVector(0, positions);
            } else {
                segmentsVector = segmentsBuilder.build();
            }
            docsVector = docsBuilder.build();
            page = new Page(
                new DocVector(shardContexts, shardsVector, segmentsVector, docsVector, DocVector.config().mayContainDuplicates()).asBlock(),
                positionsVector.asBlock()
            );
        } finally {
            if (page == null) {
                Releasables.close(positionsBuilder, segmentsVector, docsBuilder, positionsVector, shardsVector, docsVector);
            }
        }
        return page;
    }

    private Query nextQuery() {
        ++queryPosition;
        Page inputPage = getInputPageInternal();
        while (isFinished() == false) {
            Query query = queryList.getQuery(queryPosition, inputPage, searchExecutionContext);
            if (query != null) {
                return query;
            }
            ++queryPosition;
        }
        return null;
    }

    private static class DocCollector implements LeafCollector {
        final IntVector.Builder docIds;
        int matches = 0;

        DocCollector(IntVector.Builder docIds) {
            this.docIds = docIds;
        }

        @Override
        public void setScorer(Scorable scorer) {

        }

        @Override
        public void collect(int doc) {
            ++matches;
            docIds.appendInt(doc);
        }
    }

    @Override
    public void close() {
        this.shardContext.decRef();
        // Release optimized page if it was created
        if (optimizedPage != null && optimizedPage != originalPage) {
            Releasables.close(optimizedPage);
        }
    }
}
