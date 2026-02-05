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
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.lucene.ShardContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;

/**
 * Intermediate operator that processes match field pages from ExchangeSourceOperator
 * and generates queries to lookup document IDs.
 * This operator reads pages via addInput() and emits Pages consisting of a {@link DocVector}
 * and {@link IntBlock} of positions for each query.
 * Similar to {@link EnrichQuerySourceOperator} but reads from exchange instead of a stored page.
 */
public final class LookupQueryOperator implements Operator {
    private final BlockFactory blockFactory;
    private final LookupEnrichQueryGenerator queryList;
    private final BlockOptimization blockOptimization;
    private final IndexedByShardId<? extends ShardContext> shardContexts;
    private final ShardContext shardContext;
    private final SearchExecutionContext searchExecutionContext;
    private final IndexReader indexReader;
    private final IndexSearcher searcher;
    private final Warnings warnings;
    private final int maxPageSize;

    private Page currentInputPage;
    private Page optimizedInputPage;
    private int queryPosition = -1;
    private Page outputPage;
    private boolean finished = false;

    // Status tracking
    private int pagesReceived = 0;
    private int pagesEmitted = 0;
    private long rowsReceived = 0;
    private long rowsEmitted = 0;

    // using smaller pages enables quick cancellation and reduces sorting costs
    public static final int DEFAULT_MAX_PAGE_SIZE = 256;

    public LookupQueryOperator(
        BlockFactory blockFactory,
        int maxPageSize,
        LookupEnrichQueryGenerator queryList,
        BlockOptimization blockOptimization,
        IndexedByShardId<? extends ShardContext> shardContexts,
        int shardId,
        SearchExecutionContext searchExecutionContext,
        Warnings warnings
    ) {
        this.blockFactory = blockFactory;
        this.maxPageSize = maxPageSize;
        this.queryList = queryList;
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
        if (currentInputPage == null) {
            return null;
        }
        if (blockOptimization == BlockOptimization.DICTIONARY) {
            if (optimizedInputPage == null) {
                // Create optimized page on-demand
                Block inputBlock = currentInputPage.getBlock(0);
                if (inputBlock instanceof BytesRefBlock bytesRefBlock) {
                    OrdinalBytesRefBlock ordinalsBytesRefBlock = bytesRefBlock.asOrdinals();
                    Block optimizedBlock = ordinalsBytesRefBlock.getDictionaryVector().asBlock();
                    Block[] blocks = new Block[currentInputPage.getBlockCount()];
                    blocks[0] = optimizedBlock;
                    optimizedBlock.incRef();
                    for (int i = 1; i < blocks.length; i++) {
                        Block originalBlock = currentInputPage.getBlock(i);
                        originalBlock.incRef();
                        blocks[i] = originalBlock;
                    }
                    optimizedInputPage = new Page(blocks);
                } else {
                    optimizedInputPage = currentInputPage;
                }
            }
            return optimizedInputPage;
        }
        return currentInputPage;
    }

    @Override
    public void addInput(Page page) {
        if (currentInputPage != null) {
            throw new IllegalStateException("Operator already has input page, must consume it first");
        }
        currentInputPage = page;
        queryPosition = -1; // Reset query position for new page
        optimizedInputPage = null; // Reset optimized page
        pagesReceived++;
        rowsReceived += page.getPositionCount();
    }

    @Override
    public Page getOutput() {
        // If we have a pending output page, return it first
        if (outputPage != null) {
            Page result = outputPage;
            outputPage = null;
            return result;
        }

        if (currentInputPage == null) {
            return null;
        }

        Page inputPage = getInputPageInternal();
        if (inputPage == null) {
            return null;
        }

        int positionCount = queryList.getPositionCount(inputPage);

        // Check if we've finished processing all queries for this input page
        if (queryPosition >= positionCount - 1) {
            // Finished processing this input page - release it
            if (optimizedInputPage != null && optimizedInputPage != currentInputPage) {
                optimizedInputPage.allowPassingToDifferentDriver();
                Releasables.closeExpectNoException(optimizedInputPage);
            }
            currentInputPage.releaseBlocks();
            currentInputPage = null;
            optimizedInputPage = null;
            queryPosition = -1; // Reset for next page
            return null;
        }

        // Process next batch of queries
        int estimatedSize = Math.min(maxPageSize, positionCount - queryPosition - 1);
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
                    // Use current queryPosition (nextQuery increments it before returning query)
                    for (int i = 0; i < matches; i++) {
                        positionsBuilder.appendInt(queryPosition);
                    }
                    totalMatches += matches;
                }
            } while (totalMatches < maxPageSize && queryPosition < positionCount);

            if (totalMatches > 0) {
                return buildPage(totalMatches, positionsBuilder, segmentsBuilder, docsBuilder);
            } else {
                // No matches - check if we've finished processing all queries for this input page
                if (queryPosition >= positionCount - 1) {
                    // Finished processing this input page with no matches - release it
                    if (optimizedInputPage != null && optimizedInputPage != currentInputPage) {
                        optimizedInputPage.allowPassingToDifferentDriver();
                        Releasables.closeExpectNoException(optimizedInputPage);
                    }
                    currentInputPage.releaseBlocks();
                    currentInputPage = null;
                    optimizedInputPage = null;
                    queryPosition = -1;
                }
                return null;
            }
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
                new DocVector(shardContexts, shardsVector, segmentsVector, docsVector, null).asBlock(),
                positionsVector.asBlock()
            );
            pagesEmitted++;
            rowsEmitted += page.getPositionCount();
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
        if (inputPage == null) {
            return null;
        }
        int positionCount = queryList.getPositionCount(inputPage);
        while (queryPosition < positionCount) {
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
    public void finish() {
        finished = true;
    }

    @Override
    public boolean isFinished() {
        // Finished if: marked as finished AND no input page AND no output page remaining
        return finished && currentInputPage == null && outputPage == null;
    }

    @Override
    public boolean needsInput() {
        return currentInputPage == null && finished == false;
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        // Can produce more data if we have a pending output page
        if (outputPage != null) {
            return true;
        }
        // Or if we have an input page that hasn't been fully processed
        if (currentInputPage != null) {
            Page inputPage = getInputPageInternal();
            if (inputPage != null) {
                int positionCount = queryList.getPositionCount(inputPage);
                return queryPosition < positionCount - 1;
            }
        }
        return false;
    }

    @Override
    public void close() {
        this.shardContext.decRef();
        // Release pages
        if (optimizedInputPage != null && optimizedInputPage != currentInputPage) {
            optimizedInputPage.allowPassingToDifferentDriver();
            Releasables.closeExpectNoException(optimizedInputPage);
        }
        if (currentInputPage != null) {
            currentInputPage.releaseBlocks();
        }
        if (outputPage != null) {
            outputPage.releaseBlocks();
        }
    }

    @Override
    public String toString() {
        return "LookupQueryOperator[maxPageSize=" + maxPageSize + "]";
    }

    @Override
    public Status status() {
        return new Status(pagesReceived, pagesEmitted, rowsReceived, rowsEmitted);
    }

    public static class Status implements Operator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "lookup_query",
            Status::new
        );

        private final int pagesReceived;
        private final int pagesEmitted;
        private final long rowsReceived;
        private final long rowsEmitted;

        Status(int pagesReceived, int pagesEmitted, long rowsReceived, long rowsEmitted) {
            this.pagesReceived = pagesReceived;
            this.pagesEmitted = pagesEmitted;
            this.rowsReceived = rowsReceived;
            this.rowsEmitted = rowsEmitted;
        }

        Status(StreamInput in) throws IOException {
            pagesReceived = in.readVInt();
            pagesEmitted = in.readVInt();
            rowsReceived = in.readVLong();
            rowsEmitted = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(pagesReceived);
            out.writeVInt(pagesEmitted);
            out.writeVLong(rowsReceived);
            out.writeVLong(rowsEmitted);
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        public int pagesReceived() {
            return pagesReceived;
        }

        public int pagesEmitted() {
            return pagesEmitted;
        }

        public long rowsReceived() {
            return rowsReceived;
        }

        public long rowsEmitted() {
            return rowsEmitted;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("pages_received", pagesReceived);
            builder.field("pages_emitted", pagesEmitted);
            builder.field("rows_received", rowsReceived);
            builder.field("rows_emitted", rowsEmitted);
            return builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Status status = (Status) o;
            return pagesReceived == status.pagesReceived
                && pagesEmitted == status.pagesEmitted
                && rowsReceived == status.rowsReceived
                && rowsEmitted == status.rowsEmitted;
        }

        @Override
        public int hashCode() {
            return Objects.hash(pagesReceived, pagesEmitted, rowsReceived, rowsEmitted);
        }

        @Override
        public String toString() {
            return "LookupQueryOperator.Status["
                + "pagesReceived="
                + pagesReceived
                + ", pagesEmitted="
                + pagesEmitted
                + ", rowsReceived="
                + rowsReceived
                + ", rowsEmitted="
                + rowsEmitted
                + "]";
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.current();
        }
    }
}
