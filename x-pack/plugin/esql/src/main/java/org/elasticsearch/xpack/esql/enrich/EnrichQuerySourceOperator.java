/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Lookup document IDs for the input queries.
 * This operator will emit Pages consisting of a {@link DocVector} and {@link IntBlock} of positions for each query of the input queries.
 * The position block will be used as keys to combine the extracted values by {@link MergePositionsOperator}.
 */
final class EnrichQuerySourceOperator extends SourceOperator {

    private final BlockFactory blockFactory;
    private final QueryList queryList;
    private int queryPosition;
    private Weight weight = null;
    private final IndexReader indexReader;
    private int leafIndex = 0;
    private final IndexSearcher searcher;

    EnrichQuerySourceOperator(BlockFactory blockFactory, QueryList queryList, IndexReader indexReader) {
        this.blockFactory = blockFactory;
        this.queryList = queryList;
        this.indexReader = indexReader;
        this.searcher = new IndexSearcher(indexReader);
    }

    @Override
    public void finish() {}

    @Override
    public boolean isFinished() {
        return queryPosition >= queryList.getPositionCount();
    }

    @Override
    public Page getOutput() {
        if (leafIndex == indexReader.leaves().size()) {
            queryPosition++;
            leafIndex = 0;
            weight = null;
        }
        if (isFinished()) {
            return null;
        }
        if (weight == null) {
            Query query = queryList.getQuery(queryPosition);
            if (query != null) {
                try {
                    query = searcher.rewrite(new ConstantScoreQuery(query));
                    weight = searcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 1.0f);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
        try {
            return queryOneLeaf(weight, leafIndex++);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    private Page queryOneLeaf(Weight weight, int leafIndex) throws IOException {
        if (weight == null) {
            return null;
        }
        LeafReaderContext leafReaderContext = indexReader.leaves().get(leafIndex);
        var scorer = weight.bulkScorer(leafReaderContext);
        if (scorer == null) {
            return null;
        }
        IntVector docs = null, segments = null, shards = null;
        boolean success = false;
        try (IntVector.Builder docsBuilder = blockFactory.newIntVectorBuilder(1)) {
            scorer.score(new DocCollector(docsBuilder), leafReaderContext.reader().getLiveDocs());
            docs = docsBuilder.build();
            final int positionCount = docs.getPositionCount();
            segments = blockFactory.newConstantIntVector(leafIndex, positionCount);
            shards = blockFactory.newConstantIntVector(0, positionCount);
            var positions = blockFactory.newConstantIntBlockWith(queryPosition, positionCount);
            success = true;
            return new Page(new DocVector(shards, segments, docs, true).asBlock(), positions);
        } finally {
            if (success == false) {
                Releasables.close(docs, shards, segments);
            }
        }
    }

    private static class DocCollector implements LeafCollector {
        final IntVector.Builder docIds;

        DocCollector(IntVector.Builder docIds) {
            this.docIds = docIds;
        }

        @Override
        public void setScorer(Scorable scorer) {

        }

        @Override
        public void collect(int doc) {
            docIds.appendInt(doc);
        }
    }

    @Override
    public void close() {

    }
}
