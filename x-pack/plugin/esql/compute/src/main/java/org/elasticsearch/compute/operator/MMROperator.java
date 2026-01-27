/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.diversification.mmr.MMRResultDiversification;
import org.elasticsearch.search.diversification.mmr.MMRResultDiversificationContext;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.vectors.VectorData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MMROperator implements Operator {

    public static Float DEFAULT_LAMBDA = 0.5f;

    private final int docIdChannel;
    private final String diversifyField;
    private final int diversifyFieldChannel;
    private final Integer scoreChannel;
    private final int limit;
    private final VectorData queryVector;
    private final Float lambda;

    private final List<Page> inputPages = new ArrayList<>();
    private Page outputPage = null;
    private boolean isDiversificationComplete = false;

    private boolean finished = false;

    MMROperator(
        int docIdChannel,
        String diversifyField,
        int diversifyFieldChannel,
        int limit,
        @Nullable VectorData queryVector,
        @Nullable Float lambda,
        Integer scoreChannel
    ) {
        this.docIdChannel = docIdChannel;
        this.diversifyField = diversifyField;
        this.diversifyFieldChannel = diversifyFieldChannel;
        this.limit = limit;
        this.queryVector = queryVector;
        this.lambda = lambda;
        this.scoreChannel = scoreChannel;
    }

    @Override
    public boolean needsInput() {
        // be greedy as we need to get all input pages before we can do the work
        return finished == false;
    }

    @Override
    public void addInput(Page page) {
        inputPages.add(page);
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public boolean isFinished() {
        return finished && isDiversificationComplete;
    }

    @Override
    public Page getOutput() {
        // now we can perform our work
        // gather our input rows
        // TODO - refactor code for clarity
        // TODO - ensure docs and vectors have the same positions!
        List<Tuple<RankDoc, VectorData>> docsAndVectors = new ArrayList<>();
        for (Page page : inputPages) {
            Block pageDocIdBlock = page.getBlock(docIdChannel);
            Block diversificationFieldBlock = page.getBlock(diversifyFieldChannel);
            Block scoreBlock = scoreChannel == null ? null : page.getBlock(scoreChannel);
            // TODO - type checking
            var interleaved = new InterleaveBlocks(
                (DocBlock) pageDocIdBlock,
                (FloatBlock) diversificationFieldBlock,
                (DoubleBlock) scoreBlock
            );
            for (Tuple<RankDoc, VectorData> docValues : interleaved) {
                docsAndVectors.add(docValues);
            }
        }

        // set our doc ranks and final mappings for diversification
        docsAndVectors.sort(Comparator.comparing(Tuple::v1));

        List<RankDoc> docs = new ArrayList<>();
        Map<Integer, VectorData> vectors = new HashMap<>();

        int docRank = 1;
        for (Tuple<RankDoc, VectorData> docValues : docsAndVectors) {
            docValues.v1().rank = docRank;
            docs.add(docValues.v1());
            vectors.put(docRank, docValues.v2());
            docRank++;
        }

        var diversificationContext = new MMRResultDiversificationContext(
            diversifyField,
            lambda == null ? DEFAULT_LAMBDA : lambda,
            limit,
            () -> queryVector
        );
        diversificationContext.setFieldVectors(vectors);

        try {
            var diversification = new MMRResultDiversification(diversificationContext);
            var results = diversification.diversify(docs.toArray(new RankDoc[0]));

            // TODO - create proper output page

        } catch (IOException e) {
            // TODO -- return proper exception
            throw new RuntimeException(e);
        }

        isDiversificationComplete = true;
        return outputPage;
    }

    @Override
    public void close() {
        // no cleanup needed
    }

    public record Factory(
        int docIdChannel,
        String diversificationField,
        int diversificationFieldChannel,
        int limit,
        @Nullable VectorData queryVector,
        @Nullable Float lambda,
        @Nullable Integer scoreChannel
    ) implements OperatorFactory {

        @Override
        public Operator get(DriverContext driverContext) {
            return new MMROperator(
                docIdChannel,
                diversificationField,
                diversificationFieldChannel,
                limit,
                queryVector,
                lambda,
                scoreChannel
            );
        }

        @Override
        public String describe() {
            return "MMROperator[diversificationField="
                + "(docIDChannel="
                + docIdChannel
                + "), diversificationField="
                + diversificationField
                + " (channel="
                + diversificationFieldChannel
                + "), limit="
                + limit
                + ", queryVector="
                + (queryVector != null ? queryVector.toString() : "null")
                + ", lambda="
                + (lambda != null ? lambda.toString() : "null")
                + "]";
        }
    }

    public static class InterleaveBlocks implements Iterable<Tuple<RankDoc, VectorData>> {

        private final IntVector docIds;
        private final IntVector shardIds;
        private final FloatBlock floatBlock;
        private final DoubleBlock scoreBlock;
        private final int numPositions;

        public InterleaveBlocks(DocBlock docBlock, FloatBlock floatBlock, DoubleBlock scoreBlock) {
            DocVector docsVector = docBlock.asVector();
            this.docIds = docsVector.docs();
            this.shardIds = docsVector.shards();
            this.floatBlock = floatBlock;
            this.scoreBlock = scoreBlock;
            this.numPositions = floatBlock.getPositionCount();
        }

        @Override
        public Iterator<Tuple<RankDoc, VectorData>> iterator() {
            return new InterleaveBlocksIter(numPositions, docIds, shardIds, floatBlock, scoreBlock);
        }
    }

    public static class InterleaveBlocksIter implements Iterator<Tuple<RankDoc, VectorData>> {

        private final IntVector docIds;
        private final IntVector shardIds;
        private final FloatBlock floatBlock;
        private final DoubleBlock scoreBlock;
        private final int numPositions;
        private int currentPosition = 0;

        // TODO -- _score field
        public InterleaveBlocksIter(
            int numPositions,
            IntVector docIds,
            IntVector shardIds,
            FloatBlock floatBlock,
            @Nullable DoubleBlock scoreBlock
        ) {
            this.docIds = docIds;
            this.shardIds = shardIds;
            this.floatBlock = floatBlock;
            this.scoreBlock = scoreBlock;
            this.numPositions = numPositions;
        }

        @Override
        public boolean hasNext() {
            return currentPosition < numPositions;
        }

        @Override
        public Tuple<RankDoc, VectorData> next() {
            if (currentPosition >= numPositions) {
                return null;
            }

            int docId = docIds.getInt(currentPosition);
            int shardId = shardIds.getInt(currentPosition);
            float docScore = this.scoreBlock == null ? 1.0f : (float) scoreBlock.getDouble(currentPosition);

            var valueIndex = floatBlock.getFirstValueIndex(currentPosition);
            var valueCount = floatBlock.getValueCount(currentPosition);
            float[] vectorValues = new float[valueCount];
            for (int position = 0; position < valueCount; position++) {
                vectorValues[position] = floatBlock.getFloat(valueIndex + position);
            }

            currentPosition++;
            return new Tuple<>(new RankDoc(docId, docScore, shardId), new VectorData(vectorValues));
        }
    }
}
