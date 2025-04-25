/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

public class RerankOperator extends InferenceOperator<RankedDocsResults> {
    public record Factory(
        InferenceRunner inferenceRunner,
        String inferenceId,
        String queryText,
        ExpressionEvaluator.Factory rowEncoderFactory,
        int scoreChannel
    ) implements OperatorFactory {

        @Override
        public String describe() {
            return "RerankOperator[inference_id=[" + inferenceId + "], query=[" + queryText + "], score_channel=[" + scoreChannel + "]]";
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new RerankOperator(
                driverContext,
                inferenceRunner,
                inferenceId,
                queryText,
                rowEncoderFactory().get(driverContext),
                scoreChannel
            );
        }
    }

    private static final int DEFAULT_BATCH_SIZE = 20;

    private final BlockFactory blockFactory;
    private final String queryText;
    private final ExpressionEvaluator rowEncoder;
    private final int scoreChannel;

    // TODO: make it configurable either in the command or as query pragmas
    private final int batchSize = DEFAULT_BATCH_SIZE;

    public RerankOperator(
        DriverContext driverContext,
        InferenceRunner inferenceRunner,
        String inferenceId,
        String queryText,
        ExpressionEvaluator rowEncoder,
        int scoreChannel
    ) {
        super(driverContext, inferenceRunner, inferenceId);

        this.blockFactory = driverContext.blockFactory();
        this.queryText = queryText;
        this.rowEncoder = rowEncoder;
        this.scoreChannel = scoreChannel;
    }

    @Override
    protected void doClose() {
        Releasables.closeExpectNoException(rowEncoder);
    }

    @Override
    public String toString() {
        return "RerankOperator[inference_id=[" + inferenceId() + "], query=[" + queryText + "], score_channel=[" + scoreChannel + "]]";
    }

    @Override
    protected RequestIterator requests(Page inputPage) {
        return new RequestIterator() {
            private final BytesRefBlock inputBlock = (BytesRefBlock) rowEncoder.eval(inputPage);
            private int remainingPositions = inputPage.getPositionCount();

            @Override
            public boolean hasNext() {
                return remainingPositions > 0;
            }

            @Override
            public InferenceAction.Request next() {
                if (hasNext() == false) {
                    throw new NoSuchElementException();
                }

                final int inputSize = Math.min(remainingPositions, batchSize);
                final List<String> inputs = new ArrayList<>(inputSize);
                BytesRef scratch = new BytesRef();

                int startIndex = inputBlock.getPositionCount() - remainingPositions;
                for (int i = 0; i < inputSize; i++) {
                    int pos = startIndex + i;
                    if (inputBlock.isNull(pos)) {
                        inputs.add("");
                    } else {
                        scratch = inputBlock.getBytesRef(inputBlock.getFirstValueIndex(pos), scratch);
                        inputs.add(BytesRefs.toString(scratch));
                    }
                }

                remainingPositions -= inputSize;
                return inferenceRequest(inputs);
            }

            @Override
            public void close() {
                inputBlock.allowPassingToDifferentDriver();
                Releasables.closeExpectNoException(inputBlock);
            }
        };
    }

    @Override
    protected OutputBuilder<RankedDocsResults> outputBuilder(Page inputPage) {
        return new InferenceOperator.OutputBuilder<>() {
            private final DoubleBlock.Builder scoreBlockBuilder = blockFactory.newDoubleBlockBuilder(inputPage.getPositionCount());

            @Override
            protected Class<RankedDocsResults> inferenceResultsClass() {
                return RankedDocsResults.class;
            }

            @Override
            public Page buildOutput() {
                int blockCount = Integer.max(inputPage.getBlockCount(), scoreChannel + 1);
                Block[] blocks = new Block[blockCount];

                try {
                    for (int b = 0; b < blockCount; b++) {
                        if (b == scoreChannel) {
                            blocks[b] = scoreBlockBuilder.build();
                        } else {
                            blocks[b] = inputPage.getBlock(b);
                            blocks[b].incRef();
                        }
                    }
                    return new Page(blocks);
                } catch (Exception e) {
                    Stream.of(blocks).forEach(Block::allowPassingToDifferentDriver);
                    Releasables.closeExpectNoException(blocks);
                    throw (e);
                }
            }

            @Override
            public void close() {
                inputPage.allowPassingToDifferentDriver();
                inputPage.releaseBlocks();

                Releasables.closeExpectNoException(scoreBlockBuilder);
            }

            @Override
            public void onInferenceResults(RankedDocsResults results) {
                results.getRankedDocs()
                    .stream()
                    .sorted(Comparator.comparingInt(RankedDocsResults.RankedDoc::index))
                    .mapToDouble(RankedDocsResults.RankedDoc::relevanceScore)
                    .forEach(scoreBlockBuilder::appendDouble);
            }
        };
    }

    private InferenceAction.Request inferenceRequest(List<String> inputs) {
        return InferenceAction.Request.builder(inferenceId(), TaskType.RERANK).setInput(inputs).setQuery(queryText).build();
    }
}
