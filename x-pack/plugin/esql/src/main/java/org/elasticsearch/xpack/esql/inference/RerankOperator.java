/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AsyncOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

public class RerankOperator extends AsyncOperator<Page> {

    // Move to a setting.
    private static final int MAX_INFERENCE_WORKER = 10;

    public record Factory(
        InferenceService inferenceService,
        String inferenceId,
        String queryText,
        Map<String, ExpressionEvaluator.Factory> fieldsEvaluatorFactories,
        int scoreChannel
    ) implements OperatorFactory {

        @Override
        public String describe() {
            return "RerankOperator[inference_id="
                + inferenceId
                + " query="
                + queryText
                + " rerank_fields="
                + fieldsEvaluatorFactories.keySet()
                + " score_channel="
                + scoreChannel
                + "]";
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new RerankOperator(
                driverContext,
                inferenceService,
                inferenceId,
                queryText,
                fieldNames(),
                fieldsEvaluators(driverContext),
                scoreChannel
            );
        }

        private String[] fieldNames() {
            return fieldsEvaluatorFactories.keySet().toArray(String[]::new);
        }

        private ExpressionEvaluator[] fieldsEvaluators(DriverContext context) {
            return fieldsEvaluatorFactories.values().stream().map(factory -> factory.get(context)).toArray(ExpressionEvaluator[]::new);
        }
    }

    private final InferenceService inferenceService;
    private final BlockFactory blockFactory;
    private final String inferenceId;
    private final String queryText;
    private final String[] fieldNames;
    private final ExpressionEvaluator[] fieldsEvaluators;
    private final int scoreChannel;

    public RerankOperator(
        DriverContext driverContext,
        InferenceService inferenceService,
        String inferenceId,
        String queryText,
        String[] fieldNames,
        ExpressionEvaluator[] fieldsEvaluators,
        int scoreChannel
    ) {
        super(driverContext, inferenceService.getThreadContext(), MAX_INFERENCE_WORKER);

        assert inferenceService.getThreadContext() != null;
        assert fieldNames.length == fieldsEvaluators.length;

        this.blockFactory = driverContext.blockFactory();
        this.inferenceService = inferenceService;
        this.inferenceId = inferenceId;
        this.queryText = queryText;
        this.fieldNames = fieldNames;
        this.fieldsEvaluators = fieldsEvaluators;
        this.scoreChannel = scoreChannel;
    }

    @Override
    protected void performAsync(Page inputPage, ActionListener<Page> listener) {
        // Ensure input page blocks are released when the listener is called.
        final ActionListener<Page> outputListener = ActionListener.runAfter(listener, () -> { inputPage.releaseBlocks(); });

        try {
            inferenceService.doInference(
                buildInferenceRequest(inputPage),
                ActionListener.wrap(
                    inferenceResponse -> outputListener.onResponse(buildOutput(inputPage, inferenceResponse)),
                    outputListener::onFailure
                )
            );
        } catch (Exception e) {
            outputListener.onFailure(e);
        }
    }

    @Override
    protected void doClose() {
        Releasables.closeExpectNoException(this.fieldsEvaluators);
    }

    @Override
    protected void releaseFetchedOnAnyThread(Page page) {
        releasePageOnAnyThread(page);
    }

    @Override
    public Page getOutput() {
        return fetchFromBuffer();
    }

    @Override
    public String toString() {
        return "RerankOperator[inference_id="
            + inferenceId
            + " query="
            + queryText
            + " rerank_fields="
            + List.of(fieldNames)
            + " score_channel="
            + scoreChannel
            + "]";
    }

    private Page buildOutput(Page inputPage, InferenceAction.Response inferenceResponse) {
        if (inferenceResponse.getResults() instanceof RankedDocsResults rankedDocsResults) {
            return buildOutput(inputPage, rankedDocsResults);

        }

        throw new IllegalStateException(
            "Inference result has wrong type. Got ["
                + inferenceResponse.getResults().getClass()
                + "] while expecting ["
                + RankedDocsResults.class
                + "]"
        );
    }

    private Page buildOutput(Page inputPage, RankedDocsResults rankedDocsResults) {
        int blockCount = inputPage.getBlockCount();
        Block[] blocks = new Block[blockCount];

        try {
            for (int b = 0; b < blockCount; b++) {
                if (b == scoreChannel) {
                    blocks[b] = buildScoreBlock(inputPage, rankedDocsResults);
                } else {
                    blocks[b] = inputPage.getBlock(b);
                    blocks[b].incRef();
                }
            }
            return new Page(blocks);
        } catch (Exception e) {
            Releasables.closeExpectNoException(blocks);
            throw (e);
        }
    }

    private Block buildScoreBlock(Page inputPage, RankedDocsResults rankedDocsResults) {
        Double[] sortedRankedDocsScores = new Double[inputPage.getPositionCount()];

        try (DoubleBlock.Builder scoreBlockFactory = blockFactory.newDoubleBlockBuilder(inputPage.getPositionCount())) {
            for (RankedDocsResults.RankedDoc rankedDoc : rankedDocsResults.getRankedDocs()) {
                sortedRankedDocsScores[rankedDoc.index()] = (double) rankedDoc.relevanceScore();
            }

            for (int pos = 0; pos < inputPage.getPositionCount(); pos++) {
                if (sortedRankedDocsScores[pos] != null) {
                    scoreBlockFactory.appendDouble(sortedRankedDocsScores[pos]);
                } else {
                    scoreBlockFactory.appendNull();
                }
            }

            return scoreBlockFactory.build();
        }
    }

    private InferenceAction.Request buildInferenceRequest(Page inputPage) {
        Block[] inputBlocks = new Block[fieldsEvaluators.length];

        try {
            for (int b = 0; b < inputBlocks.length; b++) {
                inputBlocks[b] = fieldsEvaluators[b].eval(inputPage);
            }

            String[] inputs = new String[inputPage.getPositionCount()];
            for (int pos = 0; pos < inputPage.getPositionCount(); pos++) {
                try (XContentBuilder yamlBuilder = XContentFactory.yamlBuilder().startObject()) {
                    for (int i = 0; i < inputBlocks.length; i++) {
                        String fieldName = fieldNames[i];
                        Block currentBlock = inputBlocks[i];
                        if (currentBlock.isNull(pos)) {
                            continue;
                        }
                        yamlBuilder.field(fieldName, toYaml(BlockUtils.toJavaObject(currentBlock, pos)));
                    }
                    inputs[pos] = Strings.toString(yamlBuilder.endObject());
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            return InferenceAction.Request.builder(inferenceId, TaskType.RERANK).setInput(List.of(inputs)).setQuery(queryText).build();
        } finally {
            Releasables.closeExpectNoException(inputBlocks);
        }
    }

    private Object toYaml(Object value) {
        try {
            return switch (value) {
                case BytesRef b -> b.utf8ToString();
                case List<?> l -> l.stream().map(this::toYaml).toList();
                default -> value;
            };
        } catch (Error | Exception e) {
            // Swallow errors caused by invalid byteref.
            return "";
        }
    }
}
