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
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AsyncOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class RerankOperator extends AsyncOperator<Page> {

    // Move to a setting.
    private static final int MAX_INFERENCE_WORKER = 10;

    public record Factory(
        InferenceService inferenceService,
        String inferenceId,
        String queryText,
        Map<String, EvalOperator.ExpressionEvaluator.Factory> rerankFieldsEvaluatorFactories,
        int scoreChannel
    ) implements OperatorFactory {

        @Override
        public String describe() {
            return "RerankOperator[inference_id="
                + inferenceId
                + " query="
                + queryText
                + " rerank_fields="
                + rerankFieldsEvaluatorFactories.keySet()
                + " scoreChannel="
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
                rerankFieldsEvaluatorFactories.keySet().toArray(new String[0]),
                rerankFieldsEvaluatorFactories.values()
                    .stream()
                    .map(factory -> factory.get(driverContext))
                    .toArray(EvalOperator.ExpressionEvaluator[]::new),
                scoreChannel
            );
        }
    }

    private final InferenceService inferenceService;
    private final BlockFactory blockFactory;
    private final String inferenceId;
    private final String queryText;
    private final String[] rerankFieldNames;
    private final EvalOperator.ExpressionEvaluator[] rerankFieldsEvaluators;
    private final int scoreChannel;

    public RerankOperator(
        DriverContext driverContext,
        InferenceService inferenceService,
        String inferenceId,
        String queryText,
        String[] rerankFieldNames,
        EvalOperator.ExpressionEvaluator[] rerankFieldsEvaluators,
        int scoreChannel
    ) {
        super(driverContext, inferenceService.getThreadContext(), MAX_INFERENCE_WORKER);
        this.blockFactory = driverContext.blockFactory();
        this.inferenceService = inferenceService;
        this.inferenceId = inferenceId;
        this.queryText = queryText;
        this.rerankFieldNames = rerankFieldNames;
        this.rerankFieldsEvaluators = rerankFieldsEvaluators;
        this.scoreChannel = scoreChannel;
    }

    @Override
    protected void performAsync(Page inputPage, ActionListener<Page> listener) {
        // Ensure input page blocks are released when the listener is called.
        ActionListener<Page> outputListener = ActionListener.runAfter(listener, inputPage::releaseBlocks);
        try {
            inferenceService.doInference(
                buildInferenceRequest(inputPage),
                ActionListener.wrap(
                    inferenceResponse -> buildOutput(inputPage, inferenceResponse, outputListener),
                    outputListener::onFailure
                )
            );
        } catch (Exception e) {
            outputListener.onFailure(e);
        }
    }

    @Override
    protected void doClose() {
        Releasables.closeExpectNoException(this.rerankFieldsEvaluators);
    }

    @Override
    protected void releaseFetchedOnAnyThread(Page page) {
        releasePageOnAnyThread(page);
    }

    @Override
    public Page getOutput() {
        return fetchFromBuffer();
    }

    private void buildOutput(Page inputPage, InferenceAction.Response inferenceResponse, ActionListener<Page> listener) {
        if (inferenceResponse.getResults() instanceof RankedDocsResults rankedDocsResults) {
            buildOutput(inputPage, rankedDocsResults, listener);
            return;
        }

        listener.onFailure(
            new IllegalStateException(
                "Inference result has wrong type. Got ["
                    + inferenceResponse.getResults().getClass()
                    + "] while expecting ["
                    + RankedDocsResults.class
                    + "]"
            )
        );
    }

    private void buildOutput(Page inputPage, RankedDocsResults rankedDocsResults, ActionListener<Page> listener) {
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
            listener.onResponse(new Page(blocks));
        } catch (Exception e) {
            Releasables.closeExpectNoException(blocks);
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
                    scoreBlockFactory.beginPositionEntry().appendDouble(sortedRankedDocsScores[pos]).endPositionEntry();
                } else {
                    scoreBlockFactory.appendNull();
                }
            }

            return scoreBlockFactory.build();
        }
    }

    private InferenceAction.Request buildInferenceRequest(Page inputPage) throws IOException {
        Block[] inputBlocks = inputBlocks(inputPage);

        try {
            String[] inputs = new String[inputPage.getPositionCount()];
            if (inputBlocks.length > 0) for (int pos = 0; pos < inputPage.getPositionCount(); pos++) {
                inputs[pos] = toYaml(inputBlocks, pos);
            }

            return InferenceAction.Request.builder(inferenceId, TaskType.RERANK).setInput(List.of(inputs)).setQuery(queryText).build();
        } finally {
            Releasables.closeExpectNoException(inputBlocks);
        }
    }

    private Block[] inputBlocks(Page inputPage) {
        Block[] blocks = new Block[rerankFieldsEvaluators.length];

        for (int i = 0; i < rerankFieldsEvaluators.length; i++) {
            blocks[i] = rerankFieldsEvaluators[i].eval(inputPage);
        }

        return blocks;
    }

    private String toYaml(Block[] inputBlocks, int position) throws IOException {
        try (XContentBuilder yamlBuilder = XContentFactory.yamlBuilder().startObject()) {
            for (int i = 0; i < inputBlocks.length; i++) {
                String fieldName = rerankFieldNames[i];
                Block currentBlock = inputBlocks[i];
                if (currentBlock.isNull(position)) {
                    continue;
                }
                yamlBuilder.field(fieldName, toYaml(BlockUtils.toJavaObject(currentBlock, position)));
            }
            return Strings.toString(yamlBuilder.endObject());
        }
    }

    private Object toYaml(Object value) {
        try {
            return switch (value) {
                case BytesRef b -> toYaml(b);
                case List<?> l -> l.stream().map(this::toYaml).toList();
                default -> value;
            };
        } catch (Error e) {
            throw new IllegalStateException(LoggerMessageFormat.format("Unexpected error while processing value: {}", e.getMessage()), e);
        }
    }

    private String toYaml(BytesRef b) {
        try {
            return b.utf8ToString();
        } catch (Exception | Error e) {
            // TODO better handling of these cases.
            return "";
        }
    }
}
