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
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AsyncOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RerankOperator extends AsyncOperator<Page> {

    // Move to a setting.
    private static final int MAX_INFERENCE_WORKER = 10;

    private static final Logger logger = LogManager.getLogger(RerankOperator.class);

    public record Factory(
        InferenceService inferenceService,
        String inferenceId,
        String queryText,
        Map<String, EvalOperator.ExpressionEvaluator.Factory> rerankFieldsEvaluatorSuppliers,
        int scoreChannel
    ) implements OperatorFactory {

        @Override
        public String describe() {
            return "RerankOperator[inference_id="
                + inferenceId
                + " query="
                + queryText
                + " rerank_fields="
                + rerankFieldsEvaluatorSuppliers.keySet()
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
                buildRerankFieldEvaluator(rerankFieldsEvaluatorSuppliers, driverContext),
                scoreChannel
            );
        }

        private Map<String, EvalOperator.ExpressionEvaluator> buildRerankFieldEvaluator(
            Map<String, EvalOperator.ExpressionEvaluator.Factory> rerankFieldsEvaluatorSuppliers,
            DriverContext driverContext
        ) {
            Map<String, EvalOperator.ExpressionEvaluator> rerankFieldsEvaluators = new HashMap<>();

            for (var entry : rerankFieldsEvaluatorSuppliers.entrySet()) {
                rerankFieldsEvaluators.put(entry.getKey(), entry.getValue().get(driverContext));
            }

            return rerankFieldsEvaluators;
        }
    }

    private final InferenceService inferenceService;
    private final BlockFactory blockFactory;
    private final String inferenceId;
    private final String queryText;
    private final Map<String, EvalOperator.ExpressionEvaluator> rerankFieldsEvaluator;
    private final int scoreChannel;

    public RerankOperator(
        DriverContext driverContext,
        InferenceService inferenceService,
        String inferenceId,
        String queryText,
        Map<String, EvalOperator.ExpressionEvaluator> rerankFieldsEvaluator,
        int scoreChannel
    ) {
        super(driverContext, inferenceService.getThreadContext(), MAX_INFERENCE_WORKER);
        this.blockFactory = driverContext.blockFactory();
        this.inferenceService = inferenceService;
        this.inferenceId = inferenceId;
        this.queryText = queryText;
        this.rerankFieldsEvaluator = rerankFieldsEvaluator;
        this.scoreChannel = scoreChannel;
    }

    @Override
    protected void performAsync(Page inputPage, ActionListener<Page> listener) {
        try {
            inferenceService.doInference(
                buildInferenceRequest(inputPage),
                ActionListener.wrap(
                    (inferenceResponse) -> listener.onResponse(buildOutput(inputPage, inferenceResponse)),
                    listener::onFailure
                )
            );
        } catch (IOException e) {
            listener.onFailure(e);
        }
    }

    @Override
    protected void doClose() {

    }

    @Override
    protected void releaseFetchedOnAnyThread(Page page) {
        releasePageOnAnyThread(page);
    }

    @Override
    public Page getOutput() {
        return fetchFromBuffer();
    }

    private Page buildOutput(Page inputPage, InferenceAction.Response inferenceResponse) {
        int blockCount = inputPage.getBlockCount();
        Block.Builder[] blocksBuilders = new Block.Builder[blockCount];

        for (int b = 0; b < blockCount; b++) {
            if (b == scoreChannel) {
                blocksBuilders[b] = ElementType.DOUBLE.newBlockBuilder(inputPage.getPositionCount(), blockFactory);
            } else {
                blocksBuilders[b] = inputPage.getBlock(b).elementType().newBlockBuilder(inputPage.getPositionCount(), blockFactory);
            }
        }

        if (inferenceResponse.getResults() instanceof RankedDocsResults rankedDocsResults) {
            for (var rankedDoc : rankedDocsResults.getRankedDocs()) {
                for (int b = 0; b < blockCount; b++) {
                    if (b == scoreChannel) {
                        if (blocksBuilders[b] instanceof DoubleBlock.Builder scoreBlockBuilder) {
                            scoreBlockBuilder.beginPositionEntry().appendDouble(rankedDoc.relevanceScore()).endPositionEntry();
                        }
                    } else {
                        blocksBuilders[b].copyFrom(inputPage.getBlock(b), rankedDoc.index(), rankedDoc.index() + 1);
                    }
                }
            }

            return new Page(Block.Builder.buildAll(blocksBuilders));
        }

        throw new IllegalStateException(
            "Inference result has wrong type. Got ["
                + inferenceResponse.getResults().getClass()
                + "] while expecting ["
                + RankedDocsResults.class
                + "]"
        );
    }

    private InferenceAction.Request buildInferenceRequest(Page inputPage) throws IOException {
        String[] inputs = new String[inputPage.getPositionCount()];
        Map<String, Block> inputBlocks = new HashMap<>();

        for (var entry : rerankFieldsEvaluator.entrySet()) {
            inputBlocks.put(entry.getKey(), entry.getValue().eval(inputPage));
        }

        for (int pos = 0; pos < inputPage.getPositionCount(); pos++) {
            inputs[pos] = toYaml(inputBlocks, pos);
        }

        return InferenceAction.Request.builder(inferenceId, TaskType.RERANK).setInput(List.of(inputs)).setQuery(queryText).build();
    }

    private String toYaml(Map<String, Block> inputBlocks, int position) throws IOException {
        try (XContentBuilder yamlBuilder = XContentFactory.yamlBuilder().startObject()) {
            for (var blockEntry : inputBlocks.entrySet()) {
                String fieldName = blockEntry.getKey();
                Block currentBlock = blockEntry.getValue();
                if (currentBlock.isNull(position)) {
                    continue;
                }
                yamlBuilder.field(fieldName, toYaml(BlockUtils.toJavaObject(currentBlock, position)));
            }
            return Strings.toString(yamlBuilder.endObject());
        }
    }

    private Object toYaml(Object value) {
        return switch (value) {
            case BytesRef b -> b.utf8ToString();
            case List<?> l -> l.stream().map(this::toYaml).toList();
            default -> value;
        };
    }
}
