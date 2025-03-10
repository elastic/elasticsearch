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
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.logging.LogManager;
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
        try {
            InferenceAction.Request rerankInferenceRequest = buildInferenceRequest(inputPage);
            ActionListener<InferenceAction.Response> inferenceResponseListener = ActionListener.wrap(
                inferenceResponse -> listener.onResponse(buildOutput(inputPage, inferenceResponse)),
                listener::onFailure
            );

            inferenceService.doInference(
                rerankInferenceRequest,
                ActionListener.runAfter(inferenceResponseListener, inputPage::releaseBlocks)
            );
        } catch (IOException e) {
            inputPage.releaseBlocks();
            listener.onFailure(e);
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

    private Page buildOutput(Page inputPage, InferenceAction.Response inferenceResponse) {
        int blockCount = inputPage.getBlockCount();
        Block[] blocks = new Block[blockCount];

        for (int b = 0; b < blockCount; b++) {
            if (b == scoreChannel) {
                DoubleBlock.Builder scoreBlockBuilder = blockFactory.newDoubleBlockBuilder(inputPage.getPositionCount());
                blocks[b] = scoreBlockBuilder.build();
            } else {
                blocks[b] = inputPage.getBlock(b);
            }
        }

//        if (inferenceResponse.getResults() instanceof RankedDocsResults rankedDocsResults) {
//            for (var rankedDoc : rankedDocsResults.getRankedDocs()) {
//                for (int b = 0; b < blockCount; b++) {
//                    if (b == scoreChannel) {
//                        LogManager.getLogger(RerankOperator.class).info("Score block builder {}", blocksBuilders[b].getClass());
//                        if (blocksBuilders[b] instanceof DoubleBlock.Builder scoreBlockBuilder) {
//                            DoubleBlock.Builder scoreBlockBuilder = new
//                            LogManager.getLogger(RerankOperator.class).info("Score {}", rankedDoc.relevanceScore());
//                            scoreBlockBuilder.beginPositionEntry().appendDouble(rankedDoc.relevanceScore()).endPositionEntry();
//                        }
//                    } else {
//                        blocksBuilders[b].copyFrom(inputPage.getBlock(b), rankedDoc.index(), rankedDoc.index() + 1);
//                    }
//                }
//            }
//            return new Page(Block.Builder.buildAll(blocksBuilders));
//        }

        throw new IllegalStateException(
            "Inference result has wrong type. Got ["
                + inferenceResponse.getResults().getClass()
                + "] while expecting ["
                + RankedDocsResults.class
                + "]"
        );
    }


    /**
     *     book_no:keyword | author:text                                        | _score:double
     *     8077            | William Faulkner                                   | 1.600000023841858
     *     3293            | Danny Faulkner                                     | 1.399999976158142
     *     4724            | William Faulkner                                   | 1.399999976158142
     *     3535            | [Beverlie Manson, Keith Faulkner]                  | 1.2000000476837158
     *     2847            | Colleen Faulkner                                   | 1.2000000476837158
     *     4977            | William Faulkner                                   | 1.0
     *     4502            | Colleen Faulkner                                   | 1.0
     *     8423            | Paul Faulkner                                      | 0.800000011920929
     *     5404            | William Faulkner                                   | 0.800000011920929
     *     5119            | William Faulkner                                   | 0.6000000238418579
     *     2713            | William Faulkner                                   | 0.6000000238418579
     *     6151            | [Keith Faulkner, Rory Tyger]                       | 0.6000000238418579
     *     2378            | [Carol Faulkner, Holly Byers Ochoa, Lucretia Mott] | 0.4000000059604645
     *     2883            | William Faulkner                                   | 0.4000000059604645
     *     3870            | Keith Faulkner                                     | 0.4000000059604645
     *     9896            | William Faulkner                                   | 0.20000000298023224
     *     7381            | Bettilu Stein Faulkner                             | 0.20000000298023224
     *     5578            | William Faulkner                                   | 0.20000000298023224
     */*

    private InferenceAction.Request buildInferenceRequest(Page inputPage) throws IOException {
        Block[] inputBlocks = inputBlocks(inputPage);

        try {
            String[] inputs = new String[inputPage.getPositionCount()];

            for (int pos = 0; pos < inputPage.getPositionCount(); pos++) {
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
        return switch (value) {
            case BytesRef b -> b.utf8ToString();
            case List<?> l -> l.stream().map(this::toYaml).toList();
            default -> value;
        };
    }
}
