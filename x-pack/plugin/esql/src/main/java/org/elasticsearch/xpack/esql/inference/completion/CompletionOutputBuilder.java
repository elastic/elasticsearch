/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.completion;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.BulkInferenceResponseItem;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.OutputBuilder;

import java.util.List;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

/**
 * Builds output pages for completion inference operations.
 * <p>
 * Converts {@link ChatCompletionResults} from inference responses into a {@link BytesRefBlock} that is appended
 * to the input page. Handles multi-valued outputs where a single input row can produce multiple completion results.
 * </p>
 */
class CompletionOutputBuilder implements OutputBuilder {

    private final BlockFactory blockFactory;

    CompletionOutputBuilder(BlockFactory blockFactory) {
        this.blockFactory = blockFactory;
    }

    /**
     * Builds the output page by converting inference responses into a {@link BytesRefBlock}.
     * <p>
     * The shape array in each response determines how output values are distributed across rows:
     * <ul>
     *   <li>shape[i] = 0: produces a null value for row i</li>
     *   <li>shape[i] = N: produces N completion strings for row i (multi-valued)</li>
     * </ul>
     *
     * @param inputPage The original input page
     * @param responses The ordered list of inference responses corresponding to the input rows
     * @return A new page with the completion results appended as an additional block
     */
    @Override
    public Page buildOutputPage(Page inputPage, List<BulkInferenceResponseItem> responses) {
        try (BytesRefBlock.Builder outputBlockBuilder = blockFactory.newBytesRefBlockBuilder(inputPage.getPositionCount())) {
            for (BulkInferenceResponseItem response : responses) {
                List<String> outputValues = extractOutputValues(response);
                appendResponseToBlock(outputBlockBuilder, response.shape(), outputValues);
            }

            return inputPage.appendBlock(outputBlockBuilder.build());
        }
    }

    /**
     * Appends the output values to the block builder according to the shape specification.
     * <p>
     * The shape array defines the distribution of output values across input rows. Each element
     * in the shape array corresponds to one input row and specifies how many output values
     * that row should consume.
     * </p>
     *
     * @param builder      The block builder to append to
     * @param shape        Array where shape[i] = number of output values for row i (0 means null)
     * @param outputValues The flat list of output values to distribute according to the shape
     */
    private void appendResponseToBlock(BytesRefBlock.Builder builder, int[] shape, List<String> outputValues) {
        int outputIndex = 0;

        for (int valueCount : shape) {
            if (valueCount == 0) {
                // No output values for this row - append null
                builder.appendNull();
            } else {
                // Multiple output values for this row - create multi-valued entry
                builder.beginPositionEntry();
                for (int i = 0; i < valueCount; i++) {
                    if (outputIndex >= outputValues.size()) {
                        throw new IllegalStateException(
                            format(
                                "Not enough output values. Expected at least [{}], but only got [{}]",
                                outputIndex + valueCount - i,
                                outputValues.size()
                            )
                        );
                    }
                    String outputValue = outputValues.get(outputIndex++);
                    if (outputValue == null) {
                        throw new IllegalStateException("Unexpected null value in inference results");
                    }
                    builder.appendBytesRef(new BytesRef(outputValue));
                }
                builder.endPositionEntry();
            }
        }

        // Verify all output values were consumed (catches shape/output mismatch bugs)
        if (outputIndex != outputValues.size()) {
            throw new IllegalStateException(
                format("Not all output values were consumed. Expected [{}], consumed [{}]", outputValues.size(), outputIndex)
            );
        }
    }

    /**
     * Extracts completion text values from an inference response.
     * <p>
     * Validates that the response contains {@link ChatCompletionResults} and extracts
     * the content strings from each completion result.
     * </p>
     *
     * @param responseItem The inference response item to extract values from
     * @return A list of completion strings (empty if response is null)
     * @throws IllegalStateException if the response is not of type {@link ChatCompletionResults}
     */
    public static List<String> extractOutputValues(BulkInferenceResponseItem responseItem) {
        if (responseItem == null || responseItem.inferenceResponse() == null) {
            return List.of();
        }

        if (responseItem.inferenceResponse().getResults() instanceof ChatCompletionResults chatCompletionResults) {
            List<ChatCompletionResults.Result> results = chatCompletionResults.getResults();
            return results.stream().map(ChatCompletionResults.Result::content).toList();
        }

        throw new IllegalStateException(
            format(
                "Inference result has wrong type. Got [{}] while expecting [{}]",
                responseItem.inferenceResponse().getResults().getClass().getName(),
                ChatCompletionResults.class.getName()
            )
        );
    }
}
