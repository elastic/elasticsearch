/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.workflow;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.plan.logical.workflow.Workflow;

import java.util.List;

/**
 * Operator that executes Kibana workflows synchronously.
 * This is a POC implementation that makes blocking HTTP calls to a Kibana synchronous execution API.
 */
public class WorkflowOperator implements Operator {

    private static final Logger logger = LogManager.getLogger(WorkflowOperator.class);

    private final DriverContext driverContext;
    private final String workflowId;
    private final List<ExpressionEvaluator> inputEvaluators;
    private final List<String> inputNames;
    private final Workflow.ErrorHandling errorHandling;
    private final WorkflowClient workflowClient;

    private Page currentPage;
    private int currentPosition;
    private boolean finished;

    public WorkflowOperator(
        DriverContext driverContext,
        String workflowId,
        List<ExpressionEvaluator> inputEvaluators,
        List<String> inputNames,
        Workflow.ErrorHandling errorHandling,
        WorkflowClient workflowClient
    ) {
        this.driverContext = driverContext;
        this.workflowId = workflowId;
        this.inputEvaluators = inputEvaluators;
        this.inputNames = inputNames;
        this.errorHandling = errorHandling;
        this.workflowClient = workflowClient;
        this.currentPosition = 0;
        this.finished = false;
    }

    @Override
    public boolean needsInput() {
        return currentPage == null && !finished;
    }

    @Override
    public void addInput(Page page) {
        this.currentPage = page;
        this.currentPosition = 0;
    }

    @Override
    public void finish() {
        this.finished = true;
    }

    @Override
    public boolean isFinished() {
        return finished && currentPage == null;
    }

    @Override
    public Page getOutput() {
        if (currentPage == null) {
            return null;
        }

        try {
            // Build the output page by processing each row
            int positionCount = currentPage.getPositionCount();
            BytesRefBlock.Builder outputBlockBuilder = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount);

            // Evaluate inputs for all rows
            Block[] evaluatedInputs = new Block[inputEvaluators.size()];
            try {
                for (int i = 0; i < inputEvaluators.size(); i++) {
                    evaluatedInputs[i] = inputEvaluators.get(i).eval(currentPage);
                }

                // Process each row
                for (int row = 0; row < positionCount; row++) {
                    try {
                        // Build input map for this row
                        String inputJson = buildInputJson(evaluatedInputs, row);

                        // Execute the workflow synchronously
                        String outputJson = workflowClient.executeWorkflowSync(workflowId, inputJson);

                        // Add the result to the output
                        outputBlockBuilder.appendBytesRef(new org.apache.lucene.util.BytesRef(outputJson));
                    } catch (Exception e) {
                        if (errorHandling == Workflow.ErrorHandling.NULL) {
                            outputBlockBuilder.appendNull();
                            logger.warn("Workflow execution failed for row {}, returning NULL: {}", row, e.getMessage());
                        } else {
                            throw new RuntimeException("Workflow execution failed at row " + row, e);
                        }
                    }
                }
            } finally {
                // Release evaluated input blocks
                for (Block block : evaluatedInputs) {
                    if (block != null) {
                        block.close();
                    }
                }
            }

            // Build the output page by appending the output block to the input page
            Block[] outputBlocks = new Block[currentPage.getBlockCount() + 1];
            for (int i = 0; i < currentPage.getBlockCount(); i++) {
                outputBlocks[i] = currentPage.getBlock(i);
            }
            outputBlocks[currentPage.getBlockCount()] = outputBlockBuilder.build();

            Page result = new Page(positionCount, outputBlocks);

            // Clear current page
            currentPage = null;

            return result;
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute workflow operator", e);
        }
    }

    /**
     * Build a JSON object from the evaluated input blocks for a specific row.
     */
    private String buildInputJson(Block[] blocks, int row) {
        StringBuilder json = new StringBuilder("{");
        for (int i = 0; i < blocks.length; i++) {
            if (i > 0) {
                json.append(",");
            }
            String name = inputNames.get(i);
            String value = blockValueToJson(blocks[i], row);
            json.append("\"").append(name).append("\":").append(value);
        }
        json.append("}");
        return json.toString();
    }

    /**
     * Convert a block value at a specific position to JSON.
     */
    private String blockValueToJson(Block block, int position) {
        if (block.isNull(position)) {
            return "null";
        }

        int valueCount = block.getValueCount(position);

        // For POC, handle common types
        if (block instanceof BytesRefBlock bytesRefBlock) {
            // Handle multi-valued fields (e.g., from VALUES() aggregation)
            if (valueCount > 1) {
                // Build JSON array for multi-valued field
                StringBuilder arrayJson = new StringBuilder("[");
                int firstValueIndex = bytesRefBlock.getFirstValueIndex(position);
                for (int i = 0; i < valueCount; i++) {
                    if (i > 0) {
                        arrayJson.append(",");
                    }
                    org.apache.lucene.util.BytesRef value = new org.apache.lucene.util.BytesRef();
                    bytesRefBlock.getBytesRef(firstValueIndex + i, value);
                    String str = value.utf8ToString();
                    arrayJson.append("\"").append(escapeJson(str)).append("\"");
                }
                arrayJson.append("]");
                return arrayJson.toString();
            }

            // Single value
            org.apache.lucene.util.BytesRef value = new org.apache.lucene.util.BytesRef();
            bytesRefBlock.getBytesRef(bytesRefBlock.getFirstValueIndex(position), value);
            String str = value.utf8ToString();
            return "\"" + escapeJson(str) + "\"";
        }
        // For other types, convert to string
        return "\"" + block.toString() + "\"";
    }

    /**
     * Simple JSON string escaping.
     */
    private String escapeJson(String str) {
        return str.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(inputEvaluators.toArray(new ExpressionEvaluator[0]));
    }

    @Override
    public Status status() {
        return null; // No status tracking for POC
    }

    /**
     * Factory for creating WorkflowOperator instances.
     */
    public record Factory(
        String workflowId,
        List<ExpressionEvaluator.Factory> inputEvaluatorFactories,
        List<String> inputNames,
        Workflow.ErrorHandling errorHandling,
        WorkflowClient workflowClient
    ) implements OperatorFactory {

        @Override
        public String describe() {
            return "WorkflowOperator[workflow_id=[" + workflowId + "]]";
        }

        @Override
        public Operator get(DriverContext driverContext) {
            List<ExpressionEvaluator> evaluators = inputEvaluatorFactories.stream().map(f -> f.get(driverContext)).toList();
            return new WorkflowOperator(driverContext, workflowId, evaluators, inputNames, errorHandling, workflowClient);
        }
    }
}
