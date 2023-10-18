/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.huggingface;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig.DEFAULT_RESULTS_FIELD;
import static org.elasticsearch.xpack.inference.external.entity.Parser.OBJECT_MAPPER;

public class HuggingFaceElserResponseEntity {

    private static final Logger logger = LogManager.getLogger(HuggingFaceElserResponseEntity.class);

    /**
     * The format of the response will look like this:
     *
     * [ -> arrayNode
     *   { -> baseObjectNode
     *     "outputs": [
     *       [ -> intermediateNode
     *         [ -> resultTuple
     *           ".",
     *           0.133155956864357
     *         ],
     *         [
     *           "the",
     *           0.6747211217880249
     *         ]
     *       ]
     *     ]
     *   }
     * ]
     */
    public static TextExpansionResults fromResponse(HttpResult response) throws IOException {
        JsonNode node = OBJECT_MAPPER.readTree(response.body());

        validateArray(
            node,
            format("Expected ELSER Hugging Face response to be an array of objects, but was [%s]", node.getNodeType()),
            "Expected ELSER Hugging Face response to be an array of objects"
        );

        ArrayNode arrayNode = (ArrayNode) node;
        List<TextExpansionResults.WeightedToken> tokens = new ArrayList<>();

        parseBaseArrayNode(arrayNode, tokens);

        // TODO how do we determine if it is truncated?
        return new TextExpansionResults(DEFAULT_RESULTS_FIELD, tokens, false);
    }

    private static void parseBaseArrayNode(ArrayNode arrayNode, List<TextExpansionResults.WeightedToken> tokens) throws IOException {
        for (JsonNode baseObjectNode : arrayNode) {
            JsonNode outputs = baseObjectNode.required("outputs");
            validateArray(
                outputs,
                format("Expected ELSER Hugging Face response node [outputs] to be an array but was [%s], skipping", outputs.getNodeType()),
                "Expected ELSER Hugging Face response node [outputs] to be an array"
            );

            ArrayNode outputsArray = (ArrayNode) outputs;
            parseIntermediateNodes(outputsArray, tokens);
        }
    }

    private static void parseIntermediateNodes(ArrayNode outputsArray, List<TextExpansionResults.WeightedToken> tokens) throws IOException {
        for (JsonNode intermediateNode : outputsArray) {
            validateArray(
                intermediateNode,
                format(
                    "Expected ELSER Hugging Face response node [outputs] internal node to be an array, but was [%s]",
                    intermediateNode.getNodeType()
                ),
                "Expected ELSER Hugging Face response node [outputs] internal node to be an array"
            );

            ArrayNode intermediateArray = (ArrayNode) intermediateNode;
            getTokensFromTuples(intermediateArray, tokens);
        }
    }

    private static void getTokensFromTuples(ArrayNode intermediateArray, List<TextExpansionResults.WeightedToken> tokens)
        throws IOException {
        for (JsonNode resultTuple : intermediateArray) {
            validateArray(
                resultTuple,
                format("Expected ELSER Hugging Face response result tuple to be an array, but was [%s]", resultTuple.getNodeType()),
                "Expected ELSER Hugging Face response result tuple to be an array"
            );

            ArrayNode resultTupleArray = (ArrayNode) resultTuple;
            validateTupleSize(resultTupleArray);

            String token = resultTupleArray.get(0).textValue();
            float weight = OBJECT_MAPPER.readerFor(float.class).readValue(resultTupleArray.get(1));

            tokens.add(new TextExpansionResults.WeightedToken(token, weight));
        }
    }

    private static void validateArray(JsonNode node, String errorMsg, String exceptionMsg) {
        if (node.isArray() == false) {
            logger.warn(errorMsg);

            throw new IllegalArgumentException(exceptionMsg);
        }
    }

    private static void validateTupleSize(ArrayNode resultTupleArray) {
        if (resultTupleArray.size() != 2) {
            logger.warn(format("Expected Elser Hugging Face response result tuple to be of size 2, but was [%s]", resultTupleArray.size()));

            throw new IllegalArgumentException("Expected Elser Hugging Face response result tuple to be of size 2");
        }
    }

    private HuggingFaceElserResponseEntity() {}
}
