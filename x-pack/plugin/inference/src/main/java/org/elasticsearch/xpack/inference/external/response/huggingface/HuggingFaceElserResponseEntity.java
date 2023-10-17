/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.huggingface;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig.DEFAULT_RESULTS_FIELD;
import static org.elasticsearch.xpack.inference.external.entity.Parser.OBJECT_MAPPER;

public class HuggingFaceElserResponseEntity {

    /**
     * The format of the response will look like this:
     *
     * [
     *   {
     *     "outputs": [
     *       [
     *         [
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
     * @param response
     * @return
     * @throws IOException
     */
    public static TextExpansionResults fromResponse(HttpResult response) throws IOException {
        JsonNode node = OBJECT_MAPPER.readTree(response.body());

        if (node.isArray() == false) {
            throw new IllegalArgumentException("Expected ELSER Hugging Face response to be an array of objects");
        }

        ArrayNode arrayNode = (ArrayNode) node;

        List<TextExpansionResults.WeightedToken> tokens = new ArrayList<>();

        for (JsonNode baseObjectNode : arrayNode) {
            JsonNode outputs = baseObjectNode.required("outputs");
            if (outputs.isArray() == false) {
                throw new IllegalArgumentException("Expected ELSER Hugging Face response node [outputs] to be an array");
            }

            ArrayNode outputsArray = (ArrayNode) outputs;
            for (JsonNode intermediateNode : outputsArray) {
                if (intermediateNode.isArray() == false) {
                    throw new IllegalArgumentException("Expected ELSER Hugging Face response node [outputs] node to be an array");
                }

                ArrayNode intermediateArray = (ArrayNode) intermediateNode;
                for (JsonNode resultTuple : intermediateArray) {
                    if (resultTuple.isArray() == false) {
                        throw new IllegalArgumentException("Expected ELSER Hugging Face response result tuple to be an array");
                    }

                    ArrayNode resultTupleArray = (ArrayNode) resultTuple;
                    if (resultTupleArray.size() != 2) {
                        throw new IllegalArgumentException("Expected Elser Hugging Face response result tuple to be of size 2");
                    }

                    String token = resultTupleArray.get(0).textValue();
                    float weight = OBJECT_MAPPER.readerFor(float.class).readValue(resultTupleArray.get(1));

                    tokens.add(new TextExpansionResults.WeightedToken(token, weight));
                }
            }
        }

        // TODO how do we determine if it is truncated?
        return new TextExpansionResults(DEFAULT_RESULTS_FIELD, tokens, false);
    }

    private HuggingFaceElserResponseEntity() {}
}
