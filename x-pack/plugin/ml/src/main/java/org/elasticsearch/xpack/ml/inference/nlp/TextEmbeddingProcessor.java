/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.MlChunkedTextEmbeddingFloatResults;
import org.elasticsearch.xpack.core.ml.inference.results.MlTextEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.NlpTokenizer;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.TokenizationResult;
import org.elasticsearch.xpack.ml.inference.pytorch.results.PyTorchInferenceResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig.DEFAULT_RESULTS_FIELD;

/**
 * A NLP processor that returns a single double[] output from the model. Assumes that only one tensor is returned via inference
 **/
public class TextEmbeddingProcessor extends NlpTask.Processor {

    private final NlpTask.RequestBuilder requestBuilder;

    TextEmbeddingProcessor(NlpTokenizer tokenizer) {
        super(tokenizer);
        this.requestBuilder = tokenizer.requestBuilder();
    }

    @Override
    public void validateInputs(List<String> inputs) {
        // nothing to validate
    }

    @Override
    public NlpTask.RequestBuilder getRequestBuilder(NlpConfig config) {
        return requestBuilder;
    }

    @Override
    public NlpTask.ResultProcessor getResultProcessor(NlpConfig config) {
        return (tokenization, pyTorchResult, chunkResults) -> processResult(
            tokenization,
            pyTorchResult,
            config.getResultsField(),
            chunkResults
        );
    }

    static InferenceResults processResult(
        TokenizationResult tokenization,
        PyTorchInferenceResult pyTorchResult,
        String resultsField,
        boolean chunkResults
    ) {
        if (chunkResults) {
            var embeddings = new ArrayList<MlChunkedTextEmbeddingFloatResults.EmbeddingChunk>();
            for (int i = 0; i < pyTorchResult.getInferenceResult()[0].length; i++) {
                String matchedText;
                if (tokenization.getTokenization(i).tokens().get(0).isEmpty() == false) {
                    int startOffset = tokenization.getTokenization(i).tokens().get(0).get(0).startOffset();
                    int lastIndex = tokenization.getTokenization(i).tokens().get(0).size() - 1;
                    int endOffset = tokenization.getTokenization(i).tokens().get(0).get(lastIndex).endOffset();
                    matchedText = tokenization.getTokenization(i).input().get(0).substring(startOffset, endOffset);

                } else {
                    // No tokens in the input, this should only happen with and empty string
                    assert tokenization.getTokenization(i).input().get(0).isEmpty();
                    matchedText = "";
                }

                embeddings.add(
                    new MlChunkedTextEmbeddingFloatResults.EmbeddingChunk(matchedText, pyTorchResult.getInferenceResult()[0][i])
                );
            }
            return new MlChunkedTextEmbeddingFloatResults(
                Optional.ofNullable(resultsField).orElse(DEFAULT_RESULTS_FIELD),
                embeddings,
                tokenization.anyTruncated()
            );
        } else {
            return new MlTextEmbeddingResults(
                Optional.ofNullable(resultsField).orElse(DEFAULT_RESULTS_FIELD),
                pyTorchResult.getInferenceResult()[0][0],
                tokenization.anyTruncated()
            );
        }
    }
}
